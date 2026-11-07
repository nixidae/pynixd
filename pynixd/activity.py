"""Activity ids for the log messages pynixd makes itself, and the activity of a build in pynixd's sandbox."""

from __future__ import annotations

import itertools
import os
from typing import TYPE_CHECKING

from nix_daemon_protocol.logs import ActivityField, LogNext, LogResult, LogStartActivity, LogStopActivity
from nix_daemon_protocol.protocol import ActivityType, FieldType, ResultType, Verbosity
from pynixd_store.builder import jsonlog

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

    from nix_daemon_protocol.logs import LogMessage
    from pynixd_store.builder.buildlog import Output

_act_counter = itertools.count()


def next_activity_id() -> int:
    """An activity id that cannot collide with the ones a backend sends.

    pynixd forwards a backend daemon's log messages with their activity ids
    unchanged, so an id pynixd makes for itself has to miss that space.
    `src/libutil/logging.cc:208` is where Nix answers the same question:

        id(nextId++ + (((uint64_t) getPid()) << 32))

    Same scheme here. Two daemons on one machine have different pids, so the
    high half separates them. Two on different machines can share a pid, and
    that is a collision Nix has as well with a remote builder -- copying the
    scheme copies its limit, and inventing a different one would not remove
    it.
    """
    return next(_act_counter) + (os.getpid() << 32)


def _fields(values: Iterable[int | str]) -> list[ActivityField]:
    return [
        ActivityField(type=FieldType.STRING, valstr=v)
        if isinstance(v, str)
        else ActivityField(type=FieldType.INT, valint=v)
        for v in values
    ]


class BuildActivity:
    """`BuildLog`'s `actBuild` and the activities the builder's `@nix` lines start (`build/build-log.hh`).

    *verbosity* is the client's: Nix's `TunnelLogger::log` drops a line above
    it (`daemon.cc:89`), and so does `printMsg` before it.
    """

    def __init__(
        self,
        drv_path: str,
        on_log: Callable[[LogMessage], Awaitable[None]],
        verbosity: int,
        *,
        checking: bool = False,
        repairing: bool = False,
    ) -> None:
        self.drv_path = drv_path
        self._verb = "repairing outputs of" if repairing else "checking outputs of" if checking else "building"
        self.id = next_activity_id()
        self._on_log = on_log
        self._verbosity = verbosity
        self._children: dict[int, int] = {}

    async def start(self) -> None:
        """`derivation-building-goal.cc:884`."""
        await self._on_log(
            LogStartActivity(
                act_id=self.id,
                level=Verbosity.INFO,
                type=ActivityType.BUILD,
                text=f"{self._verb} '{self.drv_path}'",
                fields=_fields((self.drv_path, "", 1, 1)),
            )
        )

    async def emit(self, outputs: list[Output]) -> None:
        for output in outputs:
            await self._emit(output)

    async def _emit(self, output: Output) -> None:
        match output:
            case str():
                await self._result(self.id, ResultType.BUILD_LOG_LINE, (output,))
            case jsonlog.SetPhase(phase=phase):
                await self._result(self.id, ResultType.SET_PHASE, (phase,))
            case jsonlog.Message(level=level, text=text):
                if level <= self._verbosity:
                    await self._on_log(LogNext(text=f"{text}\n"))
            case jsonlog.Start():
                child = next_activity_id()
                await self._on_log(
                    LogStartActivity(
                        act_id=child,
                        level=output.level,
                        type=output.type,
                        text=output.text,
                        fields=_fields(output.fields),
                        parent=self.id,
                    )
                )
                # `std::map::emplace` builds the `Activity` before it finds the key taken,
                # so a second start of one id starts an activity and stops it at once.
                if output.id in self._children:
                    await self._on_log(LogStopActivity(act_id=child))
                else:
                    self._children[output.id] = child
            case jsonlog.Stop(id=key):
                if key in self._children:
                    await self._on_log(LogStopActivity(act_id=self._children.pop(key)))
            case jsonlog.Result(id=key, type=kind, fields=fields):
                if key in self._children:
                    await self._result(self._children[key], kind, fields)

    async def stop(self) -> None:
        """The builder's activities first: `builderActivities` is declared after `act`."""
        for key in sorted(self._children, reverse=True):
            await self._on_log(LogStopActivity(act_id=self._children.pop(key)))
        await self._on_log(LogStopActivity(act_id=self.id))

    async def _result(self, act_id: int, kind: int, fields: Iterable[int | str]) -> None:
        await self._on_log(LogResult(act_id=act_id, result_type=kind, fields=_fields(fields)))


class PostBuildHookActivity:
    """`PostBuildHookState::act` (`derivation-building-goal.cc:118`): the hook's activity, one result per line."""

    def __init__(self, hook: str, drv_path: str, on_log: Callable[[LogMessage], Awaitable[None]]) -> None:
        self.hook = hook
        self.drv_path = drv_path
        self.id = next_activity_id()
        self._on_log = on_log

    async def start(self) -> None:
        await self._on_log(
            LogStartActivity(
                act_id=self.id,
                level=Verbosity.TALKATIVE,
                type=ActivityType.POST_BUILD_HOOK,
                text=f"running post-build-hook '{self.hook}'",
                fields=_fields((self.drv_path,)),
            )
        )

    async def line(self, line: str) -> None:
        await self._on_log(
            LogResult(act_id=self.id, result_type=ResultType.POST_BUILD_LOG_LINE, fields=_fields((line,)))
        )

    async def stop(self) -> None:
        await self._on_log(LogStopActivity(act_id=self.id))
