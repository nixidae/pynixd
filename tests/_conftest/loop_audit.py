"""Record every file call pynixd's code makes on a running event loop, across a whole pytest session.

`PYNIXD_LOOP_AUDIT=record` turns it on, and `=gate` also fails the session on a site that
`loop_audit_exempt.EXEMPT` does not name (`tests/derivations/guest` sets `gate`). The xdist
controller merges every worker's sites into `loop-file-calls.json` in the run's log directory,
and the terminal summary counts them.

The suites run the session's pynixd server on the tests' own loop, so the loop thread also runs
test code. A call counts only when a frame of pynixd's packages is on its stack; `from_test` says
a test frame is there too (a test that awaited pynixd code directly, or set up a fixture). A task
the server started has no test frame: its stack begins at the loop.

Blind spots, written into each report: a call made in C (uvloop, sqlite3, huggorm's C++), and a
read or write through a Python file object, whose `open` is seen but whose later calls are not.
"""

from __future__ import annotations

import asyncio
import enum
import json
import os
import stat
import sys
import threading
from collections import Counter
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Final, Protocol, TypedDict

import pytest

import nix_daemon_protocol
import pynixd
import pynixd_store
from tests._conftest import loop_files
from tests._conftest.constants import _log_dir_key
from tests._conftest.loop_audit_exempt import EXEMPT

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Mapping
    from types import FrameType

ENV: Final = "PYNIXD_LOOP_AUDIT"

BLIND: Final = (
    "calls made in C: uvloop, sqlite3, huggorm's C++",
    "read, write and seek through a Python file object after its open",
)

_SPAWN_EVENTS: Final = frozenset({"os.fork", "os.forkpty", "os.posix_spawn", "os.posix_spawnp", "os.system"})
_IMPORT_PREFIX: Final = "<frozen importlib"
_TRACEBACK_FILES: Final = ("/linecache.py", "/traceback.py")
_PSEUDO_FILES: Final = ("/proc/", "/sys/", "/dev/")
_WORKER_KEY: Final = "pynixd_loop_audit"
_NOT_FILES: Final = frozenset({"os.kill", "os.killpg", "os.putenv", "os.unsetenv"})


class Mode(enum.StrEnum):
    OFF = "off"
    RECORD = "record"
    GATE = "gate"
    """Record, and fail the session on a site `violations` names."""


class Kind(enum.StrEnum):
    FILE = "file"
    IMPORT = "import"
    """A module loaded on the loop: it reads the module's files there."""
    TRACEBACK = "traceback"
    """A traceback rendered on the loop: `linecache` reads each frame's source file there."""
    PSEUDO = "pseudo"
    """A file under `/proc`, `/sys` or `/dev`: the kernel or a device answers it, not a disk."""
    SPAWN = "spawn"


@dataclass(frozen=True, slots=True)
class Frame:
    filename: str
    lineno: int
    function: str


@dataclass(frozen=True, slots=True)
class Roots:
    first_party: tuple[str, ...]
    """Directory prefixes, each ending in `/`, of the code under audit."""
    tests: str
    skip: frozenset[str]
    """The recorder's own files."""

    def relative(self, filename: str) -> str:
        for prefix in (*self.first_party, self.tests):
            if filename.startswith(prefix):
                # The package directory's own name stays, so `pynixd/store/x.py` reads as a path.
                return filename[len(prefix.rstrip("/").rpartition("/")[0]) + 1 :]
        return filename


@dataclass(frozen=True, slots=True, order=True)
class Site:
    kind: Kind
    event: str
    site: str
    """The innermost frame of the audited code: `path:line function`."""
    leaf: str
    """The innermost frame of all, where the call is made: `path:line function`."""
    from_test: bool

    @property
    def key(self) -> str:
        """`path function`: what an exemption names, so an edit that moves a line keeps it."""
        where, _, function = self.site.partition(" ")
        return f"{where.rpartition(':')[0]} {function}"


GATED: Final = frozenset({Kind.FILE, Kind.IMPORT})
"""What the gate fails on. A traceback is an error path; a pseudo file or a spawn waits on no disk."""


def violations(sites: Iterable[Site], exempt: Mapping[str, str]) -> list[Site]:
    """The sites the server reached on its own, of a gated kind, that no exemption names."""
    return sorted(s for s in sites if s.kind in GATED and not s.from_test and s.key not in exempt)


def stale(sites: Iterable[Site], exempt: Mapping[str, str]) -> list[str]:
    """Exemptions no site used. Only a run of every suite can call one stale, so this is reported, never failed."""
    return sorted(set(exempt) - {s.key for s in sites})


def classify(event: str, target: str, frames: Iterable[Frame], roots: Roots) -> Site | None:
    """The site of *event* on *target* raised under *frames*, innermost first; `None` when no audited frame is there."""
    leaf: Frame | None = None
    site: Frame | None = None
    from_test = importing = tracing = False
    for frame in frames:
        if frame.filename in roots.skip:
            continue
        if leaf is None:
            leaf = frame
        if site is None and frame.filename.startswith(roots.first_party):
            site = frame
        from_test = from_test or frame.filename.startswith(roots.tests)
        importing = importing or frame.filename.startswith(_IMPORT_PREFIX)
        tracing = tracing or frame.filename.endswith(_TRACEBACK_FILES)
    if site is None or leaf is None:
        return None
    return Site(
        kind=_kind(event, target, importing=importing, tracing=tracing),
        event=event,
        site=_where(site, roots),
        leaf=_where(leaf, roots),
        from_test=from_test,
    )


def _kind(event: str, target: str, *, importing: bool, tracing: bool) -> Kind:
    if importing:
        return Kind.IMPORT
    if tracing:
        return Kind.TRACEBACK
    if event in _SPAWN_EVENTS:
        return Kind.SPAWN
    if target.startswith(_PSEUDO_FILES):
        return Kind.PSEUDO
    return Kind.FILE


def _where(frame: Frame, roots: Roots) -> str:
    return f"{roots.relative(frame.filename)}:{frame.lineno} {frame.function}"


def default_roots() -> Roots:
    def prefix(module_file: str | None) -> str:
        if module_file is None:
            raise RuntimeError("a namespace package has no single directory to audit")
        return str(Path(module_file).resolve().parent) + "/"

    return Roots(
        first_party=tuple(prefix(m.__file__) for m in (pynixd, pynixd_store, nix_daemon_protocol)),
        tests=str(Path(__file__).resolve().parents[1]) + "/",
        skip=frozenset({str(Path(loop_files.__file__).resolve()), str(Path(__file__).resolve())}),
    )


class _Row(TypedDict):
    """A site as a worker hands it to the controller: execnet carries plain data only."""

    kind: str
    event: str
    site: str
    leaf: str
    from_test: bool
    count: int
    first_test: str


class _Worker(Protocol):
    """What the controller reads of an xdist `WorkerController`; xdist ships no types."""

    workeroutput: dict[str, list[_Row]]


@dataclass
class _Seen:
    count: int = 0
    first_test: str = ""


@dataclass
class LoopAudit:
    """The pytest plugin: a session sink for `loop_files`, and the report at the session's end."""

    roots: Roots
    mode: Mode
    exempt: Mapping[str, str]
    sites: dict[Site, _Seen] = field(default_factory=dict)
    test: str = "session"
    _busy: threading.local = field(default_factory=threading.local)

    def sink(self, event: str, args: tuple[object, ...]) -> None:
        # The running loop is per thread: a worker thread of `to_thread` has none.
        if asyncio._get_running_loop() is None or getattr(self._busy, "on", False):  # noqa: SLF001 -- the only probe that does not raise
            return
        self._busy.on = True
        try:
            if event in _NOT_FILES or (args and isinstance(args[0], int) and not _regular(args[0])):
                return
            target = os.fsdecode(args[0]) if args and isinstance(args[0], str | bytes | os.PathLike) else ""
            site = classify(event, target, _frames(sys._getframe(1)), self.roots)  # noqa: SLF001 -- the hook has no frame argument
            if site is not None:
                seen = self.sites.setdefault(site, _Seen(first_test=self.test))
                seen.count += 1
        finally:
            self._busy.on = False

    @pytest.hookimpl(wrapper=True)
    def pytest_runtest_protocol(self, item: pytest.Item) -> Generator[None, object, object]:
        self.test = item.nodeid
        try:
            return (yield)
        finally:
            self.test = "between tests"

    def _rows(self) -> list[_Row]:
        return [
            _Row(
                kind=str(site.kind),
                event=site.event,
                site=site.site,
                leaf=site.leaf,
                from_test=site.from_test,
                count=seen.count,
                first_test=seen.first_test,
            )
            for site, seen in sorted(self.sites.items(), key=lambda item: (-item[1].count, item[0]))
        ]

    @pytest.hookimpl(optionalhook=True)
    def pytest_testnodedown(self, node: _Worker, error: object) -> None:  # noqa: ARG002 -- xdist's signature
        """An xdist worker's sites, merged here: only the controller sees the whole session."""
        for row in node.workeroutput.get(_WORKER_KEY, []):
            site = Site(
                kind=Kind(row["kind"]),
                event=row["event"],
                site=row["site"],
                leaf=row["leaf"],
                from_test=row["from_test"],
            )
            seen = self.sites.setdefault(site, _Seen(first_test=row["first_test"]))
            seen.count += row["count"]

    def pytest_sessionfinish(self, session: pytest.Session) -> None:
        workeroutput = getattr(session.config, "workeroutput", None)
        if workeroutput is not None:
            workeroutput[_WORKER_KEY] = self._rows()
            return
        failing = violations(self.sites, self.exempt)
        log_dir = session.config.stash.get(_log_dir_key, None)
        if log_dir is not None:
            report = {
                "mode": str(self.mode),
                "blind": list(BLIND),
                "violations": [asdict(site) for site in failing],
                "stale_exemptions": stale(self.sites, self.exempt),
                "sites": self._rows(),
            }
            (log_dir / "loop-file-calls.json").write_text(json.dumps(report, indent=1) + "\n")
        if failing and self.mode is Mode.GATE and session.exitstatus == pytest.ExitCode.OK:
            session.exitstatus = pytest.ExitCode.TESTS_FAILED

    def pytest_terminal_summary(self, terminalreporter: pytest.TerminalReporter) -> None:
        if hasattr(terminalreporter.config, "workeroutput"):
            return
        kinds = Counter(site.kind for site in self.sites)
        server = sum(1 for site in self.sites if not site.from_test)
        terminalreporter.write_line(
            f"loop file calls: {len(self.sites)} sites ({server} with no test frame), "
            + ", ".join(f"{kind} {kinds[kind]}" for kind in Kind)
        )
        for site in violations(self.sites, self.exempt):
            terminalreporter.write_line(f"LOOP FILE CALL: {site.key} ({site.event} at {site.leaf}), {self.mode}")


_fstat = os.fstat
"""Taken before `watch_unaudited` wraps it, so the sink's own question raises no event."""


def _regular(fd: int) -> bool:
    """Whether *fd* is a regular file: a pipe, socket or terminal fd never waits on a disk."""
    try:
        return stat.S_ISREG(_fstat(fd).st_mode)
    except OSError:
        return False


def _frames(frame: FrameType | None) -> Generator[Frame]:
    while frame is not None:
        yield Frame(frame.f_code.co_filename, frame.f_lineno, frame.f_code.co_name)
        frame = frame.f_back


def register(config: pytest.Config) -> None:
    """Turn the audit on when `PYNIXD_LOOP_AUDIT` asks for it."""
    mode = Mode(os.environ.get(ENV) or Mode.OFF)
    if mode is Mode.OFF:
        return
    audit = LoopAudit(roots=default_roots(), mode=mode, exempt=EXEMPT)
    loop_files.watch_unaudited()
    loop_files.set_session_sink(audit.sink)
    config.pluginmanager.register(audit, "pynixd-loop-audit")
