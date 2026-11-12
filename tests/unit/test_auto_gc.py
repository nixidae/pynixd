"""pynixd collects under `min-free` as `LocalStore::autoGC` does (`gc.cc:1012`).

`gc-auto.sh:24-46` of Nix's suite sets `--min-free 1K --max-free 2K
--min-free-check-interval 1`, writes 1100 to `_NIX_TEST_FREE_SPACE_FILE`, adds
three dead paths, then writes 100 and waits for exactly one of the three to be
left. These tests hold the same numbers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
import pytest

from nix_daemon_protocol.protocol import GCAction, Verbosity
from pynixd.connection import ClientConn
from pynixd.op_routes import OpRoute
from pynixd.serde import SetOptionsRequest, TimeSpan
from pynixd.wire import BytesWriter
from pynixd.write_router import WriteRouter
from pynixd_store.autogc import FREE_SPACE_FILE, AutoGC, AutoGCSettings, bytes_to_free, free_space, parse_size
from pynixd_store.collect import GCOptions, GCSettings
from pynixd_store.errors import PynixdError
from tests.unit.test_engine_gc import A, B, C, _engine, _Lines, _Store

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping
    from pathlib import Path

GC_AUTO = AutoGCSettings(min_free=1024, max_free=2048, check_interval=1)
"""`gc-auto.sh:71`:`--min-free 1K --max-free 2K --min-free-check-interval 1`."""

LIVE = "/nix/store/00000000000000000000000000000005-live"


@pytest.mark.parametrize(
    ("text", "value"), [("0", 0), ("1100", 1100), ("1K", 1024), ("2k", 2048), ("3M", 3 << 20), ("1T", 1 << 40)]
)
def test_a_size_takes_nix_s_unit_prefixes(text: str, value: int) -> None:
    assert parse_size(text) == value


@pytest.mark.parametrize(("text", "message"), [("1X", "invalid unit specifier 'X'"), ("K", "'' is not an integer")])
def test_a_bad_size_is_refused_in_nix_s_words(text: str, message: str) -> None:
    with pytest.raises(PynixdError, match=message):
        parse_size(text)


def test_an_override_beats_the_store_s_setting_and_a_bad_one_is_ignored() -> None:
    config = {"min-free": {"value": 0}, "max-free": {"value": 4096}, "min-free-check-interval": {"value": 5}}

    got = AutoGCSettings.from_nix_config(config, {"min-free": "1K", "min-free-check-interval": "x"})

    assert got == AutoGCSettings(min_free=1024, max_free=4096, check_interval=5)
    assert not AutoGCSettings.from_nix_config(config, {}).enabled


@pytest.mark.parametrize(
    ("avail", "after", "expected"),
    [
        (1100, float("inf"), None),  # over min-free
        (100, float("inf"), 1948),  # max-free - avail
        (100, 102, None),  # still over 97 % of what the last collection left
        (98, 102, 1950),
    ],
)
def test_a_collection_is_due_by_nix_s_rule(avail: int, after: float, expected: int | None) -> None:
    assert bytes_to_free(GC_AUTO, avail, after) == expected


def test_the_test_hook_names_the_free_space(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "fake-free").write_text("1100\n")
    monkeypatch.setenv(FREE_SPACE_FILE, str(tmp_path / "fake-free"))

    assert free_space(str(tmp_path)) == 1100


async def _awaited(fn: Callable[[], Awaitable[None]]) -> None:
    await fn()


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _auto_gc(
    free: list[int],
    collect: Callable[[int, Mapping[str, str]], Awaitable[None]],
    spawn: Callable[[Callable[[], Awaitable[None]]], object],
    clock: _Clock,
) -> AutoGC:
    async def read() -> int:
        return free[0]

    return AutoGC(free=read, collect=collect, spawn=spawn, clock=clock)


async def test_the_dead_paths_go_until_max_free_is_reached(tmp_path: Path) -> None:
    """`gc-auto.sh:44`: of three dead paths one is left; a rooted path stays."""
    store = _Store(tmp_path)
    for path in (A, B, C):
        store.add(path, size=1000)
    store.add(LIVE, size=1000)
    store.root(LIVE)
    free = [1100]
    clock = _Clock()
    asked: list[int] = []

    async with _engine(store) as engine, anyio.create_task_group() as tg:

        async def collect(max_freed: int, overrides: Mapping[str, str]) -> None:
            asked.append(max_freed)
            await engine.collect_garbage(
                GCOptions(GCAction.DELETE_DEAD, max_freed=max_freed), settings=GCSettings(), report=_Lines().report
            )
            free[0] = 3100

        def spawn(fn: Callable[[], Awaitable[None]]) -> None:
            tg.start_soon(_awaited, fn)

        auto = _auto_gc(free, collect, spawn, clock)
        await auto.check(GC_AUTO, {}, wait=True)
        assert asked == []

        free[0] = 100
        clock.now = 1.0
        with anyio.fail_after(10):
            await auto.check(GC_AUTO, {}, wait=True)

    assert asked == [1948]
    assert sum(store.gone(path) for path in (A, B, C)) == 2
    assert store.valid(LIVE)


async def test_a_check_within_the_interval_reads_nothing() -> None:
    clock = _Clock()
    reads: list[int] = []

    async def read() -> int:
        reads.append(1)
        return 100

    async def collect(max_freed: int, overrides: Mapping[str, str]) -> None:
        raise AssertionError("no collection")

    auto = AutoGC(free=read, collect=collect, spawn=lambda fn: None, clock=clock)
    settings = AutoGCSettings(min_free=50, max_free=2048, check_interval=5)
    await auto.check(settings, {}, wait=False)
    clock.now = 4.9
    await auto.check(settings, {}, wait=False)
    clock.now = 5.0
    await auto.check(settings, {}, wait=False)

    assert len(reads) == 2


async def test_one_collection_runs_and_an_import_waits_for_it() -> None:
    """`autoGC(false)` starts it and returns; `autoGC()` waits for the running one and starts no second."""
    clock = _Clock()
    release = anyio.Event()
    started: list[int] = []
    free = [100]

    async def collect(max_freed: int, overrides: Mapping[str, str]) -> None:
        started.append(max_freed)
        await release.wait()
        free[0] = 3000

    async with anyio.create_task_group() as tg:

        def spawn(fn: Callable[[], Awaitable[None]]) -> None:
            tg.start_soon(_awaited, fn)

        auto = _auto_gc(free, collect, spawn, clock)
        with anyio.fail_after(10):
            await auto.check(GC_AUTO, {}, wait=False)
            waited = anyio.Event()

            async def importer() -> None:
                await auto.check(GC_AUTO, {}, wait=True)
                waited.set()

            tg.start_soon(importer)
            await anyio.wait_all_tasks_blocked()
            assert started == [1948]
            assert not waited.is_set()
            release.set()
            await waited.wait()

    assert started == [1948]


async def test_a_failed_collection_is_logged_and_the_next_check_runs() -> None:
    clock = _Clock()
    calls: list[int] = []

    async def collect(max_freed: int, overrides: Mapping[str, str]) -> None:
        calls.append(max_freed)
        raise PynixdError("disk on fire")

    async with anyio.create_task_group() as tg:

        def spawn(fn: Callable[[], Awaitable[None]]) -> None:
            tg.start_soon(_awaited, fn)

        auto = _auto_gc([100], collect, spawn, clock)
        with anyio.fail_after(10):
            await auto.check(GC_AUTO, {}, wait=True)
            clock.now = 10.0
            await auto.check(GC_AUTO, {}, wait=True)

    assert calls == [1948, 1948]


def _session(overrides: dict[str, str]) -> ClientConn:
    conn = ClientConn(BytesWriter("client"))
    conn.options = SetOptionsRequest(
        keep_failed=False,
        keep_going=False,
        try_fallback=False,
        verbosity=Verbosity.ERROR,
        max_build_jobs=1,
        max_silent_time=TimeSpan(0),
        obsolete_use_build_hook=True,
        build_verbosity=Verbosity.ERROR,
        obsolete_log_type=0,
        obsolete_print_build_trace=0,
        build_cores=1,
        use_substitutes=True,
        overrides=overrides,
    )
    return conn


class _ConfigEngine:
    async def nix_config(self) -> dict[str, object]:
        return {}


GC_AUTO_FLAGS = {"min-free": "1024", "max-free": "2048", "min-free-check-interval": "1"}


async def _imports(route: OpRoute, *sessions: ClientConn) -> list[int]:
    collected: list[int] = []

    async def collect(max_freed: int, overrides: Mapping[str, str]) -> None:
        collected.append(max_freed)

    async with anyio.create_task_group() as tg:

        def spawn(fn: Callable[[], Awaitable[None]]) -> None:
            tg.start_soon(_awaited, fn)

        router = WriteRouter(
            {20: route},
            _ConfigEngine(),  # type: ignore[arg-type]  # an engine that only reads the settings
            None,  # type: ignore[arg-type]  # no daemon: nothing here writes
            "auto-gc-test",
            lambda info: None,
            new_auto_gc=lambda: _auto_gc([100], collect, spawn, _Clock()),
        )
        with anyio.fail_after(10):
            for session in sessions:
                await router.auto_gc_before_import(session)
                await router.auto_gc_before_import(session)
    return collected


async def test_each_session_collects_once_as_each_nix_worker_does() -> None:
    """`gc-auto.sh:70-77`: two clients, so two collections, and one of three dead paths is left."""
    assert await _imports(OpRoute.pynixd, _session(GC_AUTO_FLAGS), _session(GC_AUTO_FLAGS)) == [1948, 1948]


async def test_no_auto_gc_while_the_daemon_collects_or_min_free_is_unset() -> None:
    assert await _imports(OpRoute.daemon, _session(GC_AUTO_FLAGS)) == []
    assert await _imports(OpRoute.pynixd, _session({})) == []
