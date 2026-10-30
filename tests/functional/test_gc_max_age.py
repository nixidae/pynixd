"""The collector drops dead paths nothing referenced for `gc_max_age` seconds.

Issue #1. `PynixdPathAccess` records when pynixd last saw each path, and
`gc_max_age` on the local store turns that record into a collection rule
that needs no substituter: dead and stale goes, live and fresh stays. Every
test here uses a real store and a real `Collector`, because the rule is
about what Nix calls alive, and a fake answer proves nothing about that.

Two paths of this test alone, with one reference between them, built the
honest way: `LocalStore::findRuntimeRoots` reads `/proc` of the whole
machine, and this store keeps the `/nix/store` prefix, so a test on a real
closure measures the host's libraries instead of the rule (the same reason
`test_gc_substituter` builds its own pair).
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

import aiosqlite

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.daemon_extensions import PynixdGCAction
from pynixd.db_migrations import PATH_ACCESS_TABLE
from pynixd.gc import Collector
from pynixd.store import LocalDBStore, Store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

STALE_AGE = 7200
"""How old the stale rows claim to be. Older than `gc_max_age` below."""

MAX_AGE = 3600
"""The rule under test: dead and unreferenced for this long goes."""

_PARENT = (
    'with import <nixpkgs> {}; runCommand "pynixd-gc-age-parent" {} '
    '"echo ${builtins.toFile "pynixd-gc-age-child" "the child of the age test"} > $out"'
)
"""Two paths of this test alone, with one reference between them.

**Not a package of nixpkgs.** `LocalStore::findRuntimeRoots` reads `/proc` of
the whole machine, and the lab store keeps the `/nix/store` prefix, so every
library a process of the host has mapped is a root here. A test built on a
real closure measures that instead of the rule.
"""


@dataclass
class _Lab:
    store_path: Path
    stale: str
    """Dead, unreferenced for `STALE_AGE`: the plan names it."""
    fresh: str
    """Dead, referenced just now: the plan spares it."""
    parent: str
    child: str
    """Both stale, and `parent` names `child`: the plan names the pair, so
    the delete set stays closed under referrers (`gc.cc:653`)."""
    rooted: str
    """Stale, but a gcroot names it: Nix calls it alive, and the plan spares
    it. Liveness beats age."""


async def _add(store_path: Path, work: Path, name: str, text: str) -> str:
    source = work / name
    source.write_text(text)  # noqa: ASYNC240 -- test setup
    _rc, stdout, _stderr, _both = await run_subproc(
        [str(NIX_BIN), "store", "add-path", "--store", str(store_path), str(source)],
        verbose=False,
    )
    return stdout.strip()


async def _valid(store_path: Path) -> set[str]:
    _rc, stdout, _stderr, _both = await run_subproc(
        [str(NIX_BIN), "path-info", "--store", str(store_path), "--all"],
        verbose=False,
    )
    return {line.strip() for line in stdout.splitlines() if line.strip()}


async def _backdate(store_path: Path, paths: set[str], age: int) -> None:
    """Claim pynixd last saw *paths* `age` seconds ago.

    The tracker writes these rows in production (`flush_references`); the
    test writes them directly, because the rule under test is the planner,
    and waiting out the age would make the suite wait hours. `INSERT OR
    REPLACE`, because a row may already exist from the setup above.
    """
    (db,) = list(store_path.rglob("db.sqlite"))
    async with aiosqlite.connect(db) as conn:
        await conn.executemany(
            f"INSERT OR REPLACE INTO {PATH_ACCESS_TABLE} (path, lastReferencedAt) VALUES (?, ?)",
            [(path, int(time.time()) - age) for path in sorted(paths)],
        )
        await conn.commit()


@asynccontextmanager
async def _pynixd(lab: _Lab) -> AsyncIterator[Server]:
    """A server over the lab store, with the age rule set and no substituter.

    No `gc_defer` store: the point under test is that the age rule needs
    none. `no_probe`: the capability probe adds `probe-feature-*` paths to
    the store, and the plan would then be measured against a store that grew
    under it.
    """
    spec = make_test_spec(
        store_id="local",
        store_path=lab.store_path,
        no_probe=True,
        gc_max_age=MAX_AGE,
        # The tests below run EXECUTE: the permit is this lab run's opt-in.
        gc_allow_execute=True,
    )
    stores: dict[StoreId, Store] = {StoreId("local"): LocalDBStore(spec)}
    async with Server(stores=stores, ssh_port=None, http_port=None) as server:
        _freeze_tracker(server)
        yield server


def _freeze_tracker(server: Server) -> None:
    """Stop the reference tracker for the test.

    The planner reads `PynixdPathAccess`, and the tracker's flush loop would
    rewrite the backdated rows the next time it fires. The tracker has its
    own tests (`test_path_access`); here only the planner is under test.
    Cancelled before any operation runs, so the final flush of `close`
    finds nothing pending and writes nothing.
    """
    db = getattr(server.ctx.local_store, "db", None)
    scope = getattr(db, "flush_scope", None)
    if scope is not None:
        scope.cancel()


async def _lab(tmp_path: Path) -> _Lab:
    """A store with one path for each answer the planner can give."""
    store_path = tmp_path / "store"
    store_path.mkdir()

    stale = await _add(store_path, tmp_path, "stale.txt", "nothing referenced this for a long time\n")
    fresh = await _add(store_path, tmp_path, "fresh.txt", "referenced just now\n")
    rooted = await _add(store_path, tmp_path, "rooted.txt", "stale, but a gcroot names it\n")

    _rc, stdout, _stderr, _both = await run_subproc(
        [str(NIX_BIN), "build", "--impure", "--no-link", "--print-out-paths", "--expr", _PARENT],
        verbose=False,
    )
    parent = stdout.strip()
    await run_subproc([str(NIX_BIN), "copy", "--no-check-sigs", "--to", str(store_path), parent])
    _rc, stdout, _stderr, _both = await run_subproc(
        [str(NIX_BIN), "path-info", "--store", str(store_path), "--recursive", parent],
        verbose=False,
    )
    child = next(line.strip() for line in stdout.splitlines() if line.strip() and line.strip() != parent)

    # A real root, registered the honest way: an indirect gcroot link names
    # the path, and Nix traces it from the store's own roots directory.
    await run_subproc(
        [
            str(NIX_BIN.parent / "nix-store"),
            "--store",
            str(store_path),
            "--realise",
            rooted,
            "--add-root",
            str(tmp_path / "root-link"),
            "--indirect",
        ],
        verbose=False,
    )

    return _Lab(
        store_path=store_path,
        stale=stale,
        fresh=fresh,
        parent=parent,
        child=child,
        rooted=rooted,
    )


async def test_the_plan_names_the_stale_and_spares_the_rest(tmp_path: Path) -> None:
    """Dry-run: the age rule, the fresh exception, and liveness over age."""
    lab = await _lab(tmp_path)
    async with _pynixd(lab) as server:
        await _backdate(lab.store_path, {lab.stale, lab.parent, lab.child, lab.rooted}, STALE_AGE)
        await _backdate(lab.store_path, {lab.fresh}, 0)
        resp = await Collector(server.ctx).run(PynixdGCAction.DRY_RUN)

    assert {str(path) for path in resp.store_paths} == {lab.stale, lab.parent, lab.child}
    assert resp.bytes > 0


async def test_execute_deletes_what_the_plan_named(tmp_path: Path) -> None:
    """The dry-run above is what `EXECUTE` deletes: nothing more, nothing less."""
    lab = await _lab(tmp_path)
    async with _pynixd(lab) as server:
        await _backdate(lab.store_path, {lab.stale, lab.parent, lab.child, lab.rooted}, STALE_AGE)
        await _backdate(lab.store_path, {lab.fresh}, 0)
        planned = await Collector(server.ctx).run(PynixdGCAction.DRY_RUN)
        resp = await Collector(server.ctx).run(PynixdGCAction.EXECUTE)

    assert {str(path) for path in resp.store_paths} == {str(path) for path in planned.store_paths}
    assert await _valid(lab.store_path) == {lab.fresh, lab.rooted}
