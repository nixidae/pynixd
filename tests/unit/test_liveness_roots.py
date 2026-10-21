"""The liveness mirror reads roots the way Nix does, and closes them in SQL.

Root enumeration mirrors `gc.cc:231` (`findRoots`) and `local-gc.cc`
(`/proc` scan): links, indirect links, plain files, processes, temporary
roots. A direct link through a missing target names nothing, in Nix and
here; an indirect chain is followed lexically, because a chroot store's
files live under its root and the intermediate dangles by construction.
The closure stays one recursive CTE over Nix's own tables, so the only
Python graph logic is the oracle below, which cross-checks the query on
fixtures small enough to read. Production agreement with Nix is
`test_liveness_tracker`; this file proves the pieces.
"""

from __future__ import annotations

import fcntl
import os
import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

from pynixd.db_migrations import LIVENESS_ROOT_TABLE, LIVENESS_TABLE
from pynixd.liveness import (
    _seed_or_reap_temp_file,
    _walk_temp_roots,
    query_live_set,
    read_snapshot,
    refresh_roots,
    walk_labeled_roots,
    walk_stable,
    walk_volatile,
    write_snapshot,
)
from tests.unit.temp_root_owner import live_temp_root

if TYPE_CHECKING:
    from pathlib import Path

HASH_A = "00000000000000000000000000000001"
HASH_B = "00000000000000000000000000000002"
HASH_C = "00000000000000000000000000000003"
HASH_D = "00000000000000000000000000000004"


def _layout(root: Path) -> tuple[Path, Path, Path, dict[str, str]]:
    """A state dir with every root shape, all final targets real files.

    Final targets exist; only a missing final target names nothing, in Nix
    and here. Intermediates are followed lexically either way: see
    `test_stable_walk_roots_through_a_dangling_intermediate`. `proc` is a
    fake process tree; the last mapping holds the store, its paths, and the
    environment block each name.
    """
    store = root / "store"
    store.mkdir()
    paths = {}
    for hash_part, name in ((HASH_A, "a"), (HASH_B, "b"), (HASH_C, "c"), (HASH_D, "d")):
        path = store / f"{hash_part}-{name}"
        path.write_text("x")  # noqa: ASYNC240 -- test setup
        paths[name] = str(path)

    state = root / "state"
    (state / "gcroots" / "auto" / "sub").mkdir(parents=True)
    (state / "profiles").mkdir(parents=True)
    (state / "temproots").mkdir(parents=True)
    proc = root / "proc"
    (proc / "123" / "fd").mkdir(parents=True)

    auto = state / "gcroots" / "auto"
    (auto / "direct").symlink_to(paths["a"])
    (auto / "dangling").symlink_to(root / "nowhere")
    (auto / "sub" / "nested").symlink_to(paths["b"])
    (auto / "indirect").symlink_to("rel-target")
    (auto / "rel-target").symlink_to(paths["c"])
    (state / "profiles" / "profile").symlink_to(paths["a"])

    pid = proc / "123"
    (pid / "exe").symlink_to(f"{paths['c']}-prog")
    (pid / "cwd").symlink_to("/tmp")
    (pid / "fd" / "0").symlink_to("/dev/null")
    (pid / "fd" / "3").symlink_to(paths["a"])
    (pid / "maps").write_text(  # noqa: ASYNC240 -- test setup
        "7f000000-7f100000 r--p 00000000 00:01 1 /lib/libc.so\n"
        f"7f100000-7f200000 r--p 00000000 00:01 2 {paths['b']}\n"
        "7f200000-7f300000 r--p 00000000 00:01 3\n"
    )
    (pid / "environ").write_bytes(b"PATH=/bin\x00NIX_STORE=" + f"{paths['a']}-env".encode() + b"\x00")
    (proc / "notapid").mkdir()
    (proc / "sys" / "kernel").mkdir(parents=True)
    (proc / "sys" / "kernel" / "modprobe").write_text(f"{paths['c']}-modprobe\n")  # noqa: ASYNC240 -- test setup
    return state, proc, auto, paths


def _reference_bfs(seeds: set[str], refs: dict[str, set[str]], derivers: dict[str, str | None]) -> set[str]:
    """The oracle: the same three branches as the CTE, walked in Python.

    References transitively, derivers, and the references of derivers. Any
    disagreement with `QUERY_LIVE_SET` on a fixture is a fault in one of
    the two, and the fixture is small enough to tell which.
    """
    seen: set[str] = set()
    queue = list(seeds)
    while queue:
        node = queue.pop()
        if node in seen:
            continue
        seen.add(node)
        deriver = derivers.get(node)
        for edge in list(refs.get(node, ())) + ([deriver] if deriver else []):
            if edge not in seen:
                queue.append(edge)
    return seen


def test_stable_walk_names_every_link_shape(tmp_path: Path) -> None:
    state, _proc, auto, paths = _layout(tmp_path)

    found = walk_stable(state, str(tmp_path / "store"))

    assert found == {
        str(auto / "direct"): (paths["a"], "gcroot"),
        str(auto / "sub" / "nested"): (paths["b"], "gcroot"),
        # The intermediate only: Nix attributes an indirect root to the
        # link it resolved through (`gc.cc:275`), not to the link that
        # named it. Retargeting either still changes a row, which is the
        # signal the refresh diffs on.
        str(auto / "rel-target"): (paths["c"], "gcroot"),
        str(state / "profiles" / "profile"): (paths["a"], "profile"),
    }


def test_stable_walk_follows_an_absolute_link_outside_the_store(tmp_path: Path) -> None:
    """`/run/booted-system` is absolute and outside the store, and Nix follows it.

    `gc.cc:262` resolves every target outside the store against the link,
    absolute or relative. Only a relative target took that path here, so
    the booted and current system links rooted nothing and the production
    mirror diverged by tens of thousands of paths.

    Perturbation: restore the early return for absolute targets and this fails.
    """
    state = tmp_path / "state"
    (state / "gcroots" / "auto").mkdir(parents=True)
    outside = tmp_path / "run"
    outside.mkdir()
    store = tmp_path / "store"
    store.mkdir()
    target = store / f"{HASH_A}-sys"
    target.write_text("x")  # noqa: ASYNC240 -- test setup
    (outside / "booted-system").symlink_to(target)
    (state / "gcroots" / "auto" / "sys").symlink_to(outside / "booted-system")

    assert walk_stable(state, str(store)) == {
        str(outside / "booted-system"): (str(target), "gcroot"),
    }


def test_stable_walk_roots_through_a_dangling_intermediate(tmp_path: Path) -> None:
    """An indirect root whose intermediate dangles still roots its store path.

    A chroot store keeps its files under its root, so the absolute store
    path behind an outside link never exists on the host filesystem -- the
    intermediate dangles by construction. Nix follows the chain lexically
    anyway (measured: `--print-live` names the path), so the walk stats
    nothing in it: only the final target's absence names nothing, as the
    `dangling` link above still proves. Requiring the intermediate to exist
    blinds the planner to every such root, and the LRU plan collects a live
    path. Case: the gc_max_age failures.

    Perturbation: gate the second arm of `_walk_link` on `exists()` and this fails.
    """
    state = tmp_path / "state"
    auto = state / "gcroots" / "auto"
    auto.mkdir(parents=True)
    outside = tmp_path / "run"
    outside.mkdir()
    store = tmp_path / "store"
    missing = f"{store}/{HASH_A}-rooted"
    (outside / "root-link").symlink_to(missing)
    (auto / "abc123").symlink_to(outside / "root-link")

    assert walk_stable(state, str(store)) == {
        str(outside / "root-link"): (missing, "gcroot"),
    }


def test_volatile_walk_reads_processes_and_temp_roots(tmp_path: Path) -> None:
    """Processes, mappings, environments -- and both NUL-separated temp roots.

    One temp file holds two paths; a line splitter sees one blob and seeds
    neither. The file has a living owner: an unlocked temp file is a stale
    file (`gc.cc:193`), and the walk under test reaps those.
    Perturbation: split the temp file on lines and `d` leaves this set.
    """
    state, proc, _auto, paths = _layout(tmp_path)
    content = f"{paths['b']}\x00{paths['d']}\x00not-a-path\x00".encode()
    with live_temp_root(state, "999", content):
        assert walk_volatile(state, str(tmp_path / "store"), proc) == {
            f"{paths['c']}-prog",
            paths["a"],
            paths["b"],
            paths["d"],
            f"{paths['a']}-env",
            f"{paths['c']}-modprobe",
        }


def _tiny_db(path: Path) -> None:
    """A -> B -> C, and D derives A: every closure branch on four rows."""
    a = "/fake/store/00000000000000000000000000000001-a"
    b = "/fake/store/00000000000000000000000000000002-b"
    c = "/fake/store/00000000000000000000000000000003-c"
    d = "/fake/store/00000000000000000000000000000004-d"
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute("CREATE TABLE ValidPaths (id INTEGER PRIMARY KEY, path TEXT UNIQUE, deriver TEXT)")
        conn.execute("CREATE TABLE Refs (referrer INTEGER, reference INTEGER)")
        conn.execute(f"CREATE TABLE {LIVENESS_ROOT_TABLE} (link TEXT PRIMARY KEY, target TEXT, kind TEXT)")
        conn.execute(f"CREATE TABLE {LIVENESS_TABLE} (path TEXT PRIMARY KEY, epoch INTEGER)")
        conn.execute("INSERT INTO ValidPaths VALUES (1, ?, ?)", (a, d))
        conn.execute("INSERT INTO ValidPaths VALUES (2, ?, NULL)", (b,))
        conn.execute("INSERT INTO ValidPaths VALUES (3, ?, NULL)", (c,))
        conn.execute("INSERT INTO ValidPaths VALUES (4, ?, NULL)", (d,))
        conn.execute("INSERT INTO Refs VALUES (1, 2)")
        conn.execute("INSERT INTO Refs VALUES (2, 3)")
        conn.execute("INSERT INTO Refs VALUES (4, 2)")


def test_closure_query_matches_the_oracle(tmp_path: Path) -> None:
    """The CTE and the Python walk agree on references, derivers, and both.

    From `A`: `B` and `C` by references, `D` as the deriver, and `B` again
    as the deriver's reference. Seeding `D` alone must also pull `B` and `C`
    but never `A`: referrers are not references.
    """
    db = tmp_path / "db.sqlite"
    _tiny_db(db)
    a = "/fake/store/00000000000000000000000000000001-a"
    b = "/fake/store/00000000000000000000000000000002-b"
    c = "/fake/store/00000000000000000000000000000003-c"
    d = "/fake/store/00000000000000000000000000000004-d"
    refs = {a: {b}, b: {c}, d: {b}}
    derivers: dict[str, str | None] = {a: d}

    assert query_live_set(db, {a}) == _reference_bfs({a}, refs, derivers) == {a, b, c, d}
    assert query_live_set(db, {d}) == _reference_bfs({d}, refs, derivers) == {b, c, d}


def test_refresh_roots_reports_dirtiness(tmp_path: Path) -> None:
    state, _proc, auto, paths = _layout(tmp_path)
    db = tmp_path / "db.sqlite"
    _tiny_db(db)

    assert refresh_roots(db, walk_stable(state, str(tmp_path / "store"))) is True
    assert refresh_roots(db, walk_stable(state, str(tmp_path / "store"))) is False

    (auto / "direct").unlink()
    assert refresh_roots(db, walk_stable(state, str(tmp_path / "store"))) is True
    with closing(sqlite3.connect(db)) as conn:
        # Three, not four: the indirect link folds into its intermediate's
        # row, so `direct`, `nested` and the profile remain.
        assert conn.execute(f"SELECT COUNT(*) FROM {LIVENESS_ROOT_TABLE}").fetchone()[0] == 3


def test_snapshot_round_trips_and_replaces(tmp_path: Path) -> None:
    db = tmp_path / "db.sqlite"
    _tiny_db(db)
    a = "/fake/store/00000000000000000000000000000001-a"
    b = "/fake/store/00000000000000000000000000000002-b"
    c = "/fake/store/00000000000000000000000000000003-c"

    assert read_snapshot(db) is None
    write_snapshot(db, {a, b}, 7)
    assert read_snapshot(db) == ({a, b}, 7)
    write_snapshot(db, {b, c}, 9)
    assert read_snapshot(db) == ({b, c}, 9)
    with closing(sqlite3.connect(db)) as conn:
        assert conn.execute(f"SELECT COUNT(*) FROM {LIVENESS_TABLE}").fetchone()[0] == 2


def test_live_locked_temp_file_seeds_and_survives(tmp_path: Path) -> None:
    """A lock held by a living process is a live root, and the file stays.

    The reaper only unlinks what it can lock (`gc.cc:193`); a refused lock
    is a living owner. The pid in the name is arbitrary -- only the lock
    speaks.

    Perturbation: unlink after a refused lock and the file vanishes here.
    """
    state = tmp_path / "state"
    (state / "temproots").mkdir(parents=True)
    target = f"{tmp_path}/store/{HASH_A}-a"
    with live_temp_root(state, "4242", f"{target}\x00".encode()):
        roots: set[str] = set()
        assert _walk_temp_roots(state, roots) == 0
        assert roots == {target}
        assert (state / "temproots" / "4242").exists()


def test_dead_unlocked_temp_file_reaped_and_unseeded(tmp_path: Path) -> None:
    """No lock means the owner is dead: unlink, mark, seed nothing.

    Exact replication of `findTempRoots` (`gc.cc:193`). The dead paths must
    not seed -- that is what clears the only-tracker transients, instead of
    carrying them until Nix's next trace.

    Perturbation: seed before locking and the dead path stays in the set.
    """
    state = tmp_path / "state"
    temp = state / "temproots"
    temp.mkdir(parents=True)
    stale = temp / "4243"
    stale.write_bytes(f"{tmp_path}/store/{HASH_B}-b\x00".encode())  # noqa: ASYNC240 -- test setup
    roots: set[str] = set()
    assert _walk_temp_roots(state, roots) == 1
    assert roots == set()
    assert not stale.exists()


def test_dotfiles_and_foreign_names_are_left_alone(tmp_path: Path) -> None:
    """Keepalive files and non-protocol names are neither read nor reaped.

    Nix skips dotfiles (`gc.cc:166`); a name no process file carries is not
    ours to judge. Both are unlocked, so a lock-only rule would reap them --
    the protocol check is what spares them.

    Perturbation: drop the name check and both files vanish here.
    """
    state = tmp_path / "state"
    temp = state / "temproots"
    temp.mkdir(parents=True)
    keep = temp / ".keep"
    keep.write_bytes(b"/store/dead\x00")  # noqa: ASYNC240 -- test setup
    foreign = temp / "README"
    foreign.write_bytes(b"/store/dead\x00")  # noqa: ASYNC240 -- test setup
    roots: set[str] = set()
    assert _walk_temp_roots(state, roots) == 0
    assert roots == set()
    assert keep.exists() and foreign.exists()


def test_swapped_name_is_neither_seeded_nor_unlinked(tmp_path: Path) -> None:
    """A name recreated after the open belongs to someone new.

    A racing owner recreates the name between open and lock; Nix's own pass
    can unlink that fresh file here. The device/inode check sees the swap
    and touches neither -- the next pass reads the new file, whose living
    owner holds its lock, fresh.

    Perturbation: drop the check and the new file is gone.
    """
    temp = tmp_path / "state" / "temproots"
    temp.mkdir(parents=True)
    stale = temp / "4244"
    stale.write_bytes(b"/store/dead\x00")  # noqa: ASYNC240 -- test setup
    fd = os.open(stale, os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        # Same-process locks never conflict: acquiring here is exactly what
        # a dead owner looks like to the reaper.
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        live = tmp_path / "fresh"
        live.write_bytes(b"/store/live\x00")  # noqa: ASYNC240 -- test setup
        os.replace(live, stale)
        roots: set[str] = set()
        assert _seed_or_reap_temp_file(fd, str(stale), roots) is False
        assert roots == set()
        assert stale.read_bytes() == b"/store/live\x00"  # noqa: ASYNC240 -- test assertion
    finally:
        os.close(fd)


def test_walk_labeled_roots_names_each_shape(tmp_path: Path) -> None:
    """Links by kind and path, temp files by owner, processes as one `proc`.

    A root is a label on a seed set: the report closes each one, so the
    walk only names direct targets. The indirect link labels its
    intermediate, like `walk_stable` attributes it, and the temp file
    needs its living owner, like every other temp test.
    """
    state, proc, _auto, paths = _layout(tmp_path)
    content = f"{paths['b']}\x00{paths['d']}\x00".encode()
    with live_temp_root(state, "999", content):
        labeled = walk_labeled_roots(state, str(tmp_path / "store"), proc)

    by_label = {label: seeds for label, seeds in labeled}
    assert by_label["gcroot:gcroots/auto/direct"] == {paths["a"]}
    assert by_label["gcroot:gcroots/auto/sub/nested"] == {paths["b"]}
    assert by_label["gcroot:gcroots/auto/rel-target"] == {paths["c"]}
    assert by_label["profile:profiles/profile"] == {paths["a"]}
    assert by_label["temproot:999"] == {paths["b"], paths["d"]}
    assert {f"{paths['c']}-prog", paths["a"]} <= by_label["proc"]


def test_walk_labeled_roots_labels_an_escaped_intermediate(tmp_path: Path) -> None:
    """An indirect root through a link outside the state dir keeps its absolute name.

    `walk_stable` attributes the intermediate (`gc.cc:275`), and an
    indirect root can name a link anywhere: a home-directory binary
    registered `--indirect` resolves through a symlink outside the state
    dir. The label falls back to the absolute path instead of killing the
    whole report, which is what `relative_to` unconditionally would do to
    it on a real machine.

    Perturbation: label by relative path unconditionally and this raises.
    """
    state = tmp_path / "state"
    (state / "gcroots" / "auto").mkdir(parents=True)
    (state / "profiles").mkdir(parents=True)
    (state / "temproots").mkdir(parents=True)
    store = tmp_path / "store"
    store.mkdir()
    target = store / f"{HASH_A}-a"
    target.write_text("x")  # noqa: ASYNC240 -- test setup
    outside = tmp_path / "bin"
    outside.mkdir()
    (outside / "tool").symlink_to(target)
    (state / "gcroots" / "auto" / "ind").symlink_to(outside / "tool")

    labeled = walk_labeled_roots(state, str(store), tmp_path / "nproc")

    assert labeled == [(f"gcroot:{outside / 'tool'}", {str(target)})]


def test_a_target_inside_a_store_path_roots_that_store_path(tmp_path: Path) -> None:
    """`toStorePath(target).first`: a program at `<path>/bin/prog` roots `<path>`.

    `local-gc.cc:155` reduces every runtime root this way, and `gc.cc:220`
    every gcroot. The closure query matches whole store paths, so a target
    with its subpath rooted nothing, and a running program's own package
    was planned for deletion. nixidae/nixidae#1003.
    """
    state, proc, auto, paths = _layout(tmp_path)
    store = str(tmp_path / "store")
    (proc / "123" / "exe").unlink()
    (proc / "123" / "exe").symlink_to(f"{paths['d']}/bin/prog")
    (auto / "into").symlink_to(f"{paths['b']}/share/doc")

    assert paths["d"] in walk_volatile(state, store, proc)
    assert walk_stable(state, store)[str(auto / "into")][0] == paths["b"]


def test_pynixd_own_temp_roots_files_seed_the_walk(tmp_path: Path) -> None:
    """`findTempRoots` reads every file but a dotfile (`gc.cc:166`).

    pynixd names its own files `pynixd-<pid>-<n>` (`temp_roots.py`), and
    the walk kept pid names only, so a root a client asked pynixd for never
    reached the plan. nixidae/nixidae#1007.
    """
    state, proc, _auto, paths = _layout(tmp_path)
    with live_temp_root(state, "pynixd-4242-1", f"{paths['d']}\x00".encode()):
        assert paths["d"] in walk_volatile(state, str(tmp_path / "store"), proc)
