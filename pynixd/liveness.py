"""Background liveness tracking: what Nix calls alive, without asking Nix per pass.

**SPIKE STATUS: measurement only.** Nothing plans from this yet; the
collector still asks Nix. What this builds is the evidence that it could
stop: a roots table plus one closure query that Nix agrees with. Cutover is
a later commit, after the differential test holds zero divergence
sustained -- and no delete runs anywhere until then.

Why this shape: both of Nix's query actions run the same mark phase
(`gc.cc` answers `gcReturnDead` and `gcReturnLive` from one `alive` set),
so asking differently saves nothing, and per-root closures recompute the
shared subgraphs thousands of times. One multi-source closure over the
whole graph visits each node once: measured 1.1 s on 187k paths against
6+ minutes for the trace. The query reads Nix's own tables next to
pynixd's two, so the only Python logic is enumerating root links -- the
closure itself stays in SQL, where there is nothing to diverge.

Two tables, two refresh policies. Stable roots (`gcroots`, `profiles`)
change with deployments: diffed against the table, recompute only on
change. Volatile roots (`/proc`, `temproots`) change constantly and never
land in the table at all; they join each closure query as a parameter.
Typical pass cost is the walks plus three cheap queries (roots diff,
fingerprint, skip check); the closure runs only when something moved.

What it deliberately does not mirror: unlinking stale `gcroots/auto`
links (Nix owns that mutation; the mirror only skips them), and censoring
(a tracker has no untrusted reader).
"""

from __future__ import annotations

import errno
import fcntl
import json
import os
import re
import sqlite3
import stat
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

import structlog

from pynixd.db_migrations import LIVENESS_ROOT_TABLE, LIVENESS_STREAK_TABLE, LIVENESS_TABLE
from pynixd.metrics import GC_TEMPROOTS_REAPED

log = structlog.get_logger(__name__)

_STORE_BASENAME = re.compile(r"^[0-9a-z]{32}-")
_MAPS_PATH = re.compile(r"^\s*\S+\s+\S+\s+\S+\s+\S+\s+\S+\s+(/\S+)\s*$")

STABLE_TREES = {"gcroots": "gcroot", "profiles": "profile"}
"""The stable root trees under the state dir, and the kind each names.

Both, because Nix scans both itself: `findRootsNoTemp` reads `gcroots`
and `profiles` (`gc.cc:309`), and no profile path reaches the collector
through `gcroots/auto` -- `addIndirectRoot` serves `--add-root
--indirect`, not profiles. A watcher that misses either tree goes blind
on exactly the deployments that move liveness, so the walk and the watch
below read this one constant and cannot disagree about it."""


def _suppressed(error: OSError) -> bool:
    """The failures Nix walks past while finding roots (`gc.cc`)."""
    return error.errno in (errno.ENOENT, errno.EACCES, errno.ENOTDIR, errno.ESRCH)


def _readlink(path: Path) -> Path | None:
    """The link target, or `None` when it went away underneath the walk."""
    try:
        return Path(os.readlink(path))
    except OSError as exc:
        if _suppressed(exc):
            return None
        raise


def _store_path_of(target: str, store_dir: str) -> str | None:
    """The store path a root target lies in, as `toStorePath(target).first`.

    Nix roots `/nix/store/h-bash` for a link or a running program at
    `/nix/store/h-bash/bin/bash` (`gc.cc:220`, `local-gc.cc:155`). The
    closure query matches whole store paths, so a target with its subpath
    rooted nothing. nixidae/nixidae#1003.
    """
    prefix = store_dir + "/"
    if not target.startswith(prefix):
        return None
    name = target[len(prefix) :].split("/", 1)[0]
    if not _STORE_BASENAME.match(name):
        return None
    return prefix + name


def _walk_link(link: Path, target: Path, found: dict[str, tuple[str, str]], kind: str, store_dir: str) -> None:
    """One symlink: a root when it names the store, else one indirection.

    Nix resolves every target outside the store the same way (`gc.cc:262`),
    absolute or relative: made absolute against the link, and when that names
    a link into the store the store path is the root, attributed to the
    intermediate. The booted and current system links live through this arm --
    `/run/booted-system` is absolute and outside the store, and the store path
    behind it roots tens of thousands of paths. The chain is followed
    lexically and nothing in it is statted: the intermediate itself may
    dangle -- a chroot store keeps its files under its root, so the absolute
    store path behind an outside link never exists on the host, and Nix roots
    it anyway (measured: `--print-live` names it). Stale links simply name
    nothing: Nix unlinks a stale link under `gcroots/auto` here, and the
    mirror never mutates the roots it reads.
    `found` maps the link -- or the intermediate, for an indirect root --
    to `(target, kind)`.
    """
    if target.is_absolute() and str(target).startswith(store_dir + "/"):
        if (store_path := _store_path_of(str(target), store_dir)) is not None:
            found[str(link)] = (store_path, kind)
        return
    resolved = target if target.is_absolute() else link.parent / target
    if not resolved.is_symlink():
        return
    second = _readlink(resolved)
    if second is not None and second.is_absolute() and (store_path := _store_path_of(str(second), store_dir)):
        found[str(resolved)] = (store_path, kind)


def _walk_tree(tree: Path, found: dict[str, tuple[str, str]], kind: str, store_dir: str) -> None:
    """`findRoots` over one directory: links, indirect links, plain files."""
    try:
        entries = list(os.scandir(tree))
    except OSError as exc:
        if _suppressed(exc):
            return
        raise
    for entry in entries:
        try:
            if entry.is_symlink():
                target = _readlink(Path(entry.path))
                if target is not None:
                    _walk_link(Path(entry.path), target, found, kind, store_dir)
            elif entry.is_dir(follow_symlinks=False):
                _walk_tree(Path(entry.path), found, kind, store_dir)
            elif entry.is_file(follow_symlinks=False):
                if _STORE_BASENAME.match(entry.name):
                    found[entry.path] = (f"{store_dir}/{entry.name}", kind)
        except OSError as exc:
            if not _suppressed(exc):
                raise


def _walk_proc_link(path: Path, roots: set[str]) -> None:
    target = _readlink(path)
    if target is not None and target.is_absolute():
        roots.add(str(target))


def _walk_proc(pid: Path, roots: set[str], store_dir: str) -> None:
    """One `/proc/<pid>`: exe, cwd, fds, mapped files, environment matches."""
    _walk_proc_link(pid / "exe", roots)
    _walk_proc_link(pid / "cwd", roots)
    try:
        fds = list(os.scandir(pid / "fd"))
    except OSError as exc:
        if _suppressed(exc):
            return
        raise
    for fd in fds:
        if fd.name.startswith("."):
            continue
        _walk_proc_link(Path(fd.path), roots)
    try:
        maps = (pid / "maps").read_text(errors="replace").splitlines()
    except OSError as exc:
        if _suppressed(exc):
            return
        raise
    for line in maps:
        match = _MAPS_PATH.match(line)
        if match is not None:
            roots.add(match.group(1))
    try:
        environ = (pid / "environ").read_bytes()
    except OSError as exc:
        if _suppressed(exc):
            return
        raise
    pattern = re.compile(re.escape(store_dir).encode() + rb"/[0-9a-z]+[0-9a-zA-Z+\-._?=]*")
    roots.update(match.decode() for match in pattern.findall(environ))


def _walk_runtime_roots(proc_dir: Path, roots: set[str], store_dir: str) -> None:
    """`findRuntimeRootsUnchecked`, without the censoring."""
    try:
        entries = list(os.scandir(proc_dir))
    except OSError as exc:
        if _suppressed(exc):
            return
        raise
    for entry in entries:
        if entry.name.isdigit():
            try:
                _walk_proc(Path(entry.path), roots, store_dir)
            except OSError as exc:
                if not _suppressed(exc):
                    raise
    for name in ("modprobe", "fbsplash", "poweroff_cmd"):
        try:
            content = (proc_dir / "sys" / "kernel" / name).read_text().strip()
        except OSError as exc:
            if _suppressed(exc):
                continue
            raise
        if content:
            roots.add(content)


def _walk_temp_roots_grouped(state_dir: Path) -> tuple[dict[str, set[str]], int]:
    """Temporary roots by owning file, plus the files reaped.

    The same walk as `_walk_temp_roots`, kept per file: one file is one
    process's roots, which is the unit the storage report attributes.
    Files that seed nothing hold no live paths and are left out.
    """
    try:
        entries = list(os.scandir(state_dir / "temproots"))
    except OSError as exc:
        if _suppressed(exc):
            return {}, 0
        raise
    grouped: dict[str, set[str]] = {}
    reaped = 0
    for entry in entries:
        if entry.name.startswith(".") or not entry.is_file(follow_symlinks=False):
            continue
        try:
            int(entry.name)
        except ValueError:
            continue
        try:
            fd = os.open(entry.path, os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW)
        except OSError as exc:
            if exc.errno == errno.ELOOP or _suppressed(exc):
                # Swapped for a link, or gone, between the listing and the
                # open: nothing here to attribute or reap.
                continue
            raise
        try:
            seeds: set[str] = set()
            if _seed_or_reap_temp_file(fd, entry.path, seeds):
                reaped += 1
            elif seeds:
                grouped[entry.name] = seeds
        finally:
            os.close(fd)
    GC_TEMPROOTS_REAPED.inc(reaped)
    return grouped, reaped


def _walk_temp_roots(state_dir: Path, roots: set[str]) -> int:
    """Every path named by the temporary roots files, reaping the stale ones.

    One file per process, its held paths NUL-separated (`gc.cc:163`), the
    owner holding a write lock from creation (`gc.cc:62-65`). A file whose
    lock acquires is a dead owner's: Nix unlinks it and writes `"d"` into
    the unlinked file (`gc.cc:193`) -- the byte is the retry signal, a
    racing owner that opened before the unlink sees a nonzero size and
    recreates its file (`gc.cc:69-74`). The mirror does exactly that, so a
    stale file stops seeding within one wake instead of lingering until
    Nix's next trace. Returns the files unlinked.

    One guard Nix lacks: after the lock acquires, the directory entry must
    still name the locked file (same device and inode). A racing owner
    recreates the name between our open and unlink; without the check we
    would unlink its live file. A mismatch skips both seeding and unlinking
    -- the next pass reads the new file fresh.

    Files outside the protocol -- dotfiles, which Nix skips (`gc.cc:166`),
    and names no process file carries -- are left alone. Nix itself throws
    on those; the mirror neither seeds nor reaps what it cannot attribute.
    """
    grouped, reaped = _walk_temp_roots_grouped(state_dir)
    for seeds in grouped.values():
        roots.update(seeds)
    return reaped


def _seed_or_reap_temp_file(fd: int, path: str, roots: set[str]) -> bool:
    """Seed live temp roots, or unlink a dead owner's file. Returns unlinked.

    A separate function so the race guard has a deterministic test: the
    caller opens `path`, and between that open and this call the name may
    have been recreated. Only `True` unlinks, and only the locked file's
    own name.
    """
    st = os.fstat(fd)
    if not stat.S_ISREG(st.st_mode):
        return False
    try:
        # `flock`, as Nix holds it (`lockFile` at `unix/pathlocks.cc:51`). A
        # POSIX `lockf` never sees a `flock`, so it took every live worker's
        # file for a dead one, unlinked it, and the collector lost those roots.
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        if exc.errno not in (errno.EACCES, errno.EAGAIN):
            raise
        chunks = []
        while True:
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            chunks.append(chunk)
        content = b"".join(chunks)
        roots.update(part.decode(errors="replace") for part in content.split(b"\x00") if part.strip())
        return False
    try:
        current = os.stat(path)
    except OSError as exc:
        if _suppressed(exc):
            # The name went away; the locked content is a dead owner's
            # either way, so it seeds nothing.
            return False
        raise
    if (current.st_dev, current.st_ino) != (st.st_dev, st.st_ino):
        return False
    try:
        os.unlink(path)
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            # Nix's own pass reaped it first; still dead, still unseeded.
            return False
        log.warning("temproot-unlink-failed", path=path, error=str(exc))
        return False
    os.write(fd, b"d")
    return True


def walk_stable(state_dir: Path, store_dir: str) -> dict[str, tuple[str, str]]:
    """The deployment roots: `{link: (target, kind)}` under `gcroots` and `profiles`.

    Compared against the roots table on every pass; only a difference
    recomputes anything. A regular file names its store basename, the way
    `findRoots` parses it against the store directory rather than the link.
    """
    found: dict[str, tuple[str, str]] = {}
    for name, kind in STABLE_TREES.items():
        _walk_tree(state_dir / name, found, kind, store_dir)
    return found


def walk_volatile(state_dir: Path, store_dir: str, proc_dir: Path | None = None) -> set[str]:
    """The living roots: processes and temporary roots, fresh every pass.

    Never stored: they change constantly, and writing them would churn the
    table for no reader. They join each closure query as a parameter.
    """
    targets: set[str] = set()
    _walk_runtime_roots(proc_dir if proc_dir is not None else Path("/proc"), targets, store_dir)
    _walk_temp_roots(state_dir, targets)
    return {path for target in targets if (path := _store_path_of(target, store_dir)) is not None}


def walk_labeled_roots(state_dir: Path, store_dir: str, proc_dir: Path | None = None) -> list[tuple[str, set[str]]]:
    """Every root with its name: stable links, temp files, and `proc` as one.

    A root here is a label and the store paths it names directly; the
    storage report closes each one over references. Stable links label by
    kind and path under the state dir, a temp file labels by its name --
    its owner's pid -- and every process seed shares the `proc` label,
    because per-pid process roots would flap with every fork. A root is
    practically a pointer to store paths; the labels only say which
    pointer each seed set came from.
    """
    labeled: list[tuple[str, set[str]]] = []
    for link, (target, kind) in walk_stable(state_dir, store_dir).items():
        # The intermediate of an indirect root can live anywhere: a
        # home-directory binary registered `--indirect` resolves through a
        # link outside the state dir, and `relative_to` raises on those.
        # The absolute path still names the root exactly.
        try:
            name = str(Path(link).relative_to(state_dir))
        except ValueError:
            name = link
        labeled.append((f"{kind}:{name}", {target}))
    prefix = store_dir + "/"
    grouped, _reaped = _walk_temp_roots_grouped(state_dir)
    for name, seeds in grouped.items():
        kept = {seed for seed in seeds if seed.startswith(prefix)}
        if kept:
            labeled.append((f"temproot:{name}", kept))
    targets: set[str] = set()
    _walk_runtime_roots(proc_dir if proc_dir is not None else Path("/proc"), targets, store_dir)
    proc = {path for target in targets if (path := _store_path_of(target, store_dir)) is not None}
    if proc:
        labeled.append(("proc", proc))
    return labeled


QUERY_LIVE_SET = f"""
WITH RECURSIVE closure(id) AS (
    SELECT vp.id FROM ValidPaths vp
    WHERE vp.path IN (
        SELECT target FROM {LIVENESS_ROOT_TABLE}
        UNION
        SELECT value FROM json_each(?)
    )
    UNION
    SELECT r.reference FROM closure c JOIN Refs r ON c.id = r.referrer
    UNION
    SELECT deriver_vp.id FROM closure c
    JOIN ValidPaths current_vp ON c.id = current_vp.id
    JOIN ValidPaths deriver_vp ON current_vp.deriver = deriver_vp.path
    WHERE current_vp.deriver IS NOT NULL
    UNION
    SELECT r.reference FROM closure c
    JOIN ValidPaths current_vp ON c.id = current_vp.id
    JOIN ValidPaths deriver_vp ON current_vp.deriver = deriver_vp.path
    JOIN Refs r ON deriver_vp.id = r.referrer
    WHERE current_vp.deriver IS NOT NULL
)
SELECT path FROM ValidPaths WHERE id IN (SELECT id FROM closure)
"""


def query_live_set(db_path: Path, volatile: set[str]) -> set[str]:
    """The live set: one closure over the table roots plus `volatile`.

    Read-only, and the only graph query in the design: stable roots come
    from the table, living roots join as a parameter. Seconds on hundreds
    of thousands of paths, where per-root closures take minutes.
    """
    with closing(sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)) as conn:
        return {row[0] for row in conn.execute(QUERY_LIVE_SET, (json.dumps(sorted(volatile)),))}


def db_fingerprint(db_path: Path) -> tuple[int, int]:
    """`(row count, max id)`: any add moves the max, any delete the count.

    Ids never reuse, so the pair answers whether the adjacency changed
    without reading it. Cheap enough to ask on every poll.
    """
    with closing(sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)) as conn:
        count, maximum = conn.execute("SELECT COUNT(*), COALESCE(MAX(id), 0) FROM ValidPaths").fetchone()
    return (int(count), int(maximum))


def refresh_roots(db_path: Path, stable: dict[str, tuple[str, str]]) -> bool:
    """Reconcile the roots table with the walked links. Returns dirtiness.

    Links the walk no longer sees leave; new and retargeted links land.
    Unchanged links are untouched, so an unchanged deployment writes
    nothing. `True` means the roots moved and the live set must recompute.
    """
    with closing(sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)) as conn:
        current = {
            row[0]: (row[1], row[2]) for row in conn.execute(f"SELECT link, target, kind FROM {LIVENESS_ROOT_TABLE}")
        }
    if current == stable:
        return False
    with closing(sqlite3.connect(db_path)) as conn:
        # Wait behind the daemon's writers instead of failing instantly:
        # a reconcile that gives up at once leaves the roots stale for
        # that interval. Same 5s the sync readers already allow.
        conn.execute("PRAGMA busy_timeout = 5000")
        with conn:
            for link in current:
                if link not in stable:
                    conn.execute(f"DELETE FROM {LIVENESS_ROOT_TABLE} WHERE link = ?", (link,))
            conn.executemany(
                f"INSERT OR REPLACE INTO {LIVENESS_ROOT_TABLE} (link, target, kind) VALUES (?, ?, ?)",
                [
                    (link, target, kind)
                    for link, (target, kind) in sorted(stable.items())
                    if current.get(link) != (target, kind)
                ],
            )
    return True


def write_snapshot(db_path: Path, live: set[str], epoch: int) -> None:
    """A complete liveness snapshot tagged with `epoch`, in one transaction.

    Atomicity is the whole leak argument: the replace commits at once, so a
    pass that dies leaves the previous complete snapshot, never a partial
    epoch. Readers take the newest epoch; the prune drops anything older,
    which only an unclean recovery could leave behind. Dead needs no rows:
    it is the complement against `ValidPaths`.
    """
    with closing(sqlite3.connect(db_path)) as conn:
        # Same wait as the roots reconcile above: a snapshot that gives
        # up at once leaves no liveness evidence for that interval.
        conn.execute("PRAGMA busy_timeout = 5000")
        with conn:
            conn.execute(f"DELETE FROM {LIVENESS_TABLE}")
            conn.executemany(
                f"INSERT INTO {LIVENESS_TABLE} (path, epoch) VALUES (?, ?)",
                [(path, epoch) for path in sorted(live)],
            )


def read_snapshot(db_path: Path) -> tuple[set[str], int] | None:
    """The newest snapshot: `(live paths, epoch)`, or `None` when empty."""
    with closing(sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)) as conn:
        row = conn.execute(f"SELECT COALESCE(MAX(epoch), -1) FROM {LIVENESS_TABLE}").fetchone()
        epoch = int(row[0])
        if epoch < 0:
            return None
        live = {r[0] for r in conn.execute(f"SELECT path FROM {LIVENESS_TABLE} WHERE epoch = ?", (epoch,))}
    return (live, epoch)


class RootsTracker:
    """The mirror: roots table current, closure in one query, answer snapshotted.

    `refresh` walks the stable links and diffs the table, walks the living
    roots fresh, and recomputes only when something moved: changed links, a
    changed volatile set, or a changed store. Otherwise the previous live
    set stands, and the pass costs two walks plus three cheap queries. No
    daemon round trip anywhere in it. `differential` compares against what
    Nix answers for the same store, which is the gate the spike must pass
    before anything plans from this.
    """

    def __init__(self, state_dir: Path, store_dir: str, db_path: Path, proc_dir: Path | None = None) -> None:
        self.state_dir = state_dir
        self.store_dir = store_dir
        self.db_path = db_path
        self.proc_dir = proc_dir
        self.fingerprint: tuple[int, int] | None = None
        self.last_volatile: set[str] = set()
        self.live: set[str] = set()
        self.epoch = 0

    def refresh(self) -> set[str]:
        """A liveness pass: sync roots, recompute on change, snapshot."""
        dirty = refresh_roots(self.db_path, walk_stable(self.state_dir, self.store_dir))
        volatile = walk_volatile(self.state_dir, self.store_dir, self.proc_dir)
        fingerprint = db_fingerprint(self.db_path)
        if not dirty and volatile == self.last_volatile and fingerprint == self.fingerprint and self.live:
            return self.live
        self.live = query_live_set(self.db_path, volatile)
        self.epoch = int(time.time())
        write_snapshot(self.db_path, self.live, self.epoch)
        self.last_volatile = volatile
        self.fingerprint = fingerprint
        return self.live

    def differential(self, nix_live: set[str]) -> tuple[set[str], set[str]]:
        """`(only_tracker, only_nix)`: empty on both sides is agreement.

        Compared as strings in both directions, because a divergence either
        way is a fact about the mirror, not about who is right.
        """
        mine = set(self.live)
        theirs = set(nix_live)
        return (mine - theirs, theirs - mine)


@dataclass(frozen=True)
class Streak:
    """Consecutive liveness agreements, as the last check filed them."""

    agreements: int
    divergences: int
    checks: int
    live: int
    updated_at: int


_RECORD_STREAK = f"""
INSERT INTO {LIVENESS_STREAK_TABLE} (id, agreements, divergences, checks, live, updatedAt)
VALUES (1, ?, ?, 1, ?, unixepoch())
ON CONFLICT (id) DO UPDATE SET
    agreements = CASE WHEN ? THEN {LIVENESS_STREAK_TABLE}.agreements + 1 ELSE 0 END,
    divergences = {LIVENESS_STREAK_TABLE}.divergences + CASE WHEN ? THEN 0 ELSE 1 END,
    checks = {LIVENESS_STREAK_TABLE}.checks + 1,
    live = excluded.live,
    updatedAt = unixepoch()
"""


def read_streak(db_path: Path) -> Streak | None:
    """The filed streak, or `None` when no check ever filed one."""
    with closing(sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)) as conn:
        row = conn.execute(
            f"SELECT agreements, divergences, checks, live, updatedAt FROM {LIVENESS_STREAK_TABLE} WHERE id = 1"
        ).fetchone()
    if row is None:
        return None
    return Streak(
        agreements=int(row[0]),
        divergences=int(row[1]),
        checks=int(row[2]),
        live=int(row[3]),
        updated_at=int(row[4]),
    )


def record_check(db_path: Path, agreed: bool, live: set[str]) -> Streak | None:
    """File one check's verdict, and read back the streak it leaves.

    Best-effort, and only ever touches the streak table: a streak the
    database refuses must not fail a check whose differential already
    answered. Returns `None` when nothing was filed.
    """
    if not db_path.exists():
        return None
    agreed_int = 1 if agreed else 0
    try:
        with closing(sqlite3.connect(db_path)) as conn, conn:
            conn.execute(_RECORD_STREAK, (agreed_int, 1 - agreed_int, len(live), agreed_int, agreed_int))
        return read_streak(db_path)
    except (OSError, sqlite3.Error) as exc:
        log.warning("gc_liveness_streak_unrecorded", error=str(exc))
        return None
