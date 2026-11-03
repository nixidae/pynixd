"""The building blocks of `LocalStore::collectGarbage` (`gc.cc:356`): the lock, the roots socket, deletion, the links sweep.

Every function here but `RootsServer` blocks on the file system. The engine
calls them on a worker thread. Not `gc.py`: that module plans the passes of
op 101, a collector of pynixd's own.
"""

from __future__ import annotations

import fcntl
import os
import stat
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import anyio
from anyio.streams.buffered import BufferedByteReceiveStream

from nix_daemon_protocol.protocol import GCAction, Verbosity

from .errors import PynixdError
from .hintfmt import magenta
from .temp_roots import COLLECTOR_ACK
from .utils import NIX32_CHARS

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping
    from pathlib import Path
    from typing import Any

    from anyio.abc import SocketStream, TaskStatus

    Report = Callable[[Verbosity, str], Awaitable[None]]

HASH_LEN = 32
"""`StorePath::HashLen`."""

_MAX_NAME_LEN = 211
_NAME_CHARS = frozenset("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+-._?=")


class CycleError(PynixdError):
    """`topoSortPaths` found a cycle in the references, and the message is Nix's (`misc.cc:350`)."""


@dataclass(frozen=True)
class GCOptions:
    """`GCOptions` (`gc-store.hh`): what to collect. `paths` is `None` for the whole store."""

    action: GCAction
    paths: frozenset[str] | None = None
    delete_referrers: bool = False
    max_freed: int = 2**64 - 1


_TRUE = frozenset({"true", "yes", "1"})
_FALSE = frozenset({"false", "no", "0"})


def _parse_bool(name: str, value: str) -> bool:
    """`BaseSetting<bool>::parse` (`config.cc`)."""
    if value in _TRUE:
        return True
    if value in _FALSE:
        return False
    raise PynixdError(f"Boolean setting '{name}' has invalid value '{value}'")


@dataclass(frozen=True)
class GCSettings:
    """What `collectGarbage` reads of the settings (`local-settings.hh:53`), a trusted client's overrides applied."""

    keep_outputs: bool = False
    keep_derivations: bool = True

    @classmethod
    def from_nix_config(cls, config: Mapping[str, Any], overrides: Mapping[str, str]) -> GCSettings:
        def value(names: tuple[str, ...], default: bool) -> bool:
            for name in names:
                if name in overrides:
                    return _parse_bool(name, overrides[name])
            entry = config.get(names[0])
            return bool(entry["value"]) if entry is not None else default

        return cls(
            keep_outputs=value(("keep-outputs", "gc-keep-outputs"), False),
            keep_derivations=value(("keep-derivations", "gc-keep-derivations"), True),
        )


@dataclass
class GCResults:
    """`GCResults`: the paths deleted (or found live or dead), and the bytes freed."""

    paths: set[str] = field(default_factory=set)
    bytes_freed: int = 0


def try_gc_lock_exclusive(fd: int) -> bool:
    """Try the big garbage-collector lock for writing without waiting (`FdLock`, `pathlocks.cc:146`).

    False means the caller reports the wait, then calls `wait_gc_lock_exclusive`.
    """
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    return True


def wait_gc_lock_exclusive(fd: int) -> None:
    fcntl.flock(fd, fcntl.LOCK_EX)


def store_base_name(path: str, store_dir: str) -> str | None:
    """`maybeParseStorePath` (`path.cc:49`): the base name of a store path, or `None` for anything else."""
    if not path.startswith(f"{store_dir}/"):
        return None
    base = path[len(store_dir) + 1 :]
    if "/" in base or len(base) < HASH_LEN + 1 or base[HASH_LEN] != "-":
        return None
    if any(char not in NIX32_CHARS for char in base[:HASH_LEN]):
        return None
    name = base[HASH_LEN + 1 :]
    if not name or len(name) > _MAX_NAME_LEN or any(char not in _NAME_CHARS for char in name):
        return None
    if name in {".", ".."} or name.startswith((".-", "..-")):
        return None
    return base


def store_dir_entries(real_store_dir: Path, links_name: str) -> list[str]:
    """The names in the store directory, in `readdir` order, without `.`, `..` and the links directory (`gc.cc:829`)."""
    with os.scandir(real_store_dir) as entries:
        return [entry.name for entry in entries if entry.name != links_name]


def lock_tmp_dir(path: Path) -> int | None:
    """The write lock of a `tmp-` directory, or `None` when another process holds it (`gc.cc:549`).

    The caller keeps the descriptor open while it deletes, and closes it after.
    A directory that cannot be opened is skipped too.
    """
    try:
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    except OSError:
        return None
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        return None
    return fd


def delete_path(path: Path) -> int:
    """`deletePath` with its count of bytes freed (`unix/file-system.cc:155`).

    A file counts its size when it has one or two links: two is an optimised
    file whose `.links` entry the sweep removes next. A directory gets
    `u+rwx` before the descent. A name that is already gone counts nothing.
    The first failure to unlink is raised after the walk finishes; any other
    failure at once. Each raises Nix's `SysError` text.
    """
    try:
        parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    except FileNotFoundError:
        return 0
    except OSError as exc:
        raise sys_error("opening directory", path.parent, exc) from exc
    failures: list[DeleteError] = []
    try:
        return _delete_at(parent, path.name, path, failures)
    finally:
        os.close(parent)
        if failures:
            raise failures[0]


def _delete_at(parent: int, name: str, path: Path, failures: list[DeleteError]) -> int:
    try:
        st = os.stat(name, dir_fd=parent, follow_symlinks=False)
    except FileNotFoundError:
        return 0
    freed = 0
    is_dir = stat.S_ISDIR(st.st_mode)
    if not is_dir and st.st_nlink in {1, 2}:
        freed += st.st_size
    if is_dir:
        mask = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
        if st.st_mode & mask != mask:
            try:
                os.chmod(name, st.st_mode | mask, dir_fd=parent, follow_symlinks=False)
            except OSError as exc:
                error = sys_error("fchmodat2", path, exc)
                raise error.with_trace(f"while making directory {path_fmt(path)} accessible for deletion") from exc
        try:
            fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
        except OSError as exc:
            raise sys_error("opening directory", path, exc) from exc
        try:
            for child in os.listdir(fd):
                freed += _delete_at(fd, child, path / child, failures)
        finally:
            os.close(fd)
    try:
        if is_dir:
            os.rmdir(name, dir_fd=parent)
        else:
            os.unlink(name, dir_fd=parent)
    except FileNotFoundError:
        pass
    except OSError as exc:
        failures.append(sys_error("cannot unlink", path, exc))
    return freed


class DeleteError(PynixdError):
    """A `SysError` of `deletePath` (`unix/file-system.cc:155`), with Nix's text."""


def path_fmt(path: Path | str) -> str:
    """`PathFmt` inside `HintFmt`: the path in double quotes, in magenta."""
    return magenta(f'"{path}"')


def sys_error(what: str, path: Path, exc: OSError) -> DeleteError:
    """`SysError("<what> %1%", PathFmt(path))` (`error.hh:321`): the hint, then the system's reason as an argument."""
    return DeleteError(f"{what} {path_fmt(path)}: {magenta(exc.strerror)}")


def topo_sort_paths(paths: set[str], references: Mapping[str, set[str]]) -> list[str]:
    """`Store::topoSortPaths` (`misc.cc:337`, `topo-sort.hh:23`): a referrer before each path it references.

    Nix's depth-first walk in its order: the set and each path's references
    sorted by base name. A path with no references here is an invalid one.
    """
    ordered: list[str] = []
    visited: set[str] = set()
    parents: set[str] = set()

    def visit(path: str, parent: str | None) -> None:
        if path in parents:
            raise CycleError(f"cycle detected in the references of '{parent}' from '{path}'")
        if path in visited:
            return
        visited.add(path)
        parents.add(path)
        for child in sorted(references.get(path, set()), key=_base_name):
            if child != path and child in paths:
                visit(child, path)
        ordered.append(path)
        parents.discard(path)

    for path in sorted(paths, key=_base_name):
        visit(path, None)
    ordered.reverse()
    return ordered


def _base_name(path: str) -> str:
    return path.rsplit("/", 1)[-1]


@dataclass
class LinksSweep:
    """What the `.links` sweep of `collectGarbage` (`gc.cc:896`) removed and measured."""

    unlinked: list[Path] = field(default_factory=list)
    actual_size: int = 0
    unshared_size: int = 0
    overhead: int = 0

    @property
    def saving(self) -> int:
        """What hard linking saves, as `note: hard linking is currently saving` prints it."""
        return self.unshared_size - self.actual_size - self.overhead


def sweep_unused_links(links_dir: Path) -> LinksSweep:
    """Unlink each `.links` entry no store file shares any more: a link count of 1 (`gc.cc:896`).

    Nix throws when the directory is missing; so does this.
    """
    sweep = LinksSweep()
    for name in os.listdir(links_dir):
        path = links_dir / name
        st = os.lstat(path)
        if st.st_nlink != 1:
            sweep.actual_size += st.st_size
            sweep.unshared_size += (st.st_nlink - 1) * st.st_size
            continue
        os.unlink(path)
        sweep.unlinked.append(path)
    sweep.overhead = os.stat(links_dir).st_blocks * 512
    return sweep


@dataclass
class RootsServer:
    """The collector's end of `<state>/gc-socket/socket` (`gc.cc:400`).

    A process that adds a temporary root while the collector holds the big
    lock cannot write it where the collector already read, so it sends the
    path here, one line each, and waits for one byte. The answer waits while
    the collector deletes that very path (`pending`), so the client never
    re-creates a path mid-deletion. The collector and every connection run
    on one event loop, so `temp_roots` and `pending` need no lock between a
    check and a change; the condition only wakes a waiting connection.
    """

    store_dir: str
    report: Report
    #: The hash parts of the temporary roots, as Nix keeps them to ignore suffixes such as `.lock`.
    temp_roots: set[str] = field(default_factory=set)
    _pending: str | None = None
    _changed: anyio.Condition = field(default_factory=anyio.Condition)

    async def serve(self, socket_path: Path, *, task_status: TaskStatus[None] = anyio.TASK_STATUS_IGNORED) -> None:
        """Accept clients until cancelled. The socket exists when `start` returns."""
        await anyio.Path(socket_path.parent).mkdir(parents=True, exist_ok=True)
        listener = await anyio.create_unix_listener(socket_path, mode=0o666)
        async with listener:
            task_status.started()
            await listener.serve(self._client)

    async def _client(self, stream: SocketStream) -> None:
        async with stream:
            lines = BufferedByteReceiveStream(stream)
            while True:
                try:
                    line = await lines.receive_until(b"\n", 65536)
                except (anyio.EndOfStream, anyio.IncompleteRead, anyio.DelimiterNotFound, OSError):
                    return
                base = store_base_name(line.decode(errors="replace"), self.store_dir)
                if base is None:
                    await self.report(Verbosity.ERROR, "received garbage instead of a root from client")
                else:
                    await self.report(Verbosity.DEBUG, f"got new GC root '{line.decode()}'")
                    hash_part = base[:HASH_LEN]
                    self.temp_roots.add(hash_part)
                    async with self._changed:
                        while self._pending == hash_part:
                            await self._changed.wait()
                try:
                    await stream.send(COLLECTOR_ACK)
                except (anyio.BrokenResourceError, anyio.ClosedResourceError, OSError):
                    return

    def hold(self, hash_part: str) -> bool:
        """Mark *hash_part* as being deleted, unless it is a temporary root. True when the caller may go on."""
        if hash_part in self.temp_roots:
            return False
        self._pending = hash_part
        return True

    async def release(self) -> None:
        """No path is being deleted: wake each client waiting for one."""
        self._pending = None
        async with self._changed:
            self._changed.notify_all()
