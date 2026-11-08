"""A path into the store as `LocalStore::addToStore` (`local-store.cc:1046` of Nix 2.35) and
`addToStoreFromDump` (`local-store.cc:1151`) put one there.

The file system half is here and synchronous: the path lock, the temporary
directory, the hash of the restored tree, and `canonicalisePathMetaData`. An
async caller runs it on a worker thread. `register.py` writes the rows.

**Every lock is one Nix honours**, because a daemon still writes the same
store: `<path>.lock` with `flock`, and a `tmp-*` directory locked while it
holds a path, which the GC skips (`gc.cc:551`).
"""

from __future__ import annotations

import base64
import contextlib
import errno
import fcntl
import hashlib
import itertools
import os
import random
import shutil
import stat
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import anyio
import anyio.to_thread
import structlog

from nix_daemon_protocol.content_address import ContentAddress as WireContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time

from . import optimise
from .builder.refscan import scan_for_references
from .content_address import ContentAddress, Method, content_addressed_path, parse_any_sha256
from .errors import PynixdError
from .hintfmt import magenta
from .nar_fs import BadArchive, dump_path, restore_path
from .signing import check_signatures, key_settings, public_keys
from .source import BlockingReader
from .utils import MTIME_STORE, nix32_encode

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Iterator, Mapping
    from pathlib import Path

    from .engine import StoreEngine
    from .source import Source

log = structlog.get_logger(__name__)

IGNORED_ACLS = frozenset({"security.selinux", "system.nfs4_acl", "security.csm"})
"""The default of `ignored-acls` (`local-settings.hh`)."""

_COUNTER = itertools.count(random.getrandbits(32))


class AddError(PynixdError):
    """Nix refuses this import, and the message is Nix's."""


@dataclass(frozen=True)
class ImportSettings:
    """What `addToStore` reads of the daemon's settings, from `nix config show --json`.

    `require-sigs` is the daemon's alone: `runDaemon` builds the store config
    before a client connects (`daemon.cc:489`). The keys are read at the first
    import, after `SetOptions`, so a trusted client's overrides apply to them.
    """

    require_sigs: bool = True
    trusted_public_keys: tuple[str, ...] = ()
    secret_key_files: tuple[str, ...] = ()
    auto_optimise_store: bool = False
    fsync_store_paths: bool = False
    sync_before_registering: bool = False
    ignored_acls: frozenset[str] = IGNORED_ACLS
    #: The daemon's own `experimental-features`; a client's are ignored (`daemon.cc:278`).
    git_hashing: bool = False

    @classmethod
    def from_nix_config(cls, config: Mapping[str, Any]) -> ImportSettings:
        def value(name: str) -> Any:
            return config[name]["value"]

        return cls(
            require_sigs=bool(value("require-sigs")),
            trusted_public_keys=tuple(value("trusted-public-keys")),
            secret_key_files=tuple(value("secret-key-files")),
            auto_optimise_store=bool(value("auto-optimise-store")),
            fsync_store_paths=bool(value("fsync-store-paths")),
            # Not on Windows (`local-settings.hh:252`).
            sync_before_registering=bool(config.get("sync-before-registering", {}).get("value", False)),
            ignored_acls=frozenset(value("ignored-acls")),
            git_hashing="git-hashing" in _words(value("experimental-features")),
        )


def _words(raw: Any) -> list[str]:
    return raw.split() if isinstance(raw, str) else list(raw or ())


@dataclass
class PathLock:
    """`PathLocks` of one path (`pathlocks.cc:71`): `flock` on `<path>.lock`, retried while stale."""

    path: Path
    _fd: int = field(default=-1, init=False)

    @property
    def lock_path(self) -> str:
        return f"{self.path}.lock"

    def acquire(self) -> None:
        """Blocks until the lock is held. Call it from a worker thread."""
        self._lock(wait=True)

    def try_acquire(self) -> bool:
        """`lockPaths(..., wait = false)`: `False` when another process holds the lock."""
        return self._lock(wait=False)

    def _lock(self, *, wait: bool) -> bool:
        while True:
            fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX if wait else fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                os.close(fd)
                return False
            # A holder that finished wrote "d" and unlinked the file: this lock guards nothing.
            if os.fstat(fd).st_size == 0:
                self._fd = fd
                return True
            os.close(fd)

    def release(self, *, delete: bool) -> None:
        """`PathLocks::unlock`; *delete* is `setDeletion(true)`, which writes the stale token first."""
        if self._fd < 0:
            return
        if delete:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(self.lock_path)
            os.write(self._fd, b"d")
        os.close(self._fd)
        self._fd = -1


@dataclass
class TempDir:
    """`LocalStore::createTempDirInStore` (`local-store.cc:1335`): `tmp-<pid>-<n>`, 0700, locked."""

    path: Path
    fd: int

    @classmethod
    def create(cls, real_store_dir: Path) -> TempDir:
        while True:
            path = real_store_dir / f"tmp-{os.getpid()}-{next(_COUNTER) & 0xFFFFFFFF}"
            try:
                os.mkdir(path, 0o700)
            except FileExistsError:
                continue
            try:
                fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
            except FileNotFoundError:
                # The GC took it between the mkdir and the open.
                continue
            fcntl.flock(fd, fcntl.LOCK_EX)
            if path.exists():
                return cls(path, fd)
            os.close(fd)

    def remove(self) -> None:
        delete_path(self.path)
        os.close(self.fd)


def delete_path(path: Path) -> None:
    """`deletePath`: the whole tree, made writable first, and no error when it is absent."""
    if not os.path.lexists(path):
        return
    if os.path.islink(path) or not os.path.isdir(path):
        os.unlink(path)
        return
    for root, dirs, _ in os.walk(path):
        os.chmod(root, 0o700)
        for name in dirs:
            child = os.path.join(root, name)
            if not os.path.islink(child):
                os.chmod(child, 0o700)
    shutil.rmtree(path)


class HashingReader:
    """The `TeeSource` into a `HashSink` of `addToStore`: what the restore reads, hashed and counted."""

    def __init__(self, read: Callable[[int], bytes], algo: str = "sha256") -> None:
        self._read = read
        self.hasher = hashlib.new(algo)
        self.size = 0

    def __call__(self, n: int) -> bytes:
        data = self._read(n)
        self.hasher.update(data)
        self.size += len(data)
        return data


class HashModulo:
    """`HashModuloSink` (`references.cc:104`): a hash with each self-reference zeroed, then `|<offset>` per match.

    `RewritingSink` keeps the last `len - 1` bytes back, so a match across two
    pieces is found; matches never overlap, since zeros are not Nix32.

    NIX-DEFECT (#23): Nix 2.34's `RewritingSink::operator()` calls `rewriteStrings(s, rewrites)` without its
    `matches` (`references.cc:77` of 2.34), so `HashModuloSink::finish` hashes no offset. A tree with its
    self-references zeroed and one where they are already zeros then share a hash, which the offsets exist to
    prevent. Nix 2.35 records them. Without *positions* pynixd copies 2.34, so a 2.34 store's outputs land
    where its own Nix puts them and its imports pass its own check. Fixed upstream since 2.35.
    """

    def __init__(self, algo: str, modulus: bytes, *, positions: bool = True) -> None:
        self._positions = positions
        self._hash = hashlib.new(algo)
        self._modulus = modulus
        self._zero = b"\0" * len(modulus)
        self._prev = b""
        self._pos = 0
        self._matches: list[int] = []

    def update(self, data: bytes) -> None:
        s = bytearray(self._prev + data)
        j = s.find(self._modulus)
        while j >= 0:
            self._matches.append(self._pos + j)
            s[j : j + len(self._modulus)] = self._zero
            j = s.find(self._modulus, j)
        keep = len(self._modulus) - 1
        self._prev = bytes(s[len(s) - keep :]) if len(s) > keep else bytes(s)
        consumed = len(s) - len(self._prev)
        self._pos += consumed
        self._hash.update(s[:consumed])

    def finish(self) -> bytes:
        self._pos += len(self._prev)
        self._hash.update(self._prev)
        self._prev = b""
        for pos in sorted(set(self._matches)) if self._positions else ():
            self._hash.update(f"|{pos}".encode())
        return self._hash.digest()


def content_hash(
    ca: ContentAddress, path: Path, hash_part: str, dump: Callable[[Path], Iterable[bytes]], *, positions: bool = True
) -> bytes:
    """The hash `addToStore` takes of the restored tree to check its address (`local-store.cc:1099`).

    *dump* is `nar_fs.dump_path`; a flat or text address hashes the file's bytes, and a Git
    address takes `git::dumpHash`, with no modulo.
    """
    if ca.method is Method.GIT:
        return git_hash(path, ca.algo)[1]
    sink = HashModulo(ca.algo, hash_part.encode(), positions=positions)
    if ca.method is Method.NAR:
        for piece in dump(path):
            sink.update(piece)
    elif ca.method in (Method.FLAT, Method.TEXT):
        if not stat.S_ISREG(os.lstat(path).st_mode):
            raise AddError(f"file '{magenta(path)}' is not a regular file")
        with open(path, "rb") as file:
            while piece := file.read(64 * 1024):
                sink.update(piece)
    else:
        raise AddError(f"pynixd does not hash the content address method '{ca.method}'")
    return sink.finish()


def git_hash(path: Path, algo: str) -> tuple[int, bytes]:
    """`git::dumpHash` (`libutil/git.cc:314`): the Git mode and object hash of *path*.

    A tree sorts its entries by name as bytes, a directory's name with a trailing `/`.
    """
    st = os.lstat(path)
    h = hashlib.new(algo)
    if stat.S_ISREG(st.st_mode):
        h.update(b"blob %d\0" % st.st_size)
        with open(path, "rb") as file:
            while piece := file.read(64 * 1024):
                h.update(piece)
        return (0o100755 if st.st_mode & stat.S_IXUSR else 0o100644), h.digest()
    if stat.S_ISLNK(st.st_mode):
        target = os.readlink(os.fsencode(path))
        h.update(b"blob %d\0" % len(target) + target)
        return 0o120000, h.digest()
    if stat.S_ISDIR(st.st_mode):
        entries: dict[bytes, tuple[bytes, int, bytes]] = {}
        for name in os.listdir(os.fsencode(path)):
            mode, digest = git_hash(path / os.fsdecode(name), algo)
            entries[name + b"/" if mode == 0o40000 else name] = (name, mode, digest)
        body = b"".join(b"%o %s\0" % (mode, name) + digest for _, (name, mode, digest) in sorted(entries.items()))
        h.update(b"tree %d\0" % len(body) + body)
        return 0o40000, h.digest()
    raise AddError(f"file '{magenta(str(path))}' has an unsupported type")


def _walk(path: str) -> Iterator[str]:
    yield path
    if stat.S_ISDIR(os.lstat(path).st_mode):
        for name in os.listdir(path):
            yield from _walk(os.path.join(path, name))


class InvalidOwnership(Exception):
    """A file of a build's output that its build user does not own: `ln /etc/shadow $out/foo`."""

    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.path = path


def canonicalise(
    path: Path,
    ignored_acls: frozenset[str] = IGNORED_ACLS,
    *,
    uid_range: tuple[int, int] | None = None,
    seen: set[tuple[int, int]] | None = None,
) -> None:
    """`canonicalisePathMetaData` (`posix-fs-canonicalise.cc:42`).

    With *uid_range*, a build's: a file owned outside it raises `InvalidOwnership`, unless it is a hard link to
    an inode in *seen*, which this call or an earlier one over the same build chowned already (`:91`).
    """
    euid, egid = os.geteuid(), os.getegid()
    if seen is None:
        seen = set()
    for at in _walk(os.fspath(path)):
        st = os.lstat(at)
        if not (stat.S_ISREG(st.st_mode) or stat.S_ISDIR(st.st_mode) or stat.S_ISLNK(st.st_mode)):
            raise AddError(f"file '{magenta(at)}' has an unsupported type")
        if uid_range is not None and not uid_range[0] <= st.st_uid <= uid_range[1]:
            if stat.S_ISDIR(st.st_mode) or (st.st_dev, st.st_ino) not in seen:
                raise InvalidOwnership(at)
            continue
        seen.add((st.st_dev, st.st_ino))
        try:
            names = os.listxattr(at, follow_symlinks=False)
        except OSError as error:
            if error.errno not in (errno.ENOTSUP, errno.ENODATA):
                raise
            names = []
        for name in names:
            if name not in ignored_acls:
                os.removexattr(at, name, follow_symlinks=False)
        if not stat.S_ISLNK(st.st_mode):
            mode = stat.S_IMODE(st.st_mode)
            is_dir = stat.S_ISDIR(st.st_mode)
            if (mode != 0o444 or is_dir) and mode != 0o555:
                os.chmod(at, 0o444 | (0o111 if st.st_mode & stat.S_IXUSR or is_dir else 0))
        # Nix compares whole seconds (`st_mtime`).
        if int(st.st_mtime) != MTIME_STORE:
            os.utime(at, ns=(st.st_atime_ns, MTIME_STORE * 1_000_000_000), follow_symlinks=False)
        if st.st_uid != euid:
            os.lchown(at, euid, egid)


def check_import_signatures(
    engine: StoreEngine,
    info: ValidPathInfo,
    settings: ImportSettings,
    overrides: Mapping[str, str],
    warnings: list[str],
) -> None:
    """`pathInfoIsUntrusted` and its refusal (`local-store.cc:1048`); *warnings* gets what Nix prints on the way.

    *overrides* are a client's `SetOptions`, which reach the keys (not `require-sigs`).
    """
    trusted, secret_files = key_settings(settings.trusted_public_keys, settings.secret_key_files, overrides)
    try:
        good = check_signatures(info, public_keys(trusted, secret_files), str(engine.layout.store_dir), warnings.append)
    except ValueError as error:
        raise AddError(str(error)) from error
    if not good:
        raise AddError(f"cannot add path '{magenta(info.path)}' because it lacks a signature by a trusted key")


def _sri(digest: bytes) -> str:
    return "sha256-" + base64.b64encode(digest).decode()


async def add_nar(
    engine: StoreEngine,
    info: ValidPathInfo,
    source: Source,
    *,
    repair: bool,
    ignored_acls: frozenset[str] = IGNORED_ACLS,
    auto_optimise: bool | None = None,
    fsync: bool | None = None,
) -> None:
    """`LocalStore::addToStore` after the signature check, for a path the client has temp-rooted.

    It reads the NAR from *source*, or nothing when the path is valid. What
    is left is the caller's: the rest of the frames (`FramedSource`,
    `daemon.cc:946`), or the rest of a `Bounded` path (`EnsureRead::finish`,
    `daemon.cc:564`).
    """
    path = str(info.path)
    real = engine.layout.real_store_dir / path.rsplit("/", 1)[-1]
    if not repair and await engine.is_valid_path_uncached(path):
        return
    lock = PathLock(real)
    # Not cancellable: an abandoned `flock` would hold the lock with no owner to release it.
    await anyio.to_thread.run_sync(lock.acquire)
    delete_lock = False
    try:
        if not repair and await engine.is_valid_path_uncached(path):
            delete_lock = True
            return
        await _restore_into_place(
            engine, info, real, source, await _placement(engine, ignored_acls, repair, auto_optimise, fsync)
        )
        await engine.register_valid_paths([info])
        delete_lock = True
    finally:
        lock.release(delete=delete_lock)


async def import_nar(
    engine: StoreEngine, info: ValidPathInfo, source: Source, *, dont_check_sigs: bool, repair: bool = False
) -> None:
    """AddToStoreNar for pynixd's own callers: the substitution queue and the HTTP cache's upload.

    No client session holds the path, so the import holds its own temporary
    root, as the daemon worker of a transfer connection did.
    """
    settings = await engine.settings()
    if not dont_check_sigs and settings.require_sigs:
        warnings: list[str] = []
        try:
            check_import_signatures(engine, info, settings, {}, warnings)
        finally:
            for warning in warnings:
                log.warning("import_signature_warning", path=str(info.path), warning=warning)
    roots = engine.temp_roots()
    try:
        if not await roots.add(info.path):
            raise AddError(f"pynixd cannot write a temporary root for '{magenta(info.path)}'")
        await add_nar(engine, info, source, repair=repair, ignored_acls=settings.ignored_acls)
    finally:
        with anyio.CancelScope(shield=True):
            await roots.close()


async def _restore_into_place(
    engine: StoreEngine,
    info: ValidPathInfo,
    real: Path,
    source: Source,
    placement: Placement,
) -> None:
    path = str(info.path)
    temp = await anyio.to_thread.run_sync(TempDir.create, engine.layout.real_store_dir)
    try:
        staged = temp.path / "x"
        reader = HashingReader(BlockingReader(source))
        try:
            await anyio.to_thread.run_sync(restore_path, staged, reader)
        except BadArchive as error:
            raise AddError(str(error)) from error
        got = reader.hasher.digest()
        expected = parse_any_sha256(str(info.info.nar_hash))
        if got != expected:
            specified = _sri(expected)
            raise AddError(
                f"hash mismatch importing path '{magenta(path)}';\n"
                f"  specified: {magenta(specified)}\n  got:       {magenta(_sri(got))}"
            )
        if reader.size != info.info.nar_size:
            raise AddError(
                f"size mismatch importing path '{magenta(path)}';\n"
                f"  specified: {magenta(info.info.nar_size)}\n  got:       {magenta(reader.size)}"
            )
        if info.info.ca:
            specified_ca = ContentAddress.parse(str(info.info.ca))
            hash_part = path.rsplit("/", 1)[-1][:32]
            positions = engine.hashes_self_reference_positions
            actual = await anyio.to_thread.run_sync(
                lambda: content_hash(specified_ca, staged, hash_part, dump_path, positions=positions)
            )
            if actual != specified_ca.digest:
                raise AddError(
                    f"ca hash mismatch importing path '{magenta(path)}';\n"
                    f"  specified: {magenta(f'{specified_ca.algo}:{nix32_encode(specified_ca.digest)}')}\n"
                    f"  got:       {magenta(f'{specified_ca.algo}:{nix32_encode(actual)}')}"
                )
        await anyio.to_thread.run_sync(_move_into_place, staged, real, placement)
    finally:
        with anyio.CancelScope(shield=True):
            await anyio.to_thread.run_sync(temp.remove)


def _restore_flat(dest: Path, read: Callable[[int], bytes]) -> None:
    """`restorePath` with `FileSerialisationMethod::Flat`: the bytes to one file, mode 0666, to the end."""
    fd = os.open(dest, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o666)
    with os.fdopen(fd, "wb") as file:
        while piece := read(64 * 1024):
            file.write(piece)


def _nar_hash(path: Path) -> tuple[bytes, int]:
    sha256 = hashlib.sha256()
    size = 0
    for piece in dump_path(path):
        sha256.update(piece)
        size += len(piece)
    return sha256.digest(), size


async def add_dump(
    engine: StoreEngine,
    name: str,
    method: Method,
    algo: str,
    references: frozenset[str],
    source: Source,
    *,
    root: Callable[[str], Awaitable[None]],
    repair: bool = False,
    ignored_acls: frozenset[str] = IGNORED_ACLS,
    scan: bool = False,
    auto_optimise: bool | None = None,
    fsync: bool | None = None,
) -> str:
    """`LocalStore::addToStoreFromDump` (`local-store.cc:1151`) for a flat, text, NAR or Git dump.

    With *repair* it restores over a valid path (`local-store.cc:1249`). With *scan* the references are
    those of *references* the dump names (`filterReferences`), for `AddToStoreScanning`.

    The path is known only once the dump is read, so the dump goes to a
    temporary directory first, as Nix does with a dump past `narBufferSize`.
    *root* temp-roots the path before the validity check.
    """
    temp = await anyio.to_thread.run_sync(TempDir.create, engine.layout.real_store_dir)
    try:
        staged = temp.path / "x"
        reader = HashingReader(BlockingReader(source), algo)
        # A Git dump comes as a NAR (`daemon.cc:430`).
        restore = _restore_flat if method in (Method.FLAT, Method.TEXT) else restore_path
        try:
            await anyio.to_thread.run_sync(restore, staged, reader)
        except BadArchive as error:
            raise AddError(str(error)) from error
        if method is Method.GIT:
            digest = (await anyio.to_thread.run_sync(git_hash, staged, algo))[1]
        else:
            digest = reader.hasher.digest()
        if scan:
            references = frozenset(await anyio.to_thread.run_sync(scan_for_references, staged, references))
        ca = ContentAddress(method, algo, digest)
        store_dir = str(engine.layout.store_dir)
        path = content_addressed_path(ca, name, references, store_dir)
        await root(path)
        if not repair and await engine.is_valid_path_uncached(path):
            return path
        real = engine.layout.real_store_dir / path.rsplit("/", 1)[-1]
        lock = PathLock(real)
        # Not cancellable: an abandoned `flock` would hold the lock with no owner to release it.
        await anyio.to_thread.run_sync(lock.acquire)
        delete_lock = False
        try:
            if repair or not await engine.is_valid_path_uncached(path):
                placement = await _placement(engine, ignored_acls, repair, auto_optimise, fsync)
                await anyio.to_thread.run_sync(_move_into_place, staged, real, placement)
                # A NAR dump hashed with SHA-256 is the NAR hash already (`local-store.cc:1304`).
                if method is Method.NAR and algo == "sha256":
                    nar_hash, nar_size = ca.digest, reader.size
                else:
                    nar_hash, nar_size = await anyio.to_thread.run_sync(_nar_hash, real)
                info = ValidPathInfo(
                    path=StorePath(path=path),
                    info=UnkeyedValidPathInfo(
                        nar_hash=NARHash(hash=nar_hash.hex()),
                        references={StorePath(path=ref) for ref in references},
                        registration_time=Time(ts=0),
                        nar_size=nar_size,
                        ultimate=False,
                        sigs=set(),
                        ca=WireContentAddress(value=ca.render()),
                    ),
                )
                await engine.register_valid_paths([info])
            delete_lock = True
        finally:
            lock.release(delete=delete_lock)
        return path
    finally:
        with anyio.CancelScope(shield=True):
            await anyio.to_thread.run_sync(temp.remove)


async def _placement(
    engine: StoreEngine, ignored_acls: frozenset[str], repair: bool, auto_optimise: bool | None, fsync: bool | None
) -> Placement:
    """`auto-optimise-store` and `fsync-store-paths`: each the session's override, else the store's setting."""
    settings = await engine.settings()
    optimising = settings.auto_optimise_store if auto_optimise is None else auto_optimise
    return Placement(
        ignored_acls,
        engine.layout.links_dir if optimising else None,
        repair,
        settings.fsync_store_paths if fsync is None else fsync,
    )


def recursive_sync(path: Path) -> None:
    """`recursiveSync` (`file-system.cc:336`): each regular file under *path*, then each directory, the last visited
    first. A symlink is not synced."""
    mode = os.lstat(path).st_mode
    if stat.S_ISREG(mode):
        _fsync(path, os.O_RDONLY)
        return
    if not stat.S_ISDIR(mode):
        return
    pending = [path]
    directories: list[Path] = []
    while pending:
        current = pending.pop()
        with os.scandir(current) as entries:
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    pending.append(current / entry.name)
                elif entry.is_file(follow_symlinks=False):
                    _fsync(current / entry.name, os.O_RDONLY)
        directories.append(current)
    for directory in reversed(directories):
        _fsync(directory, os.O_RDONLY | os.O_DIRECTORY)


def sync_store_path(path: Path) -> None:
    """`recursiveSync` and `syncParent` (`file-system.cc:326`), as `fsync-store-paths` asks before registration.

    Nix also starts the writeback of each file as it restores (`startFsync`, `fs-sink.cc:233`). That is a hint
    with no effect on what reaches the disk, and Python has no `sync_file_range`.
    """
    recursive_sync(path)
    _fsync(path.parent, os.O_RDONLY | os.O_DIRECTORY, follow=True)


def _fsync(path: Path, flags: int, *, follow: bool = False) -> None:
    fd = os.open(path, flags | os.O_CLOEXEC | (0 if follow else os.O_NOFOLLOW))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _move_into_place(staged: Path, real: Path, settings: Placement) -> None:
    """Into place, then `canonicalisePathMetaData`, `optimisePath`, and with `fsync-store-paths` the sync
    (`local-store.cc:1131`, `:1293`)."""
    # A path left by an import that crashed before it registered: Nix deletes it even without repair.
    delete_path(real)
    os.rename(staged, real)
    canonicalise(real, settings.ignored_acls)
    if settings.links_dir is not None:
        settings.links_dir.mkdir(parents=True, exist_ok=True)
        optimise.optimise_path(
            real,
            set(),
            real_store_dir=real.parent,
            links_dir=settings.links_dir,
            stats=optimise.OptimiseStats(),
            repair=settings.repair,
        )
    if settings.fsync:
        sync_store_path(real)


@dataclass(frozen=True)
class Placement:
    """What `_move_into_place` does after the rename. *links_dir* is set with `auto-optimise-store`, and *fsync*
    with `fsync-store-paths`."""

    ignored_acls: frozenset[str]
    links_dir: Path | None = None
    repair: bool = False
    fsync: bool = False
