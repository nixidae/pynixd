"""A path into the store as `LocalStore::addToStore` (`local-store.cc:1046` of Nix 2.35) and
`addToStoreFromDump` (`local-store.cc:1168`) put one there.

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
import anyio.from_thread
import anyio.to_thread
import structlog

from nix_daemon_protocol.content_address import ContentAddress as WireContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time

from ..content_address import ContentAddress, Method, content_addressed_path, parse_any_sha256
from ..exceptions import PynixdError
from ..hintfmt import magenta
from ..nar_fs import BadArchive, dump_path, restore_path
from ..signing import check_signatures, key_settings, public_keys
from ..temp_roots import TempRoots
from ..utils import nix32_encode
from .queries import IS_VALID_PATH

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable, Iterator, Mapping
    from pathlib import Path

    from .local_db import LocalDBStore

log = structlog.get_logger(__name__)

MTIME_STORE = 1
"""`mtimeStore`, `posix-fs-canonicalise.cc:13`."""

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
    ignored_acls: frozenset[str] = IGNORED_ACLS

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
            ignored_acls=frozenset(value("ignored-acls")),
        )


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
        while True:
            fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX)
            # A holder that finished wrote "d" and unlinked the file: this lock guards nothing.
            if os.fstat(fd).st_size == 0:
                self._fd = fd
                return
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
    """

    def __init__(self, algo: str, modulus: bytes) -> None:
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
        for pos in sorted(set(self._matches)):
            self._hash.update(f"|{pos}".encode())
        return self._hash.digest()


def content_hash(ca: ContentAddress, path: Path, hash_part: str, dump: Callable[[Path], Iterable[bytes]]) -> bytes:
    """The hash `addToStore` takes of the restored tree to check its address (`local-store.cc:1099`).

    *dump* is `nar_fs.dump_path`; a flat or text address hashes the file's bytes.
    """
    sink = HashModulo(ca.algo, hash_part.encode())
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


def _walk(path: str) -> Iterator[str]:
    yield path
    if stat.S_ISDIR(os.lstat(path).st_mode):
        for name in os.listdir(path):
            yield from _walk(os.path.join(path, name))


def canonicalise(path: Path, ignored_acls: frozenset[str] = IGNORED_ACLS) -> None:
    """`canonicalisePathMetaData` with no uid range (`posix-fs-canonicalise.cc:42`), as `addToStore` calls it."""
    euid, egid = os.geteuid(), os.getegid()
    for at in _walk(os.fspath(path)):
        st = os.lstat(at)
        if not (stat.S_ISREG(st.st_mode) or stat.S_ISDIR(st.st_mode) or stat.S_ISLNK(st.st_mode)):
            raise AddError(f"file '{magenta(at)}' has an unsupported type")
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


class FrameReader:
    """The sync `read` that `restore_path` takes, over async framed chunks, one portal call per chunk.

    It runs on the worker thread; *next_chunk* runs on the event loop and gives `b""` at the end.
    """

    def __init__(self, next_chunk: Callable[[], Awaitable[bytes]]) -> None:
        self._next_chunk = next_chunk
        self._buffer = memoryview(b"")
        self.consumed = 0

    def __call__(self, n: int) -> bytes:
        if not self._buffer:
            self._buffer = memoryview(anyio.from_thread.run(self._chunk))
        out = bytes(self._buffer[:n])
        self._buffer = self._buffer[n:]
        self.consumed += len(out)
        return out

    def leftover(self) -> bytes:
        """What the last chunk held past the bytes read: the next record of a shared stream."""
        rest = bytes(self._buffer)
        self._buffer = memoryview(b"")
        return rest

    async def _chunk(self) -> bytes:
        return await self._next_chunk()


def check_import_signatures(
    store: LocalDBStore,
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
        good = check_signatures(info, public_keys(trusted, secret_files), str(store.layout.store_dir), warnings.append)
    except ValueError as error:
        raise AddError(str(error)) from error
    if not good:
        raise AddError(f"cannot add path '{magenta(info.path)}' because it lacks a signature by a trusted key")


def _sri(digest: bytes) -> str:
    return "sha256-" + base64.b64encode(digest).decode()


async def _is_valid(store: LocalDBStore, path: str) -> bool:
    """`isValidPathUncached`: the table itself, never the path-info cache."""
    async with store.db.acquire_conn() as conn, conn.execute(IS_VALID_PATH, (path,)) as cursor:
        return await cursor.fetchone() is not None


async def add_nar(
    store: LocalDBStore,
    info: ValidPathInfo,
    read: FrameReader,
    *,
    repair: bool,
    ignored_acls: frozenset[str] = IGNORED_ACLS,
) -> None:
    """`LocalStore::addToStore` after the signature check, for a path the client has temp-rooted.

    It reads the NAR through *read* and stops at the end of the archive, or
    reads nothing when the path is valid. What is left is the caller's: the
    rest of the frames (`FramedSource`, `daemon.cc:946`), or the skip to the
    next path of a shared stream (`EnsureRead::finish`, `daemon.cc:518`).
    """
    path = str(info.path)
    real = store.layout.real_store_dir / path.rsplit("/", 1)[-1]
    if not repair and await _is_valid(store, path):
        return
    lock = PathLock(real)
    # Not cancellable: an abandoned `flock` would hold the lock with no owner to release it.
    await anyio.to_thread.run_sync(lock.acquire)
    delete_lock = False
    try:
        if not repair and await _is_valid(store, path):
            delete_lock = True
            return
        await _restore_into_place(store, info, real, read, ignored_acls)
        await store.register_valid_paths([info])
        delete_lock = True
    finally:
        lock.release(delete=delete_lock)


async def import_nar_pieces(
    store: LocalDBStore, info: ValidPathInfo, pieces: AsyncGenerator[bytes], *, dont_check_sigs: bool
) -> None:
    """AddToStoreNar for pynixd's own callers: the substitution queue and the HTTP cache's upload.

    No client session holds the path, so the import holds its own temporary
    root, as the daemon worker of a transfer connection did. *pieces* is
    closed at the end, read or not: a daemon connection it streams from
    leaves dirty mid-NAR, and the pool discards it (`Connection.__aexit__`).
    """
    settings = await store.import_settings()
    if not dont_check_sigs and settings.require_sigs:
        warnings: list[str] = []
        try:
            check_import_signatures(store, info, settings, {}, warnings)
        finally:
            for warning in warnings:
                log.warning("import_signature_warning", path=str(info.path), warning=warning)
    roots = TempRoots(store.layout.state_dir)
    try:
        if not await roots.add(info.path):
            raise AddError(f"pynixd cannot write a temporary root for '{magenta(info.path)}'")
        async with contextlib.aclosing(pieces) as stream:

            async def next_chunk() -> bytes:
                while (piece := await anext(stream, None)) is not None:
                    if piece:
                        return piece
                return b""

            await add_nar(store, info, FrameReader(next_chunk), repair=False, ignored_acls=settings.ignored_acls)
    finally:
        with anyio.CancelScope(shield=True):
            await roots.close()


async def _restore_into_place(
    store: LocalDBStore,
    info: ValidPathInfo,
    real: Path,
    read: FrameReader,
    ignored_acls: frozenset[str],
) -> None:
    path = str(info.path)
    temp = await anyio.to_thread.run_sync(TempDir.create, store.layout.real_store_dir)
    try:
        staged = temp.path / "x"
        reader = HashingReader(read)
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
            actual = await anyio.to_thread.run_sync(content_hash, specified_ca, staged, hash_part, dump_path)
            if actual != specified_ca.digest:
                raise AddError(
                    f"ca hash mismatch importing path '{magenta(path)}';\n"
                    f"  specified: {magenta(f'{specified_ca.algo}:{nix32_encode(specified_ca.digest)}')}\n"
                    f"  got:       {magenta(f'{specified_ca.algo}:{nix32_encode(actual)}')}"
                )
        await anyio.to_thread.run_sync(_move_into_place, staged, real, ignored_acls)
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
    store: LocalDBStore,
    name: str,
    method: Method,
    algo: str,
    references: frozenset[str],
    read: FrameReader,
    *,
    root: Callable[[str], Awaitable[None]],
    ignored_acls: frozenset[str] = IGNORED_ACLS,
) -> str:
    """`LocalStore::addToStoreFromDump` (`local-store.cc:1168`) for a flat, text or NAR dump, without repair.

    The path is known only once the dump is read, so the dump goes to a
    temporary directory first, as Nix does with a dump past `narBufferSize`.
    *root* temp-roots the path before the validity check.
    """
    if method is Method.GIT:
        raise ValueError("pynixd does not hash a Git dump")
    temp = await anyio.to_thread.run_sync(TempDir.create, store.layout.real_store_dir)
    try:
        staged = temp.path / "x"
        reader = HashingReader(read, algo)
        restore = restore_path if method is Method.NAR else _restore_flat
        try:
            await anyio.to_thread.run_sync(restore, staged, reader)
        except BadArchive as error:
            raise AddError(str(error)) from error
        ca = ContentAddress(method, algo, reader.hasher.digest())
        store_dir = str(store.layout.store_dir)
        path = content_addressed_path(ca, name, references, store_dir)
        await root(path)
        if await _is_valid(store, path):
            return path
        real = store.layout.real_store_dir / path.rsplit("/", 1)[-1]
        lock = PathLock(real)
        # Not cancellable: an abandoned `flock` would hold the lock with no owner to release it.
        await anyio.to_thread.run_sync(lock.acquire)
        delete_lock = False
        try:
            if not await _is_valid(store, path):
                await anyio.to_thread.run_sync(_move_into_place, staged, real, ignored_acls)
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
                await store.register_valid_paths([info])
            delete_lock = True
        finally:
            lock.release(delete=delete_lock)
        return path
    finally:
        with anyio.CancelScope(shield=True):
            await anyio.to_thread.run_sync(temp.remove)


def _move_into_place(staged: Path, real: Path, ignored_acls: frozenset[str]) -> None:
    # A path left by an import that crashed before it registered: Nix deletes it even without repair.
    delete_path(real)
    os.rename(staged, real)
    canonicalise(real, ignored_acls)
