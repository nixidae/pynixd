"""How a store path's files reach another store on this machine, and what that costs the event loop.

Runs in a guest (`tests/derivations/copy-offload`) or by hand on any machine; `--drop-caches` needs root.
Each run copies one tree in a worker thread while a task ticks on the loop, then checks the
copy's NAR hash against the source's. Nix checks the NAR hash of every import, so each
approach reads the bytes at least once.

Approaches:

- `stream`: pynixd today. The source's NAR (`nar_fs.dump_path`) is restored into the target
  (`nar_fs.restore_path`) and hashed in passing: one read and one write, both through Python.
- `copy`: file by file with `shutil.copyfile`, which tries `copy_file_range`, then `sendfile`, then
  read and write; then the copy is hashed. On btrfs and XFS the kernel may share the extents.
- `clone`: file by file with the `FICLONE` ioctl, which shares the extents or refuses; then the
  copy is hashed. Refused on a filesystem without reflinks, and across filesystems.
- `auto`: the shipped `nar_fs.copy_path`, coreutils' `cp --reflink=auto` order (9.12, `copy.c`,
  `copy-file-data.c`): `FICLONE`, then `copy_file_range` until it refuses before the first byte, then
  read and write; then hashed.

`used_bytes` is how much free space the copy took after a `sync`: near zero for shared extents.
stdout: one JSON object per run, then a summary table on stderr. Run from the tree root as
`python -m tests.benchmark.copy_offload --src DIR --dst DIR --scenario NAME`.
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import enum
import fcntl
import hashlib
import json
import os
import shutil
import stat
import statistics
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Final

import anyio
import anyio.to_thread
import uvloop

from pynixd_store.nar_fs import copy_path, dump_path, nar_sha256, restore_path
from tests.benchmark.temp_roots import Stalls

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

FICLONE: Final = 0x40049409
"""`_IOW(0x94, 9, int)` from `linux/fs.h`: share all of the source file's extents with the target."""
SMALL_FILES: Final = 3000
SMALL_BYTES: Final = 8 * 1024
LARGE_FILES: Final = 12
LARGE_BYTES: Final = 24 * 1024 * 1024
"""About 310 MB in all: many small files, as a closure has, and a few large ones, as a toolchain has."""


class Approach(enum.StrEnum):
    STREAM = "stream"
    COPY = "copy"
    CLONE = "clone"
    AUTO = "auto"


@dataclasses.dataclass(frozen=True, slots=True)
class Result:
    scenario: str
    round_index: int
    approach: str
    files: int
    tree_bytes: int
    wall_ms: float
    stall_max_ms: float
    used_bytes: int
    hash_ok: bool
    error: str | None


def make_tree(at: Path) -> tuple[int, int]:
    """A tree shaped like a store path's, with random bytes so no layer below can compress or deduplicate it."""
    at.mkdir(parents=True)
    files = total = 0
    for index in range(SMALL_FILES):
        directory = at / f"d{index // 100:02d}"
        directory.mkdir(exist_ok=True)
        file = directory / f"f{index:05d}"
        file.write_bytes(os.urandom(SMALL_BYTES))
        file.chmod(0o555 if index % 10 == 0 else 0o444)
        files, total = files + 1, total + SMALL_BYTES
    (at / "lib").mkdir()
    for index in range(LARGE_FILES):
        file = at / "lib" / f"large{index:02d}.so"
        with file.open("wb") as out:
            for _ in range(LARGE_BYTES // (1024 * 1024)):
                out.write(os.urandom(1024 * 1024))
        file.chmod(0o444)
        files, total = files + 1, total + LARGE_BYTES
    (at / "lib" / "current.so").symlink_to("large00.so")
    return files, total


class _NarReader:
    """The sync `read` `restore_path` takes, over `dump_path`'s pieces, hashing what it hands out."""

    def __init__(self, pieces: Iterator[bytes]) -> None:
        self._pieces = pieces
        self._piece = memoryview(b"")
        self.digest = hashlib.sha256()

    def __call__(self, n: int) -> bytes:
        while not self._piece:
            piece = next(self._pieces, None)
            if piece is None:
                return b""
            self._piece = memoryview(piece)
        data = bytes(self._piece[:n])
        self._piece = self._piece[n:]
        self.digest.update(data)
        return data


def stream(src: Path, dst: Path) -> bytes:
    reader = _NarReader(iter(dump_path(src)))
    restore_path(dst, reader)
    return reader.digest.digest()


def copy_tree(src: Path, dst: Path, copy_file: Callable[[Path, Path], None]) -> None:
    """*src* to *dst* with *copy_file* for each regular file; modes as a NAR restores them."""
    mode = os.lstat(src).st_mode
    if stat.S_ISLNK(mode):
        os.symlink(os.readlink(src), dst)
    elif stat.S_ISDIR(mode):
        os.mkdir(dst)
        for name in sorted(os.listdir(src)):
            copy_tree(src / name, dst / name, copy_file)
    else:
        copy_file(src, dst)
        os.chmod(dst, 0o555 if mode & stat.S_IXUSR else 0o444)


def _copyfile(src: Path, dst: Path) -> None:
    shutil.copyfile(src, dst)


def _clone(src: Path, dst: Path) -> None:
    with src.open("rb") as source, dst.open("xb") as target:
        fcntl.ioctl(target.fileno(), FICLONE, source.fileno())


def auto(src: Path, dst: Path) -> bytes:
    """The shipped `nar_fs.copy_path`, then the copy hashed, as `add.tree_stage` does."""
    copy_path(src, dst)
    return nar_sha256(dst).digest()


def copied(src: Path, dst: Path) -> bytes:
    copy_tree(src, dst, _copyfile)
    return nar_sha256(dst).digest()


def cloned(src: Path, dst: Path) -> bytes:
    copy_tree(src, dst, _clone)
    return nar_sha256(dst).digest()


ARMS: Final[dict[Approach, Callable[[Path, Path], bytes]]] = {
    Approach.STREAM: stream,
    Approach.COPY: copied,
    Approach.CLONE: cloned,
    Approach.AUTO: auto,
}


def _free_after_sync(at: Path) -> int:
    os.sync()
    stats = os.statvfs(at)
    return stats.f_bfree * stats.f_frsize


def _drop_caches() -> None:
    os.sync()
    Path("/proc/sys/vm/drop_caches").write_text("3\n")


async def measure(  # noqa: PLR0913 -- one run's coordinates
    scenario: str, round_index: int, approach: Approach, src: Path, dst: Path, expected: bytes, *, drop: bool
) -> Result:
    target = dst / f"{scenario}-{round_index}-{approach}"
    files, tree_bytes = await anyio.to_thread.run_sync(_count, src)
    free_before = await anyio.to_thread.run_sync(_free_after_sync, dst)
    if drop:
        await anyio.to_thread.run_sync(_drop_caches)
    stalls = Stalls()
    error: str | None = None
    digest = b""
    async with anyio.create_task_group() as tg:
        tg.start_soon(stalls.run)
        await anyio.sleep(0.02)
        started = time.monotonic()
        try:
            digest = await anyio.to_thread.run_sync(ARMS[approach], src, target)
        except OSError as exc:
            error = f"{type(exc).__name__}: {exc}"
        wall = time.monotonic() - started
        await anyio.sleep(0.02)
        stalls.stop()
    used = free_before - await anyio.to_thread.run_sync(_free_after_sync, dst)
    await anyio.to_thread.run_sync(lambda: shutil.rmtree(target, ignore_errors=True))
    return Result(
        scenario=scenario,
        round_index=round_index,
        approach=approach,
        files=files,
        tree_bytes=tree_bytes,
        wall_ms=wall * 1000,
        stall_max_ms=max(stalls.stalls, default=0.0) * 1000,
        used_bytes=used,
        hash_ok=error is None and digest == expected,
        error=error,
    )


def _count(src: Path) -> tuple[int, int]:
    files = total = 0
    for root, _dirs, names in os.walk(src):
        for name in names:
            mode = os.lstat(Path(root) / name)
            if stat.S_ISREG(mode.st_mode):
                files, total = files + 1, total + mode.st_size
    return files, total


def _hash_column(runs: Sequence[Result]) -> str:
    """`-` when every run refused; a refusal copies nothing, so it has no hash to be wrong."""
    ran = [r for r in runs if r.error is None]
    if not ran:
        return "-"
    return "ok" if all(r.hash_ok for r in ran) else "BAD"


def summary(results: Sequence[Result]) -> str:
    rows = [f"{'scenario':10} {'approach':8} {'wall ms':>9} {'stall max ms':>12} {'used MB':>9} {'hash':>5} error"]
    for scenario in sorted({r.scenario for r in results}):
        for approach in Approach:
            runs = [r for r in results if r.scenario == scenario and r.approach == approach]
            if not runs:
                continue
            rows.append(
                f"{scenario:10} {approach:8} {statistics.median(r.wall_ms for r in runs):9.1f} "
                f"{max(r.stall_max_ms for r in runs):12.2f} "
                f"{statistics.median(r.used_bytes for r in runs) / 1e6:9.1f} "
                f"{_hash_column(runs):>5} {runs[0].error or ''}"
            )
    return "\n".join(rows)


async def main(args: argparse.Namespace) -> None:
    if not args.src.exists():
        await anyio.to_thread.run_sync(make_tree, args.src)
    expected = (await anyio.to_thread.run_sync(nar_sha256, args.src)).digest()
    await anyio.Path(args.dst).mkdir(parents=True, exist_ok=True)
    approaches = [Approach(name) for name in args.approaches]
    results: list[Result] = []
    for round_index in range(args.rounds):
        # Rotate the order each round, so a slow moment of the host lands on a different approach.
        shift = round_index % len(approaches)
        for approach in approaches[shift:] + approaches[:shift]:
            result = await measure(
                args.scenario, round_index, approach, args.src, args.dst, expected, drop=args.drop_caches
            )
            results.append(result)
            print(json.dumps(dataclasses.asdict(result)), flush=True)
    print(summary(results), file=sys.stderr)


def parse(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--src", type=Path, required=True, help="the tree to copy; made when it does not exist")
    parser.add_argument("--dst", type=Path, required=True, help="where each copy goes, then is deleted")
    parser.add_argument("--scenario", required=True, help="a label for the results")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--approaches", nargs="+", default=list(Approach), choices=list(Approach))
    parser.add_argument("--drop-caches", action="store_true", help="drop the page cache before each run (root)")
    return parser.parse_args(argv)


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        anyio.run(main, parse(sys.argv[1:]), backend="asyncio", backend_options={"loop_factory": uvloop.new_event_loop})
