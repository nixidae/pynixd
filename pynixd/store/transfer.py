"""Multi-store path transfer utilities."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Final, Literal

import anyio
import structlog

from nix_daemon_protocol.add_multiple_to_store import AddMultipleToStoreRequest, AddMultipleToStoreResponse
from nix_daemon_protocol.nar_from_path import NarFromPathRequest

from .. import metrics, wire
from ..daemon_extensions.query_closure_with_info import QueryClosureWithInfoRequest
from ..exceptions import BackendError, leaves
from ..serde.context import ReadContext, WriteContext
from ..store_path import StorePath

if TYPE_CHECKING:
    from collections.abc import Iterable

    from nix_daemon_protocol.valid_path_info import ValidPathInfo

    from .daemon import DaemonStore


log = structlog.get_logger(__name__)

_UNSIZED_PATHS_NAMED: Final[int] = 3
"""How many paths of an unsized closure the error names.

Enough to see a pattern, and not the whole closure in one log line."""

_SELF_HEAL_ROUNDS: Final[int] = 4
"""Sends of one copy: the first, and one per path the destination refused
after it reported the path valid. Issue #887."""

_INVALID_PATH = re.compile(r"path '(?:\x1b\[[0-9;]*m)?([^'\x1b]+)(?:\x1b\[[0-9;]*m)?' is not valid")
"""`InvalidPath` of Nix (`src/libstore/store-api.cc`), coloured or not."""


def _refused_as_invalid(e: BaseException) -> str | None:
    """The path a destination called invalid, when that is all that failed."""
    failures = list(leaves(e))
    if not all(isinstance(leaf, BackendError) for leaf in failures):
        return None
    found = {m.group(1) for leaf in failures if (m := _INVALID_PATH.search(str(leaf)))}
    return found.pop() if len(found) == 1 else None


def _referenced_first(infos: list[ValidPathInfo]) -> list[ValidPathInfo]:
    """*infos*, each after every path of *infos* that it references.

    nix-daemon registers the paths of `AddMultipleToStore` in the order they
    arrive, and refuses one whose reference it does not hold: `path '...' is
    not valid`. `copyPaths` in `src/libstore/store-api.cc` sorts with
    `topoSortPaths` for this. The SQLite closure query orders by
    `ValidPaths.id`, and a path registered again after a delete gets an id
    above its referrers' (nixidae/nixidae#897).
    """
    by_path = {info.path: info for info in infos}
    out: list[ValidPathInfo] = []
    placed: set[StorePath] = set()
    for root in infos:
        stack = [(root, False)]
        while stack:
            info, expanded = stack.pop()
            if info.path in placed:
                continue
            if expanded:
                placed.add(info.path)
                out.append(info)
                continue
            stack.append((info, True))
            stack.extend(
                (by_path[ref], False)
                for ref in info.info.references
                if ref != info.path and ref in by_path and ref not in placed
            )
    return out


async def stream_paths_store_to_store(
    src: DaemonStore,
    dst: DaemonStore,
    paths: Iterable[StorePath],
    cancel_event: anyio.Event | None = None,
    *,
    peer_store_id: str | None = None,
    direction: Literal["in", "out"] | None = None,
) -> int:
    """Copy paths from src store to dst via streaming, querying closure first.

    Bypasses the normal handle() path, so we update dst knowledge manually.
    Only transfers paths that dst doesn't already have.

    `peer_store_id` names the non-local side of this transfer and
    `direction` says which way it flows relative to the local store
    ("in" is into it, "out" is out of it), so the movement is counted
    per store. Both come together or neither does: the function cannot
    tell which side is local on its own. The count sums the NAR sizes
    of the infos already walked, after the already-valid filter, so
    only moved bytes count and no byte loop is touched.

    Answers the NAR bytes it moved.
    """
    paths_set = set(paths)
    if not paths_set:
        return 0
    if (peer_store_id is None) != (direction is None):
        raise RuntimeError("peer_store_id and direction come together or not at all")
    log.debug("stream_paths_start", src=src.store_id, dst=dst.store_id, count=len(paths_set))

    # Fast-path for MockStore (used in tests)
    if type(src).__name__ == "MockStore" and type(dst).__name__ == "MockStore":
        log.debug(
            "mock_path_transfer_complete",
            src=src.store_id,
            dst=dst.store_id,
            count=len(paths_set),
        )
        return 0

    # 1. Get closure from source
    closure_resp = await src.execute(
        QueryClosureWithInfoRequest(paths=paths_set),
        client=None,
    )
    log.debug(
        "stream_paths_closure_loaded",
        src=src.store_id,
        dst=dst.store_id,
        infos=len(closure_resp.infos),
    )
    if not closure_resp.infos:
        return 0

    # 2. Filter out paths already in destination
    from nix_daemon_protocol.query_valid_paths import QueryValidPathsRequest

    closure = _referenced_first(closure_resp.infos)
    closure_paths = {info.path for info in closure}
    check = await dst.execute(QueryValidPathsRequest(paths=closure_paths, substitute=0))
    check_paths_set = set(check.paths)
    to_transfer: list[ValidPathInfo] = [info for info in closure if info.path not in check_paths_set]
    for _ in range(_SELF_HEAL_ROUNDS):
        log.debug(
            "stream_paths_filtered",
            src=src.store_id,
            dst=dst.store_id,
            already_valid=len(check_paths_set),
            to_transfer=len(to_transfer),
        )
        try:
            moved = await _stream_checked(src, dst, to_transfer, cancel_event, peer_store_id, direction)
        except Exception as e:
            refused = _refused_as_invalid(e)
            healed = [info for info in closure if str(info.path) == refused and info.path in check_paths_set]
            if not healed:
                raise
            # NIX-DEVIATION (#27): `copyPaths` in `src/libstore/store-api.cc`
            # trusts `queryValidPaths` of the destination and fails the copy
            # when it was wrong. pynixd sends the refused path and tries
            # again, because a builder that answered "valid" and then refused
            # the path costs a round trip here and not the build. Reverse it
            # when no `builder_refused_a_valid_path` is logged any more.
            log.warning("builder_refused_a_valid_path", dst=dst.store_id, path=refused)
            check_paths_set.discard(healed[0].path)
            to_transfer = [info for info in closure if info.path not in check_paths_set]
        else:
            return moved
    raise BackendError(
        f"{dst.store_id} kept refusing paths it reported valid, after {_SELF_HEAL_ROUNDS} rounds",
    )


async def _stream_checked(
    src: DaemonStore,
    dst: DaemonStore,
    to_transfer: list[ValidPathInfo],
    cancel_event: anyio.Event | None,
    peer_store_id: str | None,
    direction: Literal["in", "out"] | None,
) -> int:
    """Steps 3 and 4 of `stream_paths_store_to_store` for one selection, answering the NAR bytes moved."""
    if not to_transfer:
        return 0

    # **A NAR size of zero means unknown, and not empty.** Nix leaves
    # `ValidPathInfo::narSize` at 0 where it has no answer, and the shortest
    # NAR is 96 bytes: the magic string and the shape around it. Streaming
    # zero bytes for such a path writes a frame the destination cannot read,
    # and its daemon then answers `reached end of FramedSource`, which names
    # the frame and neither the path nor the store that described it.
    # Issue #48.
    unsized = [info for info in to_transfer if info.info.nar_size == 0]
    if unsized:
        raise BackendError(
            f"{src.store_id} gave no NAR size for "
            f"{', '.join(str(info.path) for info in unsized[:_UNSIZED_PATHS_NAMED])}"
            f"{'...' if len(unsized) > _UNSIZED_PATHS_NAMED else ''}, "
            f"so {dst.store_id} cannot be told how much to read. A size of zero means unknown.",
        )

    # 3. Stream the missing paths
    log.debug("stream_paths_acquire_src", src=src.store_id, dst=dst.store_id)
    async with src.transfer_conn() as src_conn:
        log.debug("stream_paths_acquired_src", src=src.store_id, dst=dst.store_id, conn_id=src_conn.id)
        log.debug("stream_paths_acquire_dst", src=src.store_id, dst=dst.store_id)
        async with dst.transfer_conn() as dst_conn:
            log.debug("stream_paths_acquired_dst", src=src.store_id, dst=dst.store_id, conn_id=dst_conn.id)
            await _stream_paths_over_conns(src_conn, dst_conn, to_transfer, cancel_event)

    moved = sum(info.info.nar_size for info in to_transfer)
    if peer_store_id is not None and direction is not None:
        metrics.record_store_transfer(peer_store_id, direction, moved, len(to_transfer))

    # 4. Update destination store's knowledge, as the destination registered
    # it: a copy is never `ultimate` (`daemon.cc:516`). nixidae/nixidae#1008.
    dst.add_path_infos([as_copied(info) for info in to_transfer])
    return moved


def as_copied(info: ValidPathInfo) -> ValidPathInfo:
    """*info* as a destination registers a copy: `ultimate` cleared (`daemon.cc:516`)."""
    if not info.info.ultimate:
        return info
    return info.model_copy(update={"info": info.info.model_copy(update={"ultimate": False})})


async def _stream_paths_over_conns(
    src_conn,
    dst_conn,
    to_transfer: list[ValidPathInfo],
    cancel_event: anyio.Event | None,
) -> None:
    """Stream already-selected path infos over established source/destination connections."""
    dst_conn.op_log.append("AddMultipleToStore (stream_paths_to)")
    req = AddMultipleToStoreRequest(
        repair=0,
        dont_check_sigs=1,
    )
    await req.to_writer(WriteContext.from_conn(dst_conn))
    await dst_conn.w.drain()

    async def read_response() -> AddMultipleToStoreResponse:
        log.debug("stream_paths_wait_response", dst_conn=dst_conn.id, count=len(to_transfer))
        response = await AddMultipleToStoreResponse.from_reader(ReadContext.from_conn(dst_conn))
        log.debug("stream_paths_response_read", dst_conn=dst_conn.id, count=len(to_transfer))
        return response

    async with anyio.create_task_group() as tg:
        # The group waits for the reader on exit, which is what the
        # `await response_task` at the end of this block used to do.
        tg.start_soon(read_response)
        fw = dst_conn.w.framed()
        fw.write_uint64(len(to_transfer))

        for info in to_transfer:
            if cancel_event and cancel_event.is_set():
                log.info("stream_paths_transfer_cancelled")
                break

            path = info.path
            log.debug(
                "stream_paths_path_start",
                src_conn=src_conn.id,
                dst_conn=dst_conn.id,
                path=str(path),
                nar_size=info.info.nar_size,
            )
            dst_conn.op_log.append("AddToStoreNar (stream_paths_to)")

            fw.write(await info.bytes_wire())

            # Request NAR from source
            await NarFromPathRequest(path=path).to_writer(WriteContext.from_conn(src_conn))
            await src_conn.w.drain()

            # Source will send stderr logs followed by STDERR_LAST before NAR data
            await src_conn.r.drain_stderr()

            # Pipe raw NAR data from source into the destination's framed stream
            await wire.forward_raw(
                src_conn.r,
                fw,
                info.info.nar_size,
            )
            await dst_conn.w.drain()
            log.debug("stream_paths_path_sent", src_conn=src_conn.id, dst_conn=dst_conn.id, path=str(path))

        await fw.finalize()
        await dst_conn.w.drain()
