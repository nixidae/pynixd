"""The writes of a Nix daemon, with the engine's names and plain types: Nix's `RemoteStore`.

Each method opens a transfer connection, sends one op, and its body from a
`Source`. The bytes to the daemon carry the same content as the client's, in
frames of their own: a frame boundary carries no meaning (`forward_framed`).

**The connection that adds a path carries the options of the client.**
`LocalStore::addToStore` checks each signature against `trusted-public-keys`,
and that setting reaches the daemon through `SetOptions` alone. With no
options, the daemon read its own keys, and `nix copy --from` with
`--trusted-public-keys` was refused for a path the cache had signed
correctly. `secret-key-files` travels the same way; `require-sigs` does not
(`daemon.cc:489`). nixidae/nixidae#236 and nixidae/nixidae#231.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
from anyio.lowlevel import checkpoint

from nix_daemon_protocol.add_build_log import AddBuildLogRequest, AddBuildLogResponse
from nix_daemon_protocol.add_indirect_root import AddIndirectRootRequest, AddIndirectRootResponse
from nix_daemon_protocol.add_multiple_to_store import AddMultipleToStoreRequest, AddMultipleToStoreResponse
from nix_daemon_protocol.add_perm_root import AddPermRootRequest, AddPermRootResponse
from nix_daemon_protocol.add_signatures import AddSignaturesRequest, AddSignaturesResponse
from nix_daemon_protocol.add_to_store import AddToStoreResponse
from nix_daemon_protocol.add_to_store_nar import AddToStoreNarRequest, AddToStoreNarResponse
from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.optimise_store import OptimiseStoreRequest, OptimiseStoreResponse
from nix_daemon_protocol.verify_store import VerifyStoreRequest, VerifyStoreResponse

from ..serde import AddToStoreRequest, StorePath
from ..serde.context import ReadContext
from ..wire import FramedWriter, send_framed, send_raw
from .copied import as_copied

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterable

    from nix_daemon_protocol.collect_garbage import CollectGarbageRequest, CollectGarbageResponse
    from nix_daemon_protocol.signature import Signature
    from nix_daemon_protocol.valid_path_info import ValidPathInfo
    from pynixd_store.source import Source

    from ..connection import ClientConn, Connection
    from ..serde import SetOptionsRequest
    from .daemon import DaemonStore


class DaemonWrites:
    """The write operations of *store*'s daemon."""

    def __init__(self, store: DaemonStore) -> None:
        self.store = store

    async def add_indirect_root(self, path: str) -> AddIndirectRootResponse:
        """AddIndirectRoot (op 12). The answer carries the daemon's log lines."""
        response: AddIndirectRootResponse = await self.store.call(AddIndirectRootRequest(path=path))
        return response

    async def add_perm_root(self, store_path: str, gc_root: str) -> AddPermRootResponse:
        """AddPermRoot (op 47). The answer carries the daemon's log lines."""
        response: AddPermRootResponse = await self.store.call(
            AddPermRootRequest(store_path=store_path, gc_root=gc_root)
        )
        return response

    async def optimise_store(self) -> OptimiseStoreResponse:
        """OptimiseStore (op 34). No body, no option; the answer carries the daemon's log lines."""
        response: OptimiseStoreResponse = await self.store.call(OptimiseStoreRequest())
        return response

    async def collect_garbage(
        self, request: CollectGarbageRequest, *, client: ClientConn | None
    ) -> CollectGarbageResponse:
        """CollectGarbage (op 20). The client's connection carries its options and takes the daemon's lines as they come.

        `execute`, not `call`: `DaemonStore.collect_garbage` forgets the
        cached info of each deleted path, as `invalidatePath` does
        (`local-store.cc:1016`). nixidae/nixidae#1007.
        """
        response: CollectGarbageResponse = await self.store.execute(request, client=client)
        return response

    async def verify_store(self, *, check_contents: bool, repair: bool) -> VerifyStoreResponse:
        """VerifyStore (op 35). The daemon takes a repair request and the paths it rebuilds."""
        request = VerifyStoreRequest(check_contents=int(check_contents), repair=int(repair))
        response: VerifyStoreResponse = await self.store.call(request)
        return response

    async def add_signatures(self, path: str, sigs: Iterable[Signature]) -> AddSignaturesResponse:
        """AddSignatures (op 37). Forget the path's cached info: the daemon changed its signatures."""
        response: AddSignaturesResponse = await self.store.call(
            AddSignaturesRequest(path=StorePath(path=path), sigs=set(sigs))
        )
        self.store.forget_path_info(path)
        return response

    async def add_build_log(self, path: str, log: bytes) -> AddBuildLogResponse:
        """AddBuildLog (op 45). The log follows the header framed; `addBuildLog` reads no option.

        No client options travel: `LocalStore::addBuildLog` reads no setting,
        as `addSignatures` does not (`OPTIONLESS_ROUTER_METHODS`).
        """
        view = memoryview(log)
        offset = 0

        async def read(n: int) -> bytes:
            nonlocal offset
            chunk = bytes(view[offset : offset + n])
            offset += len(chunk)
            return chunk

        async with self.store.transfer_conn(None) as conn:
            await conn.send_request(AddBuildLogRequest(path=path))
            await conn.w.drain()
            await send_framed(read, conn.w)
            response: AddBuildLogResponse = await AddBuildLogResponse.from_reader(ReadContext.from_conn(conn))
        return response

    async def add_to_store(
        self,
        info: ValidPathInfo,
        source: Source,
        *,
        repair: bool = False,
        dont_check_sigs: bool = False,
        options: SetOptionsRequest | None = None,
        logs: ClientConn | None = None,
        on_bytes: Callable[[int], None] | None = None,
    ) -> None:
        """AddToStoreNar (op 39). *logs* gets the daemon's log lines as they come."""
        request = AddToStoreNarRequest(info=info, repair=int(repair), dont_check_sigs=int(dont_check_sigs))
        async with self.store.transfer_conn(options) as conn:
            await conn.apply_options(options)
            await conn.send_request(request)
            await conn.w.drain()
            await send_framed(source.read, conn.w, on_bytes=on_bytes)
            await AddToStoreNarResponse.from_reader(ReadContext.from_conn(conn, client=logs))

    async def add_to_store_from_dump(
        self,
        name: str,
        method_algo: str,
        references: frozenset[str],
        source: Source,
        *,
        repair: bool = False,
        options: SetOptionsRequest | None = None,
        root: Callable[[str], Awaitable[None]],
        sign: Callable[[ValidPathInfo, Connection], Awaitable[ValidPathInfo]] | None = None,
        on_bytes: Callable[[int], None] | None = None,
    ) -> ValidPathInfo:
        """AddToStore (op 7). *root* temp-roots the path as soon as the daemon names it.

        *sign* runs over the same connection, idle once the response is read.
        A second pooled connection without the client's options makes the
        pool discard the idle one that carries them (`pool.py:196`), so every
        AddToStore would pay a fresh handshake.
        """
        request = AddToStoreRequest(
            path_name=name,
            cam=ContentAddress(value=method_algo),
            references={StorePath(path=ref) for ref in references},
            repair=int(repair),
        )
        async with self.store.transfer_conn(options) as conn:
            await conn.apply_options(options)
            await conn.send_request(request)
            await conn.w.drain()
            await send_framed(source.read, conn.w, on_bytes=on_bytes)
            response = await AddToStoreResponse.from_reader(ReadContext.from_conn(conn))
            info = response.info
            await root(str(info.path))
            if sign is not None:
                info = await sign(info, conn)
        self.store.add_path_info(info)
        return info

    async def add_multiple_to_store(
        self,
        count: int,
        paths: AsyncIterator[tuple[ValidPathInfo, Source]],
        *,
        repair: bool = False,
        dont_check_sigs: bool = False,
        options: SetOptionsRequest | None = None,
    ) -> list[ValidPathInfo]:
        """AddMultipleToStore (op 44): *count* paths in one stream, each NAR read to its end before the next path.

        The daemon may answer before the stream ends, so its response is read beside the send.
        """
        request = AddMultipleToStoreRequest(repair=int(repair), dont_check_sigs=int(dont_check_sigs))
        async with self.store.transfer_conn(options) as conn:
            await conn.apply_options(options)
            await conn.send_request(request)
            await conn.w.drain()
            # A task group hands back no result, so the child records it here.
            responses: list[AddMultipleToStoreResponse] = []
            infos: list[ValidPathInfo] = []

            async def read_response() -> None:
                responses.append(await AddMultipleToStoreResponse.from_reader(ReadContext.from_conn(conn)))

            async with anyio.create_task_group() as tg:
                tg.start_soon(read_response)
                framed = FramedWriter(conn.w)
                framed.write_uint64(count)
                async for info, nar in paths:
                    # Per path as well as per chunk: a transfer of many small
                    # paths spends its time here rather than in the byte loop
                    # (tests/benchmark/test_bench_nar_profile.py).
                    await checkpoint()
                    infos.append(info)
                    framed.write(await info.bytes_wire())
                    await send_raw(nar.read, framed, info.info.nar_size)
                await framed.finalize()
            if not responses:
                raise RuntimeError("the AddMultipleToStore reader task recorded no response")
        self.store.add_path_infos([as_copied(info) for info in infos])
        return infos
