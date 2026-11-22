"""A build through pynixd reaches VictoriaTraces as one trace (nixidae/nixidae#1570).

pynixd sends OTLP/HTTP to the server's `/insert/opentelemetry/v1/traces`; the test reads the trace back through
the Jaeger API, as an agent on dynhetz reads sg1's.
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

import anyio
import asyncssh
import httpx
import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.config import ExternalUnixStoreSpec, SSHSubprocessStoreSpec, TracingSettings
from pynixd.op_routes import OpRoute
from pynixd.serde import BuildMode, BuildPathsWithResultsRequest, BuildResultStatus, DerivedPath
from pynixd.store import ExternalUnixStore, LocalDBStore, SSHSubprocessStore
from tests._conftest.nix_config import for_test_store
from tests._conftest.victoriatraces import victoriatraces
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

NAME = "traced-build"
BUILD = (
    f'derivation {{ name = "{NAME}"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "echo > $out" ]; }'
)


@pytest.fixture
async def traces(tmp_path: Path) -> AsyncIterator[str]:
    data = tmp_path / "victoriatraces"
    await anyio.Path(data).mkdir()
    async with victoriatraces(data) as url:
        yield url


async def _trace_of(url: str, service: str, roots: int = 1) -> list[dict[str, Any]]:  # Any: Jaeger's JSON
    """The spans of the traces of *service* that hold a build, once *roots* spans named `build NAME` arrived.

    The evaluation that writes the `.drv` is a trace of its own: an `AddToStore` operation.
    """
    # A search without a time range returned nothing on sg1's VictoriaTraces, with the trace in the store.
    now_us = time.time_ns() // 1000
    window: dict[str, str | int] = {"service": service, "start": now_us - 3600 * 10**6, "end": now_us + 3600 * 10**6}
    async with httpx.AsyncClient(base_url=url, timeout=5) as client:
        # Nothing tells a reader that a span is searchable, so this asks until it is.
        with anyio.fail_after(30):
            while True:
                response = await client.get("/select/jaeger/api/traces", params=window)
                response.raise_for_status()
                found: list[dict[str, Any]] = response.json()["data"] or []
                builds = [t for t in found if any(s["operationName"] == f"build {NAME}" for s in t["spans"])]
                spans = [span for trace in builds for span in trace["spans"]]
                if sum(span["operationName"] == f"build {NAME}" for span in spans) >= roots:
                    return spans
                await anyio.sleep(0.2)


def _tags(span: dict[str, Any]) -> dict[str, Any]:  # Any: Jaeger tag values are strings, numbers or booleans
    return {tag["key"]: tag["value"] for tag in span["tags"]}


def _parent(span: dict[str, Any]) -> str | None:
    return next((ref["spanID"] for ref in span["references"] if ref["refType"] == "CHILD_OF"), None)


def _tracing(traces: str, service: str) -> TracingSettings:
    return TracingSettings(
        endpoint=f"{traces}/insert/opentelemetry/v1/traces", service_name=service, schedule_delay=0.1
    )


def _local(store_id: str, root: Path, **spec: Any) -> LocalDBStore:  # Any: `make_test_spec`'s keyword fields
    return LocalDBStore(
        make_test_spec(
            store_id=store_id,
            store_path=root,
            no_probe=True,
            nix_config=for_test_store(substituters=()),
            op_routes={"BuildDerivation": OpRoute.daemon},
            **spec,
        )
    )


async def _build_through(socket: Path, root: Path) -> str:
    """Evaluate the derivation into the store behind *socket* and build it there; the `.drv` path."""
    _rc, drv, _err, _both = await run_subproc(
        [
            NIX_BIN,
            "eval",
            "--store",
            f"unix://{socket}?root={root}",
            "--impure",
            "--raw",
            "--expr",
            f"({BUILD}).drvPath",
        ]
    )
    client_spec = ExternalUnixStoreSpec(store_id=StoreId("client"), socket_path=socket, store_path=root)
    async with ExternalUnixStore(client_spec) as client:
        request = BuildPathsWithResultsRequest(
            derived_paths=[DerivedPath(value=f"{drv.strip()}!*")], build_mode=BuildMode.NORMAL
        )
        response = await client.call(request, skip_probe=True)
    [keyed] = response.results
    assert keyed.result.status == BuildResultStatus.BUILT, keyed.result
    return drv.strip()


async def test_a_build_reaches_victoriatraces_as_one_trace(tmp_path: Path, traces: str) -> None:
    root = tmp_path / "store"
    socket = tmp_path / "pynixd.sock"
    service = f"pynixd-test-{uuid.uuid4().hex[:8]}"
    async with Server(
        stores={StoreId("local"): _local("local", root)},
        unix_path=socket,
        ssh_port=None,
        http_port=None,
        tracing=_tracing(traces, service),
    ):
        drv = await _build_through(socket, root)

    spans = await _trace_of(traces, service)

    by_name = {span["operationName"]: span for span in spans}
    op, build = by_name["BuildPathsWithResults"], by_name[f"build {NAME}"]
    attempt, phase = by_name["attempt"], by_name["build"]
    assert len({span["traceID"] for span in spans}) == 1
    assert _parent(build) == op["spanID"], "the build nests under the operation that asked for it"
    assert _tags(op)["pynixd.transport"] == "unix"
    assert _tags(op)["pynixd.session"]
    assert _tags(op)["pynixd.user"]
    assert _parent(attempt) == build["spanID"]
    assert _parent(phase) == attempt["spanID"]
    assert _tags(build)["pynixd.ending"] == "built"
    assert _tags(build)["pynixd.drv_path"] == drv
    assert _tags(attempt)["pynixd.store"] == "local"


async def test_a_build_on_a_pynixd_builder_is_part_of_the_controller_s_trace(tmp_path: Path, traces: str) -> None:
    """A sends the build to B over SSH, with its build phase's `traceparent`; B's operation nests under it."""
    key = asyncssh.generate_private_key("ssh-ed25519")
    admin_keys = anyio.Path(tmp_path / "admin_keys")
    await admin_keys.write_bytes(key.export_public_key())
    run = uuid.uuid4().hex[:8]
    controller, builder = f"pynixd-controller-{run}", f"pynixd-builder-{run}"
    async with Server(
        stores={StoreId("local"): _local("b-local", tmp_path / "store-b")},
        ssh_port=0,
        http_port=None,
        ssh_admin_keys=[Path(admin_keys)],
        tracing=_tracing(traces, builder),
    ) as server_b:
        remote = SSHSubprocessStore(
            SSHSubprocessStoreSpec(
                store_id=StoreId("builder-b"),
                host="127.0.0.1",
                port=server_b.port,
                username=server_b.username,
                client_keys=[key],
                # A server this test started on the loopback interface: there is no host key to pin.
                known_hosts=None,
                nix_bin=str(NIX_BIN),
                monitor=False,
            )
        )
        root_a, socket_a = tmp_path / "store-a", tmp_path / "a.sock"
        async with Server(
            # `no_schedule`: A has two stores that build, and the test is about the one behind SSH.
            stores={StoreId("local"): _local("a-local", root_a, no_schedule=True), StoreId("builder-b"): remote},
            unix_path=socket_a,
            ssh_port=None,
            http_port=None,
            tracing=_tracing(traces, controller),
        ) as server_a:
            with anyio.fail_after(10):
                while "builder-b" not in server_a.stores:  # A adds its stores in the background, and says nothing.
                    await anyio.sleep(0.05)
            await _build_through(socket_a, root_a)

    spans = await _trace_of(traces, controller, roots=2)

    roots = [s for s in spans if s["operationName"] == f"build {NAME}"]
    phases = {s["spanID"] for s in spans if s["operationName"] == "build"}
    [op_b] = [s for s in spans if s["operationName"] == "BuildDerivation"]
    [op_a] = [s for s in spans if s["operationName"] == "BuildPathsWithResults"]
    build_a = next(s for s in roots if _parent(s) == op_a["spanID"])
    build_b = next(s for s in roots if s is not build_a)
    assert len({span["traceID"] for span in spans}) == 1
    assert _parent(op_b) in phases, "B's operation nests under A's build phase"
    assert _parent(build_b) == op_b["spanID"]
    assert _tags(op_b)["pynixd.transport"] == "ssh"
    assert _parent(op_a) is None
