"""A build through pynixd reaches VictoriaTraces as one trace (nixidae/nixidae#1570).

pynixd sends OTLP/HTTP to the server's `/insert/opentelemetry/v1/traces`; the test reads the trace back through
the Jaeger API, as an agent on dynhetz reads sg1's.
"""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any

import anyio
import httpx
import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.config import ExternalUnixStoreSpec, TracingSettings
from pynixd.op_routes import OpRoute
from pynixd.serde import BuildMode, BuildPathsWithResultsRequest, BuildResultStatus, DerivedPath
from pynixd.store import ExternalUnixStore, LocalDBStore
from tests._conftest.nix_config import for_test_store
from tests._conftest.victoriatraces import victoriatraces
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

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


async def _trace_of(url: str, service: str) -> list[dict[str, Any]]:  # Any: Jaeger's JSON, read by key below
    """The spans of the one trace of *service*, once its root span arrived."""
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
                spans = [span for trace in found for span in trace["spans"]]
                if any(span["operationName"] == f"build {NAME}" for span in spans):
                    return spans
                await anyio.sleep(0.2)


def _tags(span: dict[str, Any]) -> dict[str, Any]:  # Any: Jaeger tag values are strings, numbers or booleans
    return {tag["key"]: tag["value"] for tag in span["tags"]}


def _parent(span: dict[str, Any]) -> str | None:
    return next((ref["spanID"] for ref in span["references"] if ref["refType"] == "CHILD_OF"), None)


async def test_a_build_reaches_victoriatraces_as_one_trace(tmp_path: Path, traces: str) -> None:
    root = tmp_path / "store"
    socket = tmp_path / "pynixd.sock"
    service = f"pynixd-test-{uuid.uuid4().hex[:8]}"
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
        nix_config=for_test_store(substituters=()),
        op_routes={"BuildDerivation": OpRoute.daemon},
    )
    tracing = TracingSettings(
        endpoint=f"{traces}/insert/opentelemetry/v1/traces", service_name=service, schedule_delay=0.1
    )
    async with Server(
        stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None, tracing=tracing
    ):
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

    spans = await _trace_of(traces, service)

    by_name = {span["operationName"]: span for span in spans}
    build, attempt, phase = by_name[f"build {NAME}"], by_name["attempt"], by_name["build"]
    assert len({span["traceID"] for span in spans}) == 1
    assert _parent(attempt) == build["spanID"]
    assert _parent(phase) == attempt["spanID"]
    assert _tags(build)["pynixd.ending"] == "built"
    assert _tags(build)["pynixd.drv_path"] == drv.strip()
    assert _tags(attempt)["pynixd.store"] == "local"
