"""A build pod store comes from its spec, and refuses what it cannot answer. nixidae/nixidae#1560, spike."""

from __future__ import annotations

import pytest

from nix_daemon_protocol.build_derivation import BuildDerivationRequest, BuildDerivationResponse
from nix_daemon_protocol.is_valid_path import IsValidPathRequest
from nix_daemon_protocol.store_path import StorePath
from pynixd.build_job import RESULT_BYTES, decode_request, decode_response, encode, result_message
from pynixd.build_result import failure
from pynixd.config import BuildPodStoreSpec
from pynixd.exceptions import InfrastructureError
from pynixd.serde import BasicDerivation
from pynixd.store.build_pods import BuildPodStore
from pynixd.store.scheduled import ProbeState


@pytest.mark.anyio
async def test_the_job_and_the_result_read_back_as_written() -> None:
    request = BuildDerivationRequest(
        drv_path=StorePath("/nix/store/00000000000000000000000000000003-x.drv"),
        derivation=BasicDerivation(platform="x86_64-linux", builder="/bin/sh", args=["-c", "true"]),
        build_mode=0,
    )
    response = BuildDerivationResponse(result=failure("x" * 10_000))

    assert await decode_request(await encode(request)) == request
    message = await result_message(response)
    assert len(message) <= RESULT_BYTES
    assert (await decode_response(message)).result.error_msg.endswith("x" * 100)


@pytest.mark.anyio
async def test_the_spec_makes_a_store_that_holds_nothing() -> None:
    store = BuildPodStoreSpec(template="buildpod", central_host="10.0.0.1", systems={"x86_64-linux"}).to_store("pods")

    assert isinstance(store, BuildPodStore)
    # The scheduler places only on a store with a daemon client.
    assert store.daemon_client is store
    await store.start()
    assert store.probe_state == ProbeState.PROBED
    with pytest.raises(InfrastructureError, match="holds no paths"):
        await store.is_valid_path(IsValidPathRequest(path=StorePath("/nix/store/00000000000000000000000000000001-x")))
