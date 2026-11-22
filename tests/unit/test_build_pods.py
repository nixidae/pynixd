"""A build pod store comes from its spec, and refuses what it cannot answer. nixidae/nixidae#1560, spike."""

from __future__ import annotations

import pytest

from nix_daemon_protocol.build_derivation import BuildDerivationRequest, BuildDerivationResponse
from nix_daemon_protocol.build_result import BuildResult, BuildResultStatus
from nix_daemon_protocol.derivation_output import DerivationOutput
from nix_daemon_protocol.is_valid_path import IsValidPathRequest
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.unkeyed_realisation import UnkeyedRealisation
from pynixd.build_job import RESULT_BYTES, decode_request, decode_response, encode, result_message
from pynixd.build_result import failure
from pynixd.config import BuildPodStoreSpec
from pynixd.exceptions import InfrastructureError
from pynixd.provisioning.kubernetes import pod_end
from pynixd.serde import BasicDerivation
from pynixd.store.build_pods import BuildPodStore, checked_result
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


OUT = "/nix/store/00000000000000000000000000000004-x"
REALISED = "/nix/store/00000000000000000000000000000005-x"


def _realising(outputs: dict[str, DerivationOutput], realised: dict[str, str]) -> BuildDerivationResponse:
    request = BuildDerivationRequest(
        drv_path=StorePath("/nix/store/00000000000000000000000000000003-x.drv"),
        derivation=BasicDerivation(outputs=outputs, platform="x86_64-linux", builder="/bin/sh"),
        build_mode=0,
    )
    result = BuildResult(
        status=BuildResultStatus.BUILT,
        built_outputs_by_name={name: UnkeyedRealisation(out_path=StorePath(path)) for name, path in realised.items()},
    )
    return checked_result(request, BuildDerivationResponse(result=result))


def test_central_takes_a_pods_realisation_of_its_own_outputs_alone() -> None:
    floating = {"out": DerivationOutput(path="", method="r:sha256")}

    assert _realising(floating, {"out": REALISED}).result.status == BuildResultStatus.BUILT
    assert _realising({"out": DerivationOutput(path=OUT)}, {"out": OUT}).result.status == BuildResultStatus.BUILT
    assert "no output of it" in _realising(floating, {"dev": REALISED}).result.error_msg
    assert f"not {OUT}" in _realising({"out": DerivationOutput(path=OUT)}, {"out": REALISED}).result.error_msg


def test_a_pod_says_why_it_ended() -> None:
    preempted = {
        "status": {
            "phase": "Running",
            "conditions": [{"type": "DisruptionTarget", "status": "True", "reason": "PreemptionByScheduler"}],
        }
    }
    late = {"status": {"phase": "Failed", "reason": "DeadlineExceeded"}}
    running = {"status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}}

    assert pod_end(preempted) == "PreemptionByScheduler"
    assert pod_end(late) == "DeadlineExceeded"
    assert pod_end(running) is None
    assert pod_end({}) is None


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
