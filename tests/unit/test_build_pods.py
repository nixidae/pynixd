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
from pynixd.config import BuildPodStoreSpec, KubernetesTemplateSettings
from pynixd.exceptions import InfrastructureError
from pynixd.provisioning.kubernetes import (
    BuildPodEnd,
    BuildPodWatch,
    FarmVolume,
    build_pod_manifest,
    pod_end,
    template_store_paths,
)
from pynixd.serde import BasicDerivation
from pynixd.store.build_pods import BuildPodStore, checked_result, farm_references, farm_text
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
async def test_a_result_keyed_by_output_name_reads_back() -> None:
    result = BuildResult(
        status=BuildResultStatus.BUILT,
        times_built=1,
        is_non_deterministic=0,
        start_time=0,
        stop_time=0,
        built_outputs_by_name={"out": UnkeyedRealisation(out_path=StorePath(OUT))},
    )

    read = await decode_response(await result_message(BuildDerivationResponse(result=result)))

    realised = read.result.realised_outputs()["out"].out_path
    assert StorePath(str(realised)).with_store_prefix() == StorePath(OUT)


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
    assert "realised no path for out" in _realising(floating, {}).result.error_msg
    assert "realised no path for out" in _realising(floating, {"dev": REALISED}).result.error_msg
    floating_two = {**floating, "dev": DerivationOutput(path="", method="r:sha256")}
    assert (
        "no output of it"
        in _realising(floating_two, {"out": REALISED, "dev": REALISED, "doc": REALISED}).result.error_msg
    )
    assert f"not {OUT}" in _realising({"out": DerivationOutput(path=OUT)}, {"out": REALISED}).result.error_msg


RUNS = "/nix/store/00000000000000000000000000000006-pynixd"
INPUT = "/nix/store/00000000000000000000000000000007-busybox"
FARM_TEMPLATE = KubernetesTemplateSettings(
    pod={
        "spec": {
            "containers": [{"name": "pynixd", "command": [f"{RUNS}/bin/pynixd", "build-one"]}],
            "volumes": [{"name": "nix", "csi": {"driver": "nixkube", "readOnly": True}}],
        }
    }
)


def test_a_farm_holds_the_inputs_and_what_the_template_runs() -> None:
    request = BuildDerivationRequest(
        drv_path=StorePath("/nix/store/00000000000000000000000000000003-x.drv"),
        derivation=BasicDerivation(input_srcs={StorePath(INPUT)}, platform="x86_64-linux", builder="/bin/sh"),
        build_mode=0,
    )
    template_paths = template_store_paths(FARM_TEMPLATE)

    assert template_paths == {RUNS}
    references = farm_references(request, template_paths)
    assert references == {RUNS, INPUT}
    # The same inputs give the same text, so the same path.
    assert farm_text(references) == f"{RUNS}\n{INPUT}\n" == farm_text(frozenset(reversed(sorted(references))))


def test_a_farm_pod_asks_nixkube_for_the_builds_path_on_each_system() -> None:
    root = "/nix/store/00000000000000000000000000000008-pynixd-build-inputs"
    farm = FarmVolume(volume="nix", root=root, systems=frozenset({"x86_64-linux", "i686-linux"}))

    pod = build_pod_manifest(FARM_TEMPLATE, "pynixd-build-x", farm)

    (volume,) = (v for v in pod["spec"]["volumes"] if v["name"] == "nix")
    assert volume["csi"]["volumeAttributes"] == {"x86_64-linux": root, "i686-linux": root, "presentation": "farm"}
    assert "volumeAttributes" not in FARM_TEMPLATE.pod["spec"]["volumes"][0]["csi"]
    with pytest.raises(ValueError, match="no CSI volume 'store'"):
        build_pod_manifest(FARM_TEMPLATE, "pynixd-build-x", FarmVolume(volume="store", root=root, systems=farm.systems))


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


def _pod(state: dict[str, object], **status: object) -> dict[str, object]:
    return {"status": {"phase": "Running", "containerStatuses": [{"name": "pynixd", "state": state}], **status}}


@pytest.mark.anyio
async def test_one_watch_says_when_to_read_the_log_and_how_the_pod_ended() -> None:
    watch = BuildPodWatch("pynixd")
    watch.saw(_pod({"waiting": {"reason": "ContainerCreating"}}))
    assert not watch.settled.is_set()

    watch.saw(_pod({"running": {}}))
    assert watch.ran and watch.settled.is_set() and not watch.ended.is_set()
    watch.saw(_pod({"terminated": {"exitCode": 0, "message": "result"}}))

    assert watch.end == BuildPodEnd(exit_code=0, message="result", cause="exited")


@pytest.mark.anyio
async def test_a_watch_keeps_why_a_pod_went_after_it_is_gone() -> None:
    preempted = BuildPodWatch("pynixd")
    preempted.cause = "PreemptionByScheduler"
    preempted.gone()
    never = BuildPodWatch("pynixd")
    never.saw({"status": {"phase": "Failed", "reason": "DeadlineExceeded"}})
    deleted = BuildPodWatch("pynixd")
    deleted.gone()

    assert preempted.end is not None and preempted.end.disrupted
    assert never.end is not None and never.end.cause == "DeadlineExceeded" and not never.ran
    assert never.settled.is_set()
    assert deleted.end is not None and deleted.end.cause == "deleted" and not deleted.end.disrupted


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
