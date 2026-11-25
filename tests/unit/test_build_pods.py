"""A build pod store comes from its spec, and refuses what it cannot answer. nixidae/nixidae#1560, spike."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import pytest

from nix_daemon_protocol.build_derivation import BuildDerivationRequest, BuildDerivationResponse
from nix_daemon_protocol.build_result import BuildResult, BuildResultStatus
from nix_daemon_protocol.derivation_output import DerivationOutput
from nix_daemon_protocol.is_valid_path import IsValidPathRequest
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.unkeyed_realisation import UnkeyedRealisation
from pynixd.build_job import (
    CENTRAL_SECONDS,
    POD_SECONDS,
    RESULT_BYTES,
    CentralPart,
    PodPart,
    decode_request,
    decode_response,
    encode,
    read_result_line,
    read_seconds,
    result_line,
    result_message,
    seconds_line,
)
from pynixd.build_result import failure
from pynixd.config import (
    BuildPodFarmSpec,
    BuildPodStoreSpec,
    KubernetesSettings,
    KubernetesTemplateSettings,
    ProvisioningSettings,
    PynixdSettings,
    StoreRankingSettings,
    StoreSpec,
)
from pynixd.exceptions import InfrastructureError
from pynixd.placement import missing_input_bytes, store_setup_seconds
from pynixd.pod_build import _own_home, process_started
from pynixd.provisioning.kubernetes import (
    BUILD_STORE_LABEL,
    RUNNER_STORE_ATTRIBUTE,
    BuildPodEnd,
    BuildPodOwner,
    BuildPodWatch,
    FarmVolume,
    KubernetesProvider,
    OwnerPod,
    build_pod_manifest,
    build_store_label,
    first_placeable,
    pod_end,
    ready_labels,
    template_store_paths,
)
from pynixd.serde import BasicDerivation
from pynixd.store.build_pods import (
    BuildPodStore,
    PodTimes,
    checked_result,
    farm_references,
    farm_text,
    runner_references,
)
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


OWNER = BuildPodOwner(store="pods", run="r0")
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

    pod = build_pod_manifest(FARM_TEMPLATE, "pynixd-build-x", OWNER, farm)

    (volume,) = (v for v in pod["spec"]["volumes"] if v["name"] == "nix")
    assert volume["csi"]["volumeAttributes"] == {"x86_64-linux": root, "i686-linux": root, "presentation": "farm"}
    assert "volumeAttributes" not in FARM_TEMPLATE.pod["spec"]["volumes"][0]["csi"]
    with pytest.raises(ValueError, match="no CSI volume 'store'"):
        build_pod_manifest(
            FARM_TEMPLATE, "pynixd-build-x", OWNER, FarmVolume(volume="store", root=root, systems=farm.systems)
        )


def test_a_runner_s_store_holds_the_inputs_and_the_shell_alone() -> None:
    """As Nix's sandbox: the input closure, and the closure of the `sandbox-paths` shell; not the template's paths."""
    request = BuildDerivationRequest(
        drv_path=StorePath("/nix/store/00000000000000000000000000000003-x.drv"),
        derivation=BasicDerivation(input_srcs={StorePath(INPUT)}, platform="x86_64-linux", builder="/bin/sh"),
        build_mode=0,
    )
    shell = "/nix/store/00000000000000000000000000000009-busybox"

    assert runner_references(request, shell) == {INPUT, shell}
    assert runner_references(request, None) == {INPUT}


def test_a_runner_pod_names_its_own_store_beside_the_farm() -> None:
    root = "/nix/store/00000000000000000000000000000008-pynixd-build-inputs"
    runner = "/nix/store/0000000000000000000000000000000a-pynixd-runner-inputs"
    farm = FarmVolume(volume="nix", root=root, systems=frozenset({"x86_64-linux"}), runner_root=runner)

    pod = build_pod_manifest(FARM_TEMPLATE, "pynixd-build-x", OWNER, farm)

    (volume,) = (v for v in pod["spec"]["volumes"] if v["name"] == "nix")
    assert volume["csi"]["volumeAttributes"][RUNNER_STORE_ATTRIBUTE] == runner


def test_a_build_pod_says_what_each_part_of_its_fixed_cost_took() -> None:
    pod_line = seconds_line(POD_SECONDS, {PodPart.PYTHON: 2.4, PodPart.BUILD: 1.25, PodPart.SHUTDOWN: 0.2})
    times = PodTimes(asked=100.0, created=100.1, ran=101.5)

    times.saw("building...")
    times.first_line = 103.0
    times.saw(pod_line)
    times.last_line, times.ended = 107.0, 107.5

    assert read_seconds(POD_SECONDS, pod_line) == {"python": 2.4, "build": 1.25, "shutdown": 0.2}
    assert read_seconds(CENTRAL_SECONDS, pod_line) is None
    assert times.parts() == pytest.approx(
        {
            CentralPart.CREATE: 0.1,
            CentralPart.START: 1.4,
            CentralPart.FIRST_LINE: 1.5,
            CentralPart.END: 0.5,
            CentralPart.TOTAL: 7.5,
        }
    )
    # The cost beyond the build is what the placement adds for the store.
    assert times.fixed() == pytest.approx(6.25)
    assert PodTimes(asked=0.0, ended=1.0).fixed() is None
    # The process started before this test did, and not before the machine booted.
    assert 0 < time.monotonic() - process_started() < time.clock_gettime(time.CLOCK_BOOTTIME)


def test_build_one_makes_a_home_the_pod_names(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A pod whose store is `/` names its home, and Nix takes it only when this user owns it."""
    home = tmp_path / "build" / "home"
    monkeypatch.setenv("HOME", str(home))
    settings = PynixdSettings.model_validate(
        {"stores": {"local": {"type": "local-socket", "store_path": "/", "managed": True}}}
    )
    _own_home(settings)
    assert home.is_dir()
    assert home.stat().st_uid == os.geteuid()
    assert os.environ["HOME"] == str(home)


def test_a_build_pod_store_learns_its_fixed_cost() -> None:
    store = BuildPodStoreSpec(templates=["buildpod"], central_host="10.0.0.1").to_store("pods")
    times = PodTimes(asked=0.0, ended=6.0, pod={"build": 1.0})

    assert store.setup_seconds is None
    store._learn_fixed(times)  # noqa: SLF001 -- the update rule is the unit under test
    assert store.setup_seconds == 5.0
    store._learn_fixed(PodTimes(asked=0.0, ended=16.0, pod={"build": 1.0}))  # noqa: SLF001 -- as above
    assert store.setup_seconds == pytest.approx(8.0)


def test_a_build_pod_s_fixed_cost_leaves_out_its_copies() -> None:
    """The ranking prices the input and output copies by their bytes; counted here too, a large closure counts twice."""
    times = PodTimes(asked=0.0, ended=10.0, pod={"build": 1.0, "inputs": 3.0, "outputs": 0.5})
    assert times.fixed() == pytest.approx(5.5)


def test_the_ranking_takes_the_fixed_cost_a_store_learned() -> None:
    """Until a pod ends the store has no figure, and the ranking takes `remote_setup_seconds`."""
    store = BuildPodStoreSpec(templates=["buildpod"], central_host="10.0.0.1").to_store("pods")
    ranking = StoreRankingSettings(remote_setup_seconds=1.0)

    assert store_setup_seconds(store, ranking) == 1.0
    store._learn_fixed(PodTimes(asked=0.0, ended=6.0, pod={"build": 1.0}))  # noqa: SLF001 -- a pod's end sets it
    assert store_setup_seconds(store, ranking) == 5.0


def test_a_build_pod_takes_the_priority_class_of_its_depth() -> None:
    """A nested build's pod goes one class higher than the pod it is nested in (nixidae/nixidae#1576)."""
    spec = BuildPodStoreSpec(
        templates=["buildpod"], central_host="10.0.0.1", priority_classes=["pynixd-build", "pynixd-build-nested"]
    )
    template = KubernetesTemplateSettings(
        pod={"spec": {"priorityClassName": "pynixd-build", "containers": [{"name": "pynixd"}]}}
    )

    assert [spec.priority_class(depth) for depth in (0, 1, 5)] == [
        "pynixd-build",
        "pynixd-build-nested",
        "pynixd-build-nested",
    ]
    assert BuildPodStoreSpec(templates=["buildpod"], central_host="10.0.0.1").priority_class(3) is None
    pod = build_pod_manifest(template, "pynixd-build-x", OWNER, priority_class="pynixd-build-nested")
    assert pod["spec"]["priorityClassName"] == "pynixd-build-nested"
    assert build_pod_manifest(template, "pynixd-build-x", OWNER)["spec"]["priorityClassName"] == "pynixd-build"


def test_a_build_pod_names_its_store_in_a_label() -> None:
    owner = BuildPodOwner(store="buildpods-farm", run="r1")
    pod = build_pod_manifest(FARM_TEMPLATE, "pynixd-build-x", owner)

    assert pod["metadata"]["labels"][BUILD_STORE_LABEL] == "buildpods-farm"
    # The sweep takes the store's pods of every other run, and a pod with no run label.
    assert owner.others() == (
        "app.kubernetes.io/managed-by=pynixd,pynixd.nixidae.org/build=true,"
        "pynixd.nixidae.org/build-store=buildpods-farm,pynixd.nixidae.org/build-run!=r1"
    )
    assert pod["metadata"]["labels"]["pynixd.nixidae.org/build-run"] == "r1"
    # A label value holds 63 characters of [A-Za-z0-9._-], starting and ending alphanumeric.
    assert build_store_label("pods/x") == build_store_label("pods/x") != build_store_label("pods/y")
    assert build_store_label("-pods").isalnum()
    assert len(build_store_label("p" * 64)) <= 63


def test_central_s_pod_owns_each_build_pod() -> None:
    central = OwnerPod(name="pynixd-0", uid="5f0c")
    template = KubernetesTemplateSettings(
        pod={**FARM_TEMPLATE.pod, "metadata": {"ownerReferences": [{"kind": "Pod", "name": "kept", "uid": "1"}]}}
    )

    owned = build_pod_manifest(template, "pynixd-build-x", BuildPodOwner(store="pods", run="r0", pod=central))

    assert owned["metadata"]["ownerReferences"] == [
        {"kind": "Pod", "name": "kept", "uid": "1"},
        {"apiVersion": "v1", "kind": "Pod", "name": "pynixd-0", "uid": "5f0c"},
    ]
    assert "ownerReferences" not in build_pod_manifest(FARM_TEMPLATE, "pynixd-build-x", OWNER)["metadata"]


def test_central_s_pod_is_in_the_build_pods_namespace() -> None:
    kubernetes = ProvisioningSettings(kubernetes=KubernetesSettings(namespace="builds", templates={}))
    pods: dict[str, StoreSpec] = {"pods": BuildPodStoreSpec(templates=["buildpod"], central_host="10.0.0.1")}
    PynixdSettings(provisioning=kubernetes, stores=pods, pod_name="pynixd-0", pod_uid="5f0c", pod_namespace="builds")
    with pytest.raises(ValueError, match="counts as absent"):
        PynixdSettings(
            provisioning=kubernetes, stores=pods, pod_name="pynixd-0", pod_uid="5f0c", pod_namespace="nixkube"
        )
    # Without a build pod store nothing names the pod: reverse-builder pools may live in another namespace.
    PynixdSettings(provisioning=kubernetes, pod_name="pynixd-0", pod_uid="5f0c", pod_namespace="nixkube")
    with pytest.raises(ValueError, match="set all three"):
        PynixdSettings(provisioning=kubernetes, pod_name="pynixd-0")


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
    store = BuildPodStoreSpec(templates=["buildpod"], central_host="10.0.0.1", systems={"x86_64-linux"}).to_store(
        "pods"
    )

    assert isinstance(store, BuildPodStore)
    # The scheduler places only on a store with a daemon client.
    assert store.daemon_client is store
    await store.start()
    assert store.probe_state == ProbeState.PROBED
    with pytest.raises(InfrastructureError, match="holds no paths"):
        await store.is_valid_path(IsValidPathRequest(path=StorePath("/nix/store/00000000000000000000000000000001-x")))


@pytest.mark.anyio
async def test_the_placement_prices_a_copy_in_pod_at_its_whole_input_closure() -> None:
    sizes = {StorePath(INPUT): 1000, StorePath(RUNS): 24}
    copy_in = BuildPodStoreSpec(templates=["buildpod"], central_host="10.0.0.1", systems={"x86_64-linux"})
    farm = copy_in.model_copy(update={"farm": BuildPodFarmSpec(volume="nix")})

    assert await missing_input_bytes(copy_in.to_store("pods"), {}, sizes) == 1024
    # Central cannot see what a farm pod's node holds.
    assert await missing_input_bytes(farm.to_store("farm"), {}, sizes) is None


def _node(labels: dict[str, str], *, ready: str = "True", cordoned: bool = False) -> dict[str, Any]:
    return {
        "metadata": {"labels": labels},
        "spec": {"unschedulable": cordoned},
        "status": {"conditions": [{"type": "Ready", "status": ready}]},
    }


def test_only_a_ready_uncordoned_node_takes_a_build_pod() -> None:
    assert ready_labels(_node({"a": "1"})) == {"a": "1"}
    assert ready_labels(_node({"a": "1"}, ready="False")) is None
    assert ready_labels(_node({"a": "1"}, cordoned=True)) is None
    assert ready_labels({"metadata": {}}) is None


def test_a_pod_takes_the_first_template_a_node_can_take() -> None:
    sandbox = ("sandbox", {"nixkube/userns": "true", "kubernetes.io/arch": "amd64"})
    bare = ("bare", {"nixkube/userns": "false"})
    amd64_bare = {"nixkube/userns": "false", "kubernetes.io/arch": "amd64"}
    amd64_userns = {"nixkube/userns": "true", "kubernetes.io/arch": "amd64"}

    assert first_placeable([sandbox, bare], [amd64_bare, amd64_userns]) == "sandbox"
    assert first_placeable([sandbox, bare], [amd64_bare]) == "bare"
    # Every label of the selector counts, not one.
    assert first_placeable([sandbox], [{"nixkube/userns": "true", "kubernetes.io/arch": "arm64"}]) is None
    assert first_placeable([sandbox, bare], []) is None
    assert first_placeable([("any", {})], [{}]) == "any"


class _Nodes(KubernetesProvider):
    def __init__(self, settings: KubernetesSettings, nodes: list[dict[str, str]]) -> None:
        super().__init__(settings)
        self.nodes = nodes
        self.listed = 0

    async def ready_node_labels(self) -> list[dict[str, str]]:
        self.listed += 1
        return self.nodes


def _selecting(selector: dict[str, str]) -> KubernetesTemplateSettings:
    return KubernetesTemplateSettings(pod={"spec": {"nodeSelector": selector, "containers": []}})


@pytest.mark.anyio
async def test_a_store_picks_each_pod_s_template_from_the_ready_nodes() -> None:
    settings = KubernetesSettings(
        templates={"sandbox": _selecting({"nixkube/userns": "true"}), "bare": _selecting({"nixkube/userns": "false"})}
    )
    store = BuildPodStoreSpec(templates=["sandbox", "bare"], central_host="10.0.0.1").to_store("pods")
    kubernetes = _Nodes(settings, [{"nixkube/userns": "false"}])
    store.kubernetes = kubernetes

    assert await store.pick_template() == "bare"
    kubernetes.nodes = [{"nixkube/userns": "false"}, {"nixkube/userns": "true"}]
    assert await store.pick_template() == "sandbox"
    kubernetes.nodes = [{}]
    with pytest.raises(InfrastructureError, match="no Ready node"):
        await store.pick_template()

    one = BuildPodStoreSpec(templates=["sandbox"], central_host="10.0.0.1").to_store("one")
    one.kubernetes = kubernetes
    listed = kubernetes.listed
    # One template needs no node list, and no right to list nodes.
    assert await one.pick_template() == "sandbox"
    assert kubernetes.listed == listed


@pytest.mark.anyio
async def test_central_reads_the_result_only_from_a_line_with_the_jobs_token() -> None:
    message = await result_message(BuildDerivationResponse(result=failure("no")))
    line = result_line("token", message)

    assert read_result_line("token", line + "\n") == message
    assert (await decode_response(message)).result.error_msg == "no"
    # A builder's line that copies the shape but not the token is not a result.
    assert read_result_line("token", result_line("guess", message)) is None
    assert read_result_line("token", "building") is None
