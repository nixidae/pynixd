"""The Kubernetes provider's pod shapes and states. nixidae/nixidae#1560.

The calls against an API server run in `internal.tests.provisioning-kubernetes`, on a kubeadm node.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import anyio
import httpx
import kr8s
import pytest

from pynixd.config import KubernetesSettings, KubernetesTemplateSettings, ProvisioningSettings
from pynixd.provisioning.kubernetes import (
    MANAGED_LABEL,
    TAGS_ANNOTATION,
    TICKET_VOLUME,
    failing,
    machine,
    pod_manifest,
    pod_name,
    secret_manifest,
)
from pynixd.provisioning.provider import MachineState, ProviderError

TAGS = {"pynixd-owner": "controller", "pynixd-instance": "big-0a1b2c3d", "pynixd-pool": "big"}
DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")


def _template(**changes: Any) -> KubernetesTemplateSettings:  # noqa: ANN401 -- the model's own fields
    pod = {"spec": {"containers": [{"name": "side"}, {"name": "pynixd", "command": ["pynixd"]}]}}
    return KubernetesTemplateSettings.model_validate({"pod": pod, **changes})


def _pod(phase: str | None, **metadata: Any) -> dict[str, Any]:  # noqa: ANN401 -- a pod's JSON
    return {
        "metadata": {"name": "p", "annotations": {TAGS_ANNOTATION: json.dumps(TAGS)}, **metadata},
        "status": {} if phase is None else {"phase": phase},
    }


def test_pod_name_is_a_dns_label_fixed_by_the_tags() -> None:
    name = pod_name(TAGS)
    assert DNS_LABEL.match(name)
    assert len(name) <= 63
    assert name == pod_name(dict(reversed(TAGS.items())))
    assert name != pod_name({**TAGS, "pynixd-instance": "big-ffffffff"})


def test_pod_name_of_long_and_odd_tags() -> None:
    tags = {"pynixd-owner": "Some_Host.example.org" * 4, "pynixd-instance": "x_y", "pynixd-pool": "__"}
    name = pod_name(tags)
    assert DNS_LABEL.match(name), name
    assert len(name) <= 63


def test_manifest_mounts_the_ticket_in_the_named_container() -> None:
    template = _template(container="pynixd")
    pod = pod_manifest(template, "n", TAGS)
    assert pod["kind"] == "Pod"
    assert pod["metadata"]["name"] == "n"
    assert pod["metadata"]["labels"][MANAGED_LABEL] == "pynixd"
    assert json.loads(pod["metadata"]["annotations"][TAGS_ANNOTATION]) == TAGS
    assert pod["spec"]["restartPolicy"] == "Never"
    assert {"name": TICKET_VOLUME, "secret": {"secretName": "n", "defaultMode": 0o400}} in pod["spec"]["volumes"]
    side, builder = pod["spec"]["containers"]
    assert "volumeMounts" not in side
    assert builder["volumeMounts"] == [{"name": TICKET_VOLUME, "mountPath": "/run/pynixd-join", "readOnly": True}]
    assert "metadata" not in template.pod, "the template is not changed"


def test_manifest_defaults_to_the_first_container_and_keeps_a_restart_policy() -> None:
    template = _template()
    template.pod["spec"]["restartPolicy"] = "OnFailure"
    pod = pod_manifest(template, "n", TAGS)
    assert "volumeMounts" in pod["spec"]["containers"][0]
    assert pod["spec"]["restartPolicy"] == "OnFailure"


def test_manifest_refuses_a_missing_container() -> None:
    with pytest.raises(ValueError, match="no container 'absent'"):
        pod_manifest(_template(container="absent"), "n", TAGS)


def test_secret_is_owned_by_the_pod() -> None:
    secret = secret_manifest("n", "uid-1", '{"token": "t"}')
    assert secret["metadata"]["ownerReferences"] == [{"apiVersion": "v1", "kind": "Pod", "name": "n", "uid": "uid-1"}]
    assert secret["stringData"] == {"ticket": '{"token": "t"}'}


@pytest.mark.parametrize(
    ("phase", "state"),
    [
        (None, MachineState.PENDING),
        ("Pending", MachineState.PENDING),
        ("Running", MachineState.RUNNING),
        ("Succeeded", MachineState.ENDED),
        ("Failed", MachineState.ENDED),
        ("Unknown", MachineState.PENDING),
    ],
)
def test_machine_state_of_each_phase(phase: str | None, state: MachineState) -> None:
    found = machine(_pod(phase))
    assert found is not None
    assert found.state is state
    assert found.tags == TAGS
    assert (found.error is not None) == (phase == "Failed")


def test_failed_pod_names_its_container() -> None:
    pod = _pod("Failed")
    pod["status"]["containerStatuses"] = [
        {"name": "pynixd", "state": {"terminated": {"reason": "Error", "exitCode": 3}}}
    ]
    found = machine(pod)
    assert found is not None
    assert found.error == "container pynixd Error with exit 3"


def test_deletion_is_the_notice() -> None:
    found = machine(_pod("Running", deletionTimestamp="2026-11-21T12:00:30Z"))
    assert found is not None
    assert found.state is MachineState.RUNNING
    assert found.notice == 1795262430.0


def test_pod_without_tags_is_not_a_machine() -> None:
    assert machine({"metadata": {"name": "p"}, "status": {"phase": "Running"}}) is None
    assert machine({"metadata": {"name": "p", "annotations": {TAGS_ANNOTATION: "[]"}}}) is None
    assert machine({"metadata": {"name": "p", "annotations": {TAGS_ANNOTATION: "{"}}}) is None


@pytest.mark.parametrize(
    "error",
    [
        # A new cluster before its RBAC is in place: this one ended pynixd in the vivarium test.
        kr8s.ServerError('forbidden: User "kubernetes-admin" cannot get path "/version/"'),
        kr8s.NotFoundError("no such pod"),
        kr8s.ConnectionClosedError("closed"),
        httpx.ConnectError("[Errno 111] Connection refused"),
        ConnectionRefusedError(111, "Connection refused"),
    ],
    ids=type,
)
async def test_api_failures_are_provider_errors(error: Exception) -> None:
    with pytest.raises(ProviderError, match="kubernetes: listing pods: ") as raised, failing("listing pods"):
        raise error
    assert raised.value.__cause__ is error


async def test_other_errors_pass_through() -> None:
    with pytest.raises(KeyError), failing("listing pods"):
        raise KeyError("a bug, not a failed call")


async def test_unanswered_call_is_a_provider_error() -> None:
    """kr8s sets no timeout: without this bound, an unanswered request holds the provisioner's loop."""
    with (
        pytest.raises(ProviderError, match=r"kubernetes: listing pods: no answer in 0\.05 s"),
        failing("listing pods", seconds=0.05),
    ):
        await anyio.Event().wait()


def test_kubernetes_provider_needs_its_settings() -> None:
    pool = {"name": "k", "provider": "kubernetes", "template": "t", "systems": ["x86_64-linux"]}
    with pytest.raises(ValueError, match='provider "kubernetes" needs the kubernetes settings'):
        ProvisioningSettings.model_validate({"pools": [pool]})
    settings = ProvisioningSettings.model_validate(
        {"pools": [pool], "kubernetes": {"templates": {"t": {"pod": {"spec": {"containers": [{"name": "c"}]}}}}}}
    )
    assert isinstance(settings.kubernetes, KubernetesSettings)
    assert settings.kubernetes.templates["t"].ticket_dir == Path("/run/pynixd-join")
