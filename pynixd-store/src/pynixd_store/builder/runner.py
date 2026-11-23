"""A build whose builder is a container of its own pod: the runner (nixidae/nixidae#1560, spike).

The pod has two containers. The controller runs `build()`; the runner's own process is the builder, and nixkube's
NRI plugin makes that container Nix's sandbox. Central writes the builder, its arguments and its environment into
the runner's spec when it writes the pod. The controller prepares the build directory at a fixed `top`, which the
runner mounts by subPath at `/build`, and then touches `ready`: the controller's postStart hook waits for that
file, and the kubelet starts the runner after the hook.

The builder writes its outputs straight into the farm's store, which the controller sees as its own: `build()`
treats the build as one without the chroot, with the sandbox's `/build`.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from nix_daemon_protocol.derivation_output import OutputKind

from ..drv_parser import BUILDER_RPC
from .env import (
    NETWORK_NSSWITCH,
    SANDBOX_BUILD_DIR,
    SANDBOX_HOSTS,
    build_env,
    rewrite_strings,
    sandbox_group,
    sandbox_passwd,
)
from .outputs import known_paths, output_rewrites, scratch_paths

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from nix_daemon_protocol.basic_derivation import BasicDerivation
    from nix_daemon_protocol.store_path import StorePath

SANDBOX_ANNOTATION: Final = "nixkube.nixidae.org/build-sandbox"
"""The pod's annotation that names its runner container, which nixkube makes the sandbox
(`nixkube/pkgs/nixkube/src/nri/sandbox.py`)."""
NETWORK_ANNOTATION: Final = "nixkube.nixidae.org/build-network"
"""`"true"`: the runner keeps the pod's network, as a fixed-output build does."""
STATUS_NAME: Final = "runner-status"
"""The helper's set-up error, in the top directory."""
EXIT_NAME: Final = "runner-exit"
"""The builder's exit status once the runner ended, in the top directory."""
POD_NAME_VARIABLE: Final = "PYNIXD_POD_NAME"
POD_NAMESPACE_VARIABLE: Final = "PYNIXD_POD_NAMESPACE"
"""The downward API's name and namespace of the controller's own pod."""


@dataclass(frozen=True, slots=True)
class RunnerSandbox:
    """Where the controller prepares the build, the file that lets the runner start, and the runner's name."""

    top: Path
    ready: Path
    container: str


ETC_NAME: Final = "etc"
"""The runner's `/etc` files, in the top directory: the template mounts each of `etc_files` by subPath."""


def etc_files(*, uid: int, gid: int, network: bool, pod_hosts: str) -> dict[str, str]:
    """The sandbox's `/etc` for the runner. A build with the network gets the pod's `/etc/hosts`, as Nix binds
    the host's. The template mounts the same names for every build, so a build without the network gets an
    empty `nsswitch.conf`: glibc reads it as no file at all."""
    return {
        "passwd": sandbox_passwd(uid, gid),
        "group": sandbox_group(gid),
        "hosts": pod_hosts if network else SANDBOX_HOSTS,
        "nsswitch.conf": NETWORK_NSSWITCH if network else "",
    }


def _escape(value: str) -> str:
    """Kubernetes expands `$(name)` in a container's command, arguments and variables; `$$` gives `$`."""
    return value.replace("$", "$$")


@dataclass(frozen=True, slots=True)
class RunnerInvocation:
    """The parts of the runner container's spec that make the builder's process, as Kubernetes holds them."""

    command: tuple[str, ...]
    args: tuple[str, ...]
    env: tuple[tuple[str, str | None], ...]
    working_dir: str

    def container_fields(self) -> dict[str, object]:
        return {
            "command": list(self.command),
            "args": list(self.args),
            "env": [{"name": name, "value": value} for name, value in self.env],
            "workingDir": self.working_dir,
        }


def invocation(builder: str, args: Sequence[str], env: Mapping[str, str]) -> RunnerInvocation:
    """The runner's spec for a builder: its fields escaped, so the builder gets each `$` as written."""
    return RunnerInvocation(
        command=(_escape(builder),),
        args=tuple(_escape(arg) for arg in args),
        env=tuple((name, _escape(value)) for name, value in sorted(env.items())),
        working_dir=SANDBOX_BUILD_DIR,
    )


def drv_refusal(drv: BasicDerivation) -> str | None:
    """Why the runner container cannot run *drv*, from the derivation alone: central and the controller both ask."""
    if drv.builder.startswith("builtin:"):
        return f"the runner container cannot run the builtin builder '{drv.builder}'"
    if "recursive-nix" in drv.required_system_features or BUILDER_RPC in drv.required_system_features:
        return "the runner container has no socket for a recursive build"
    if "uid-range" in drv.required_system_features:
        return "the runner container runs the build as the controller's uid, not as a build user"
    if any(o.kind is OutputKind.IMPURE for o in drv.outputs.values()):
        # `known_paths` gives an impure output a random path, which central cannot write before the controller.
        return "the runner container cannot build an impure derivation"
    words = [drv.builder, *drv.args, *drv.env, *drv.env.values()]
    if not all(_utf8(word) for word in words):
        return "the runner container takes only UTF-8 in the builder, its arguments and its environment"
    if bad := sorted(name for name in drv.env if not _env_name(name)):
        return f"Kubernetes refuses the variable names {', '.join(map(repr, bad))}"
    if sum(len(word.encode()) for word in words) > INVOCATION_BYTES:
        return f"the builder's arguments and environment exceed the {INVOCATION_BYTES} bytes a pod holds"
    return None


INVOCATION_BYTES: Final = 1 << 20
"""What the runner's command, arguments and environment may take: etcd keeps an object of 1.5 MiB at most, and the
pod's other fields need the rest."""


def _utf8(word: str) -> bool:
    """A string of the wire decodes with `surrogateescape`: a byte that is not UTF-8 does not encode back."""
    try:
        word.encode()
    except UnicodeEncodeError:
        return False
    return True


def _env_name(name: str) -> bool:
    """`RelaxedEnvironmentVariableValidation` (GA in Kubernetes 1.34): printable ASCII with no `=`."""
    return bool(name) and all(" " <= c <= "~" for c in name) and "=" not in name


def planned_invocation(
    drv_path: StorePath, drv: BasicDerivation, *, store_dir: str, build_cores: int, impure_env: Mapping[str, str]
) -> RunnerInvocation:
    """The invocation `build()` gives *drv* in a runner pod whose store holds none of its outputs and no
    realisation: what central writes before the controller runs. The helper compares the two (`mismatch`)."""
    drv_name = drv_path.base_name().removesuffix(".drv")
    scratch = scratch_paths(drv_path, drv, known_paths(drv, {}, drv_name), (), chroot=False, store_dir=store_dir)
    rewrites = output_rewrites(scratch, {})
    fixed = any(o.kind is OutputKind.CA_FIXED for o in drv.outputs.values())
    env = build_env(
        drv.env,
        outputs=drv.outputs,
        store_dir=store_dir,
        build_cores=build_cores,
        rewrites=rewrites,
        fixed_output=fixed,
        sandboxed=not fixed,
        impure_source=impure_env,
        # `exportReferencesGraph` fills files of the build directory, never a variable.
        exported={},
    )
    return invocation(drv.builder, [rewrite_strings(arg, rewrites) for arg in drv.args], env.variables)


def from_container(container: Mapping[str, Any]) -> RunnerInvocation:  # Any: a container of the API's JSON
    """The invocation a container of the API's pod holds. A variable from a reference has no value: `None`."""
    return RunnerInvocation(
        command=tuple(container.get("command") or ()),
        args=tuple(container.get("args") or ()),
        env=tuple((var["name"], var.get("value")) for var in container.get("env") or ()),
        working_dir=container.get("workingDir") or "",
    )


def mismatch(expected: RunnerInvocation, found: RunnerInvocation) -> str | None:
    """What differs between the controller's invocation and the runner's spec, or `None`."""
    if found.command != expected.command:
        return f"its command is {list(found.command)!r}, not {list(expected.command)!r}"
    if found.args != expected.args:
        return f"its arguments are {list(found.args)!r}, not {list(expected.args)!r}"
    if found.working_dir != expected.working_dir:
        return f"its working directory is {found.working_dir!r}, not {expected.working_dir!r}"
    want, have = dict(expected.env), dict(found.env)
    differs = (n for n in want.keys() | have.keys() if n not in want or n not in have or want[n] != have[n])
    if names := sorted(differs):
        return f"its environment differs in {', '.join(names)}"
    return None


def exit_status(terminated: Mapping[str, Any]) -> int:  # Any: a container state of the API's JSON
    """The builder's status as `Popen.returncode` gives it, from a container's `state.terminated`: a signal that
    the runtime names is negative."""
    if signal := terminated.get("signal"):
        return -int(signal)
    return int(terminated.get("exitCode", 0))


FAILED_WAITING: Final = frozenset(
    {
        "CreateContainerConfigError",
        "CreateContainerError",
        "ErrImageNeverPull",
        "ErrImagePull",
        "ImagePullBackOff",
        "InvalidImageName",
        "RunContainerError",
    }
)
"""A waiting container's reasons that mean it will not start without a change: the build ends at once."""


@dataclass(frozen=True, slots=True)
class RunnerState:
    """The runner container, as one read of its pod shows it."""

    started: bool
    """It runs or ran: its log can be read."""
    terminated: Mapping[str, Any] | None  # Any: the API's `state.terminated`
    refused: str | None
    """Why it will not start."""


def runner_state(pod: Mapping[str, Any], container: str) -> RunnerState:  # Any: a pod of the API's JSON
    statuses = pod.get("status", {}).get("containerStatuses") or ()
    status = next((s for s in statuses if s.get("name") == container), None)
    if status is None:
        return RunnerState(started=False, terminated=None, refused=None)
    state: dict[str, Any] = status.get("state") or {}
    if (terminated := state.get("terminated")) is not None:
        return RunnerState(started=True, terminated=terminated, refused=None)
    if "running" in state:
        return RunnerState(started=True, terminated=None, refused=None)
    waiting: dict[str, Any] = state.get("waiting") or {}
    if waiting.get("reason") in FAILED_WAITING:
        message = waiting.get("message") or "no message"
        return RunnerState(started=False, terminated=None, refused=f"{waiting['reason']}: {message}")
    return RunnerState(started=False, terminated=None, refused=None)
