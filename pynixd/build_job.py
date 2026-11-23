"""What central gives a build pod, and what the pod gives back. nixidae/nixidae#1560, spike.

Central writes a `Job` and an SSH key into the pod's Secret. The pod writes
the `BuildDerivation` response, as its wire form in base64, to its
termination message.
"""

from __future__ import annotations

import base64
from enum import StrEnum
from typing import TYPE_CHECKING, Final

from pydantic import BaseModel, ConfigDict

from nix_daemon_protocol.build_derivation import BuildDerivationRequest, BuildDerivationResponse
from nix_daemon_protocol.constants import PROTOCOL_VERSION

from .serde.context import ReadContext, WriteContext
from .wire import BytesReader, BytesWriter

if TYPE_CHECKING:
    from collections.abc import Mapping

    from nix_daemon_protocol.wire_message import WireModel

JOB_NAME: Final = "job.json"
KEY_NAME: Final = "key"
RESULT_BYTES: Final = 4096
"""The kubelet keeps this much of a container's termination message (`MaxContainerTerminationMessageLength`)."""
_ERROR_ROOM: Final = 2048
"""How much of a failure's text fits: base64 is a third longer than its bytes, and the rest of the result is small."""
JOB_FEATURES: Final = frozenset[str]()
"""The protocol features the job and its result use: none. Central writes them and the pod reads them, or the other
way, with no handshake between the two to agree on one."""


class RunnerJob(BaseModel):
    """The settings central computed the runner container's invocation with: the controller builds with them, so
    its invocation is the same (`pynixd_store.builder.runner.planned_invocation`)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    build_cores: int
    impure_env: dict[str, str] = {}


class Job(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    runner: RunnerJob | None = None

    central_host: str
    central_port: int
    central_host_key: str
    """Central's SSH host key, an OpenSSH public key line: the pod accepts no other."""
    request: str
    """The `BuildDerivation` request in base64 wire form: the derivation need not be valid in central's store."""


async def encode(model: WireModel) -> str:
    writer = BytesWriter("job")
    await model.to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION, features=JOB_FEATURES))
    return base64.b64encode(writer.get_bytes()).decode()


async def decode_request(text: str) -> BuildDerivationRequest:
    reader = BytesReader(base64.b64decode(text), "job")
    # A request's wire form starts with its op code, which `from_reader` does not read.
    if (op := await reader.read_uint64()) != BuildDerivationRequest.op:
        raise ValueError(f"the job holds operation {op}, not BuildDerivation")
    return await BuildDerivationRequest.from_reader(
        ReadContext(reader=reader, version=PROTOCOL_VERSION, features=JOB_FEATURES)
    )


async def decode_response(text: str) -> BuildDerivationResponse:
    reader = BytesReader(base64.b64decode(text), "result")
    return await BuildDerivationResponse.from_reader(
        ReadContext(reader=reader, version=PROTOCOL_VERSION, features=JOB_FEATURES)
    )


class PodPart(StrEnum):
    """A part of a build pod's fixed cost, as the pod measures it."""

    PYTHON = "python"
    """From the process's start to `build-one`'s first line: the interpreter and the imports."""
    SERVER = "server"
    """pynixd's server start, until its stores opened."""
    INPUTS = "inputs"
    """The inputs from central, with the SSH connection."""
    BUILD = "build"
    OUTPUTS = "outputs"
    SHUTDOWN = "shutdown"


class CentralPart(StrEnum):
    """A part of a build pod's fixed cost, as central sees it."""

    CREATE = "create"
    """The pod and its Secret, until the API server took both."""
    START = "start"
    """Until the container ran: scheduling, the volumes, the image, the container start."""
    FIRST_LINE = "first-line"
    """From the container's start to its first log line."""
    END = "end"
    """From the last log line until central read the result: the process's end, and the kubelet's report."""
    TOTAL = "total"


POD_SECONDS: Final = "pynixd build-one: seconds:"
CENTRAL_SECONDS: Final = "pynixd: build pod seconds:"


def seconds_line[Part: StrEnum](prefix: str, parts: Mapping[Part, float]) -> str:
    """One log line of each part's seconds, which `read_seconds` reads back."""
    return " ".join([prefix, *(f"{part.value}={seconds:.3f}" for part, seconds in parts.items())])


def read_seconds(prefix: str, line: str) -> dict[str, float] | None:
    """The parts of a `seconds_line` with *prefix*, or `None` for any other line."""
    if not line.startswith(prefix):
        return None
    return {name: float(seconds) for name, _, seconds in (word.partition("=") for word in line[len(prefix) :].split())}


async def result_message(resp: BuildDerivationResponse) -> str:
    """*resp* as a termination message, its error text cut to fit.

    A pod's store that keys realisations by path (Nix 2.35) fills only `built_outputs_by_name`, which the job's
    empty feature set does not write: `for_the_wire` fills the map it does. Measured in a nixkube farm pod.
    """
    result = resp.result.for_the_wire(JOB_FEATURES)
    if len(result.error_msg) > _ERROR_ROOM:
        result = result.model_copy(update={"error_msg": "..." + result.error_msg[-_ERROR_ROOM:]})
    return await encode(BuildDerivationResponse(result=result))
