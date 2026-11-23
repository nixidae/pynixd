"""What central gives a build pod, and what the pod gives back. nixidae/nixidae#1560, spike.

Central writes a `Job` and an SSH key into the pod's Secret. The pod writes
the `BuildDerivation` response, as its wire form in base64, to its
termination message.
"""

from __future__ import annotations

import base64
from typing import TYPE_CHECKING, Final

from pydantic import BaseModel, ConfigDict

from nix_daemon_protocol.build_derivation import BuildDerivationRequest, BuildDerivationResponse
from nix_daemon_protocol.constants import PROTOCOL_VERSION

from .serde.context import ReadContext, WriteContext
from .wire import BytesReader, BytesWriter

if TYPE_CHECKING:
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


class Job(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

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


async def result_message(resp: BuildDerivationResponse) -> str:
    """*resp* as a termination message, its error text cut to fit.

    A pod's store that keys realisations by path (Nix 2.35) fills only `built_outputs_by_name`, which the job's
    empty feature set does not write: `for_the_wire` fills the map it does. Measured in a nixkube farm pod.
    """
    result = resp.result.for_the_wire(JOB_FEATURES)
    if len(result.error_msg) > _ERROR_ROOM:
        result = result.model_copy(update={"error_msg": "..." + result.error_msg[-_ERROR_ROOM:]})
    return await encode(BuildDerivationResponse(result=result))
