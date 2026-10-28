"""`WireResponse.fast` answers what `__init__` answers, without validation.

The daemon builds thousands of responses per build with values of the
declared types, so there is nothing to coerce and nothing to refuse. What
must still hold: the bytes match a validated response exactly, the logs are
fresh per response (callers append to them), and defaulted fields are filled.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from nix_daemon_protocol import proto
from nix_daemon_protocol.add_temp_root import AddTempRootResponse
from nix_daemon_protocol.context import WriteContext
from nix_daemon_protocol.io import BytesWriter
from nix_daemon_protocol.is_valid_path import IsValidPathResponse
from nix_daemon_protocol.logs import LogNext
from nix_daemon_protocol.query_path_info import QueryPathInfoResponse

if TYPE_CHECKING:
    from nix_daemon_protocol.wire_ops import WireResponse


async def _encode(response: WireResponse) -> bytes:
    writer = BytesWriter()
    await response.to_writer(WriteContext(writer=writer, version=proto(1, 38)))
    return writer.bytes()


async def test_fast_encodes_what_init_encodes() -> None:
    """Byte-identical answers for the responses the hot build builds."""
    assert await _encode(IsValidPathResponse.fast(valid=True)) == await _encode(IsValidPathResponse(valid=True))
    assert await _encode(AddTempRootResponse.fast(value=1)) == await _encode(AddTempRootResponse(value=1))
    assert await _encode(QueryPathInfoResponse.fast(valid=False)) == await _encode(QueryPathInfoResponse(valid=False))


async def test_fast_logs_are_fresh_per_response() -> None:
    """Appending to one response's logs reaches no other response."""
    first = IsValidPathResponse.fast(valid=True)
    second = IsValidPathResponse.fast(valid=True)

    first.logs.add(LogNext(text="warning: one\n"))

    assert len(first.logs.messages) == 1
    assert second.logs.messages == []
    assert isinstance(second.logs.messages, list)
    assert second.logs is not first.logs
    assert second.logs.messages is not first.logs.messages


async def test_fast_reports_the_fields_it_set() -> None:
    """`fields_set` names the body and the logs, as a decode would."""
    response = QueryPathInfoResponse.fast(valid=False)

    assert response.info is None
    assert response.__pydantic_fields_set__ == {"valid", "logs"}
