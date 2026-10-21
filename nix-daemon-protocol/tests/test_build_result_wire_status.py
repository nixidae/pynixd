"""Every `BuildResult` this package writes has a status a client can decode.

The wire carries the status as one byte, and a Nix client indexes a 15-entry
table with it -- `buildResultStatusTable`, `src/libstore/common-protocol.cc`.
An index at or above that size raises `Invalid BuildResult status ... from
remote`, and the client drops the connection. A build that merely failed then
reads to that client as a broken daemon.

This was found in the wild. A `nix copy` out of pynixd failed with

    error: Invalid BuildResult status f from remote

and `f` is not a number: the C++ format is `%d` against a `uint8_t`, which
boost renders as a character. ASCII 102 is `f`, and 102 is `UNKNOWN`.
"""

from __future__ import annotations

import pytest

from nix_daemon_protocol.build_result import MAX_WIRE_STATUS, BuildResult, BuildResultStatus
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.unkeyed_realisation import UnkeyedRealisation


def test_the_wire_table_size_matches_the_one_nix_reads() -> None:
    """`MAX_WIRE_STATUS` names the last index of Nix's table.

    Nix lists 15 statuses, 0 to 14, ending at `NoSubstituters`. If upstream
    adds one, this is the constant to move, and this test is what says so.
    """
    assert MAX_WIRE_STATUS == 14
    assert BuildResultStatus.NO_SUBSTITUTERS == MAX_WIRE_STATUS


@pytest.mark.parametrize(
    "status",
    [status for status in BuildResultStatus if status <= MAX_WIRE_STATUS],
    ids=lambda status: status.name,
)
def test_a_status_the_wire_has_a_number_for_is_sent_unchanged(status: BuildResultStatus) -> None:
    """The mapping touches nothing it does not have to."""
    assert BuildResult(status=int(status)).wire_status() == int(status)


def test_a_hash_mismatch_is_sent_as_an_output_rejection() -> None:
    """The substitution Nix itself chose, so a client reads what it expects.

    `CommonProto::Serialise<BuildResultStatus>::write` does this, with the
    comment "hash mismatch is a type of output rejection".
    """
    result = BuildResult(status=int(BuildResultStatus.HASH_MISMATCH))
    assert result.wire_status() == int(BuildResultStatus.OUTPUT_REJECTED)


def test_for_the_wire_returns_the_same_object_when_nothing_changes() -> None:
    """No copy for the common case, which is every successful build."""
    result = BuildResult(status=int(BuildResultStatus.BUILT))
    assert result.for_the_wire() is result


def test_for_the_wire_keeps_every_other_field() -> None:
    """Only the status moves. The message is where the detail survives."""
    result = BuildResult(status=int(BuildResultStatus.UNKNOWN), error_msg="pynixd: nothing to realise")
    on_the_wire = result.for_the_wire()
    assert on_the_wire.status == int(BuildResultStatus.MISC_FAILURE)
    assert on_the_wire.error_msg == "pynixd: nothing to realise"
    assert result.status == int(BuildResultStatus.UNKNOWN), "the original must not be mutated"


def test_an_unknown_status_is_sent_as_a_plain_failure() -> None:
    """`UNKNOWN` is this project's sentinel, and the wire has no number for it.

    102 is what went out before, and it is the value that produced the error
    this module's docstring quotes.
    """
    result = BuildResult(status=int(BuildResultStatus.UNKNOWN))
    assert result.wire_status() == int(BuildResultStatus.MISC_FAILURE)


@pytest.mark.parametrize("status", [-1, 15, 101, 102, 255, 1000])
def test_no_status_outside_the_table_ever_reaches_the_wire(status: int) -> None:
    """The rule, stated over the values rather than over the names.

    A status this package does not know is still a status a client cannot
    read, so the guard is a range check and not a lookup of the two sentinels
    that exist today.
    """
    assert 0 <= BuildResult(status=status).wire_status() <= MAX_WIRE_STATUS


def test_an_old_client_gets_keys_with_the_dummy_hash() -> None:
    """Nix 2.35's reader throws "Invalid derivation output id" on a key
    with no `!` (`worker-protocol.cc:272`), and Nix writes the dummy hash
    there (`worker-protocol.cc:313`). nixidae/nixidae#1003."""
    path = StorePath("/nix/store/00000000000000000000000000000001-out")
    result = BuildResult(
        status=int(BuildResultStatus.BUILT),
        built_outputs_by_name={"out": UnkeyedRealisation(out_path=path, signatures=set())},
    )

    old = result.for_the_wire(frozenset())

    key = "sha256:" + "0" * 64 + "!out"
    assert old.built_outputs is not None
    assert list(old.built_outputs) == [key]
    assert old.built_outputs[key].id == key
    assert old.realised_outputs()["out"].out_path
