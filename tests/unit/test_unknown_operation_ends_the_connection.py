"""An operation that pynixd does not know ends the connection.

Nothing reads the arguments of an unknown operation, so a loop that continues
reads the first argument as the next operation number. `nix-daemon` closes the
connection instead: `performOp` throws `invalid operation` at
`daemon.cc:1107`, before `logger->startWork()`, so `errorAllowed` at
`daemon.cc:1218` is false and the outer catch at `daemon.cc:1232` returns.
Issue nixidae/nixidae#232.
"""

from __future__ import annotations

import pytest

from nix_daemon_protocol.operations import STANDARD_OPERATIONS
from nix_daemon_protocol.wire_ops import WIRE_REGISTRY
from pynixd.handlers._base import HANDLER_REGISTRY
from tests.unit.loop_proxy import LoopProxy as FakeProxy

_UNKNOWN_OP = 4242
"""No operation of Nix carries this code, and none is reserved for it."""


@pytest.mark.anyio
async def test_an_unknown_operation_ends_the_loop() -> None:
    """The error goes out, and the loop stops rather than read an argument."""
    proxy = FakeProxy([_UNKNOWN_OP, 1])

    await proxy.run()

    assert proxy.errors == [f"Unsupported operation: {_UNKNOWN_OP}"]
    assert proxy.dispatched == []
    # The close happens before dispatch, so the unknown op leaves no timing
    # and no metric series behind.
    assert proxy._op_timing == {}
    assert proxy._op_metrics == {}
    # One read, and not two: the code of `IsValidPath` after it stays unread,
    # because that byte could equally be an argument of the unknown operation.
    assert proxy.r.reads == 1


@pytest.mark.anyio
async def test_a_known_operation_keeps_the_loop_going() -> None:
    """The close is for the unknown operation alone."""
    proxy = FakeProxy([1, 1])

    await proxy.run()

    assert proxy.errors == []
    assert proxy.dispatched == [1, 1]


def test_every_standard_operation_has_a_codec() -> None:
    """A gap in the manifest is what made the desync invisible."""
    known = set(WIRE_REGISTRY) | set(HANDLER_REGISTRY)
    missing = {op.code: op.name for op in STANDARD_OPERATIONS if op.code not in known}
    assert missing == {}


def test_the_manifest_leaves_out_only_what_no_client_sends() -> None:
    """The operations of Nix that this package answers with a close.

    Each entry names why it is out. `worker-protocol.hh` of Nix is the list to
    compare against. SubmitOutput (1000) and AddToStoreScanning (1001) are in
    the manifest: a `builder-rpc-v0` builder's own session serves them
    (`pynixd/recursive.py`), and every other session closes on them.
    """
    left_out = {
        8: "AddTextToStore, obsolete since protocol 1.25; the floor is 1.35",
        13: "SyncWithGC; no current RemoteStore sends it",
        18: "QueryDeriver, obsolete",
        22: "QueryDerivationOutputs, obsolete",
        28: "QueryDerivationOutputNames, obsolete",
    }
    named = {op.code for op in STANDARD_OPERATIONS}
    assert not named & set(left_out)
    for code, reason in left_out.items():
        assert reason, code


def test_the_manifest_holds_the_two_substituter_operations() -> None:
    """Both were missing, and both are reachable. Issue nixidae/nixidae#232."""
    names = {op.code: op.name for op in STANDARD_OPERATIONS}
    assert names[21] == "QuerySubstitutablePathInfo"
    assert names[30] == "QuerySubstitutablePathInfos"
