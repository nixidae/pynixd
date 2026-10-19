"""What the client reads after the word "error:".

`DaemonProxy` caught an exception and sent `repr(ex)`. A client of pynixd then
read `error: BackendError("Cannot build '\\x1b[35;1m/nix/store/...")` -- the
name of a Python class, a quoted string, and every escape of the message
doubled. Nix sends the message alone.

Refs nixidae/nixidae#214, nixidae/nixidae#227.
"""

from __future__ import annotations

from nix_daemon_protocol.exceptions import DaemonProtocolError
from pynixd.exceptions import BackendError, InfrastructureError
from pynixd.proxy import _error_text  # noqa: PLC2701 -- the format is the unit under test


def test_a_pynixd_error_sends_its_message_alone() -> None:
    message = "build of resolved derivation '/nix/store/aaa-x.drv' failed"
    assert _error_text(BackendError(message)) == message


def test_every_pynixd_error_reads_the_same_way() -> None:
    assert _error_text(InfrastructureError("the builder went away")) == "the builder went away"


def test_a_fault_of_pynixd_names_the_class() -> None:
    """A reader has to know that this one is not a message from Nix."""
    assert _error_text(KeyError("out")) == "KeyError: 'out'"


def test_the_message_keeps_its_escapes() -> None:
    """`repr` doubled each one, so the client printed the text of an escape."""
    assert _error_text(BackendError("a \x1b[31;1mred\x1b[0m word")) == "a \x1b[31;1mred\x1b[0m word"


def test_the_message_of_a_daemon_passes_through() -> None:
    """pynixd is a proxy, so the text of the daemon behind it is the text."""
    message = "Cannot delete path '/nix/store/aaa-x' since it is still alive."
    assert _error_text(DaemonProtocolError(message)) == message


def test_a_task_group_gives_the_reason_of_its_task() -> None:
    """`ca:signatures` reads the reason, and the group itself says nothing."""
    reason = "cannot add path '/nix/store/aaa-x' because it lacks a signature by a trusted key"
    group = ExceptionGroup("unhandled errors in a TaskGroup", [DaemonProtocolError(reason)])
    assert _error_text(group) == reason


def test_a_task_group_of_two_gives_both_reasons() -> None:
    group = ExceptionGroup("unhandled errors in a TaskGroup", [BackendError("first"), BackendError("second")])
    assert _error_text(group) == "first\nsecond"


def test_a_nested_task_group_gives_the_reason_as_well() -> None:
    """A handler that fans out inside a fan-out leaves one group inside another."""
    inner = ExceptionGroup("unhandled errors in a TaskGroup", [BackendError("the reason")])
    outer = ExceptionGroup("unhandled errors in a TaskGroup", [inner])
    assert _error_text(outer) == "the reason"
