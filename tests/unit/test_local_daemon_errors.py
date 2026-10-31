"""What `LocalStore.ensure_daemon` says when the daemon does not come up.

These tests spawn a shell script in place of `nix`, so they need no Nix, no
store and no socket. The script is the whole fixture: it writes a line and
exits, which is what a daemon that cannot start does.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import pytest

from pynixd.store.local_db import LocalDBStore
from tests.conftest import make_test_spec

if TYPE_CHECKING:
    from pathlib import Path

# The line the fake daemon writes. Distinctive, so a test that finds it in the
# error message has really carried it through and not matched a common word.
_DISTINCTIVE = "FATAL: refusing to start, for a reason only the daemon knows"


def _fake_nix(tmp_path: Path, *, exit_code: int) -> Path:
    """A program that writes to stderr and exits, in place of `nix daemon`.

    The sleep matters. `_stream_daemon_output` reads the output in a separate
    task, and `ensure_daemon` notices the exit on a 0.1s poll. Without the
    pause the process could be reaped before its line is read, and the test
    would pass or fail on timing rather than on behaviour.
    """
    script = tmp_path / "fake-nix"
    script.write_text(f"#!/bin/sh\necho '{_DISTINCTIVE}' >&2\nsleep 0.3\nexit {exit_code}\n")
    script.chmod(0o755)
    return script


@pytest.mark.skipif(sys.platform == "win32", reason="the fake daemon is a shell script")
async def test_a_daemon_that_dies_reports_what_the_daemon_said(tmp_path: Path) -> None:
    """The error names the daemon's own output, and not the reader's failure.

    Every error path of `ensure_daemon` used to call `daemon_proc.stderr.read()`
    to build this message. `_stream_daemon_output` already holds a `readline()`
    on that stream, so the read raised `RuntimeError: read() called while
    another coroutine is already waiting for incoming data`, and that is the
    only thing a failed start-up could report. The real cause was never
    printed. This test is what keeps the real cause printed.
    """
    store = LocalDBStore(
        make_test_spec(
            store_id="fake-daemon",
            store_path=tmp_path / "store",
            nix_bin=str(_fake_nix(tmp_path, exit_code=3)),
        ),
    )

    with pytest.raises(RuntimeError) as raised:
        async with store:
            pass

    message = str(raised.value)
    assert _DISTINCTIVE in message, f"the daemon's own output is missing from:\n{message}"
    assert "already waiting for incoming data" not in message, (
        f"the error is the reader's failure and not the daemon's:\n{message}"
    )
    assert "code 3" in message, f"the exit code is missing from:\n{message}"
