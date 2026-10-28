"""A socket path pynixd cannot bind is refused where the path is still visible.

`sun_path` in `struct sockaddr_un` is `char[108]` on Linux and `char[104]` on
darwin. The kernel copies the path into that array, so a longer one cannot be
bound.

Without this check the failure arrives late and from the wrong place. The
croshome session found it on macOS while starting pynixd by hand:
`_ensure_unix_socket_parent` made the directory, the server started, and
uvloop raised `OSError: AF_UNIX path too long` -- with no path in the message
and no mention of a limit.

The module default `/run/pynixd/pynixd.sock` is 25 bytes. This reaches anyone
who sets `unix_path` under a long prefix, which a test harness does routinely:
`tests/differential/conftest.py` carries the same limit for the same reason.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pynixd.instance import _SUN_PATH_LIMIT, _SUN_PATH_LIMIT_DEFAULT, Server


def _path_of_length(length: int) -> Path:
    """A path exactly *length* bytes long."""
    prefix = "/tmp/"
    return Path(prefix + "a" * (length - len(prefix)))


def test_the_darwin_limit_is_the_smaller_of_the_two() -> None:
    """The default has to be safe on both, so it is the stricter one."""
    assert _SUN_PATH_LIMIT["darwin"] == 104
    assert _SUN_PATH_LIMIT["linux"] == 108
    assert min(_SUN_PATH_LIMIT.values()) == _SUN_PATH_LIMIT_DEFAULT


def test_a_path_over_the_limit_is_refused_by_name() -> None:
    """The error names the path, the size and the limit. That is the point."""
    too_long = _path_of_length(200)
    with pytest.raises(RuntimeError) as caught:
        Server._check_unix_socket_length(too_long)
    message = str(caught.value)
    assert "200 bytes" in message
    assert str(too_long) in message
    assert "unix_path" in message


def test_a_path_inside_the_limit_passes() -> None:
    Server._check_unix_socket_length(_path_of_length(_SUN_PATH_LIMIT_DEFAULT))


def test_the_module_default_is_nowhere_near_the_limit() -> None:
    """`/run/pynixd/pynixd.sock` is what the NixOS module sets, and darwin can bind it."""
    Server._check_unix_socket_length(Path("/run/pynixd/pynixd.sock"))


def test_the_check_runs_before_the_directory_is_made(tmp_path: Path) -> None:
    """The whole value of the check is that it fires first.

    `_ensure_unix_socket_parent` used to `mkdir` and then let the bind fail, so
    a refused configuration still left directories behind.
    """
    parent = tmp_path / ("b" * 120)
    with pytest.raises(RuntimeError, match="Unix socket path is"):
        Server._ensure_unix_socket_parent(parent / "pynixd.sock")
    assert not parent.exists(), "the check must refuse before it creates anything"
