"""Unit test overrides: disable the session-scoped pynixd_server fixture.

The default conftest.py has an autouse session-scoped fixture that starts
a real pynixd daemon (SSH, HTTP, Unix socket). Unit tests don't need this,
and it adds ~30s of startup time.
"""

import pytest


@pytest.fixture(scope="session", autouse=False)
def pynixd_server() -> None:
    """Override: unit tests don't need a real pynixd server."""
    return
