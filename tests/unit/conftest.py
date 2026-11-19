"""Unit test overrides: disable the session-scoped pynixd_server fixture.

The default conftest.py has an autouse session-scoped fixture that starts
a real pynixd daemon (SSH, HTTP, Unix socket). Unit tests don't need this,
and it adds ~30s of startup time.
"""

import pytest

from pynixd.instance import preload


@pytest.fixture(scope="session", autouse=False)
def pynixd_server() -> None:
    """Override: unit tests don't need a real pynixd server."""
    return


@pytest.fixture(scope="session", autouse=True)
def preloaded() -> None:
    """Load what `Server.start` loads in a worker thread.

    A unit test drives store code with no server, so without this the first
    test to touch a model compiles its codec on the event loop, and the loop
    audit fails whichever test that is.
    """
    preload()
