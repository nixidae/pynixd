"""Overrides for the client suite: each example starts the two daemons it compares.

The shared session pynixd holds probe builds and other tests' paths. A
comparison needs both stores to start empty, so every example starts its own
pair, and the shared server would only cost the run its startup time.
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session", autouse=False)
def pynixd_server() -> None:
    """Override: an example starts its own daemons."""
    return
