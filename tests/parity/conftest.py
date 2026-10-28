"""Overrides: a parity test starts the daemons that it compares.

The default `conftest.py` starts one pynixd for the whole session, and every
test shares it. A parity test cannot use that server: it needs the store of
the control run and the store of the pynixd run to be one path, and it needs a
recorder in front of each daemon. So it starts its own, twice, and the shared
server would only cost the run its startup time.

`tests/unit/conftest.py` overrides the same fixture, for the same reason.
"""

import pytest


@pytest.fixture(scope="session", autouse=False)
def pynixd_server() -> None:
    """Override: a parity test starts its own daemon."""
    return
