"""The stores `pynixd.store_url` opens for substituter URLs."""

from __future__ import annotations

import pytest

from pynixd.store_url import spec_for


@pytest.mark.parametrize("url", ["unix:///run/peer/socket"])
def test_a_substituter_gets_no_probe_builds(url: str) -> None:
    """A substituter builds nothing; a probe build there costs its slots and can fail its start."""
    spec = spec_for(url)
    assert spec is not None
    assert spec.to_store("t")._probe is False
