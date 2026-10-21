"""The map of pulled outputs keeps the newest entries only. nixidae/nixidae#1003."""

from __future__ import annotations

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import context
from pynixd.context import OutputLocations


def test_the_oldest_location_goes_first(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(context, "OUTPUT_LOCATIONS_KEPT", 2)
    locations = OutputLocations()

    for name in ("a", "b", "c"):
        locations[f"/nix/store/{name}"] = StoreId("sg1-n1")

    assert list(locations) == ["/nix/store/b", "/nix/store/c"]
