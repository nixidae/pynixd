"""A store spec's `overlay` makes the daemon's `local-overlay://` URI, as `--store <root>` makes a chroot store's.

`LocalOverlayStoreConfig` (`local-overlay-store.hh:26-74`) reads `lower-store`,
`upper-layer`, `check-mount` and `remount-hook`, and the store needs the
`local-overlay-store` experimental feature (`:81`).
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from pydantic import ValidationError

from nix_daemon_protocol.ids import StoreId
from pynixd.config import LocalSocketStoreSpec, OverlaySpec
from pynixd.store.local_db import LocalDBStore


def _spec(*, check_mount: bool = True, remount_hook: Path | None = None) -> LocalSocketStoreSpec:
    return LocalSocketStoreSpec(
        store_id=StoreId("local"),
        store_path=Path("/s/merged"),
        overlay=OverlaySpec(
            lower_store=Path("/s/lower"),
            upper_layer=Path("/s/upper"),
            check_mount=check_mount,
            remount_hook=remount_hook,
        ),
    )


def test_the_daemon_gets_the_overlay_uri_and_the_feature() -> None:
    layout = _spec(remount_hook=Path("/bin/remount")).layout()
    args = layout.daemon_arguments()
    assert args[0] == "--store"
    assert len(args) == 2
    assert layout.daemon_features() == ["--extra-experimental-features", "local-overlay-store"]
    uri = urlsplit(args[1])
    assert uri.scheme == "local-overlay"
    assert parse_qs(uri.query) == {
        "root": ["/s/merged"],
        "lower-store": ["/s/lower"],
        "upper-layer": ["/s/upper"],
        "check-mount": ["true"],
        "remount-hook": ["/bin/remount"],
    }


def test_the_layout_is_the_merged_directory_over_the_lower_store() -> None:
    layout = _spec(check_mount=False).layout()
    assert layout.real_store_dir == Path("/s/merged/nix/store")
    assert layout.overlay is not None
    assert layout.overlay.lower.real_store_dir == Path("/s/lower/nix/store")
    assert layout.overlay.lower.db_path == Path("/s/lower/nix/var/nix/db/db.sqlite")
    assert layout.overlay.upper_path("abc-x") == Path("/s/upper/abc-x")
    assert "check-mount=false" in layout.daemon_arguments()[1]


def test_the_feature_comes_after_the_store_s_settings() -> None:
    """An `experimental-features` option in `extra_args` would otherwise replace the set."""
    spec = _spec().model_copy(update={"extra_args": ["--option", "experimental-features", "nix-command"]})
    args = LocalDBStore(spec)._daemon.config_arguments()
    assert args[-2:] == ["--extra-experimental-features", "local-overlay-store"]


def test_a_relocated_store_takes_no_overlay() -> None:
    with pytest.raises(ValidationError, match="overlay needs a chroot root"):
        LocalSocketStoreSpec(
            store_dir=Path("/s/store"),
            state_dir=Path("/s/state"),
            log_dir=Path("/s/log"),
            overlay=OverlaySpec(lower_store=Path("/s/lower"), upper_layer=Path("/s/upper")),
        )


def test_an_overlay_store_uses_no_fast_path_yet() -> None:
    refusal = LocalDBStore(_spec())._refuses_a_database()
    assert refusal is not None
    assert "overlay store" in refusal
