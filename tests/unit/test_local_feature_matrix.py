"""The machine's own daemon is described by its nix.conf, not probed (#58)."""

from __future__ import annotations

from pynixd.store.local_host import feature_matrix_from_nix_config


def setting(value: object) -> dict[str, object]:
    return {"value": value, "defaultValue": None, "description": ""}


def test_every_platform_gets_every_feature() -> None:
    config = {
        "system": setting("x86_64-linux"),
        "extra-platforms": setting(["i686-linux"]),
        "system-features": setting(["kvm", "nixos-test"]),
    }
    assert feature_matrix_from_nix_config(config) == {
        "x86_64-linux": {"kvm", "nixos-test"},
        "i686-linux": {"kvm", "nixos-test"},
    }


def test_no_features_still_names_the_system() -> None:
    config = {
        "system": setting("aarch64-darwin"),
        "extra-platforms": setting([]),
        "system-features": setting([]),
    }
    assert feature_matrix_from_nix_config(config) == {"aarch64-darwin": set()}
