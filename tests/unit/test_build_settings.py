"""`BuildSettings` reads what `nix config show --json` and a session's overrides say."""

from __future__ import annotations

from pynixd_store.builder.settings import BuildSettings, SandboxPath

BUSYBOX = "/nix/store/r6zw8gd10c7qv9sifzg4r7h1s5qmrxjp-busybox-1.37.0/bin/busybox"


def test_the_json_map_names_each_source() -> None:
    """Nix 2.35 writes `PathsInChroot` as a map; its keys alone would bind the host's `/bin/sh`."""
    config = {
        "system": {"value": "x86_64-linux"},
        "sandbox-paths": {
            "value": {
                "/bin/sh": {"optional": False, "source": BUSYBOX},
                "/dev/kvm": {"optional": True, "source": "/dev/kvm"},
            }
        },
    }
    settings = BuildSettings.from_nix_config(config, {})
    assert set(settings.sandbox_paths) == {
        SandboxPath("/bin/sh", BUSYBOX, optional=False),
        SandboxPath("/dev/kvm", "/dev/kvm", optional=True),
    }


def test_an_override_is_words() -> None:
    settings = BuildSettings.from_nix_config(
        {"system": {"value": "x86_64-linux"}}, {"sandbox-paths": f"/bin/sh={BUSYBOX} /dev/kvm?"}
    )
    assert settings.sandbox_paths == (
        SandboxPath("/bin/sh", BUSYBOX, optional=False),
        SandboxPath("/dev/kvm", "/dev/kvm", optional=True),
    )


def test_extra_sandbox_paths_append_without_replacing_a_target() -> None:
    config = {"system": {"value": "x86_64-linux"}, "sandbox-paths": {"value": {"/bin/sh": {"source": BUSYBOX}}}}
    settings = BuildSettings.from_nix_config(config, {"extra-sandbox-paths": "/bin/sh=/other /dep=/nix/store/x-dep"})
    assert settings.sandbox_paths == (
        SandboxPath("/bin/sh", BUSYBOX, optional=False),
        SandboxPath("/dep", "/nix/store/x-dep", optional=False),
    )


def test_sandbox_paths_replace_what_extra_sandbox_paths_add() -> None:
    """`ClientSettings::apply` sets the overrides in sorted order: `extra-` first."""
    settings = BuildSettings.from_nix_config(
        {"system": {"value": "x86_64-linux"}}, {"extra-sandbox-paths": "/dep", "sandbox-paths": "/bin/sh"}
    )
    assert settings.sandbox_paths == (SandboxPath("/bin/sh", "/bin/sh", optional=False),)


def test_session_limits_override_the_config() -> None:
    config = {"system": {"value": "x86_64-linux"}, "max-silent-time": {"value": 10}, "timeout": {"value": 0}}
    settings = BuildSettings.from_nix_config(config, {"max-silent-time": "3", "timeout": "7"})
    assert (settings.max_silent_time, settings.timeout) == (3, 7)


def test_secret_key_files_take_extra_and_a_replacement() -> None:
    """`ClientSettings::apply` walks the overrides sorted: `extra-` appends, the plain name replaces."""
    config = {"system": {"value": "x86_64-linux"}, "secret-key-files": {"value": ["/etc/a"]}}
    assert BuildSettings.from_nix_config(config, {}).secret_key_files == ("/etc/a",)
    assert BuildSettings.from_nix_config(config, {"extra-secret-key-files": "/b"}).secret_key_files == ("/etc/a", "/b")
    assert BuildSettings.from_nix_config(config, {"secret-key-files": "/c"}).secret_key_files == ("/c",)
