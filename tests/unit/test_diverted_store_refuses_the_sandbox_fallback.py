"""A store Nix has diverted builds in a chroot, or it builds into the wrong store.

nix 2.34.8 `derivation-builder.cc:2111` forces the sandbox on when
`storeDir != realStoreDir`. Twelve lines later, at `:2120`, a system without
mount and PID namespaces turns it off again, with `debug()` and with
`sandbox-fallback` at its default of true. The builder then writes to
`/nix/store/<hash>-<name>`, which belongs to the machine, while Nix looks for
the output under the real directory of the diverted store.

A GitHub runner blocks unprivileged user namespaces, so the capability probe
answered `failed to produce output path` for `echo x86_64-linux > $out` there
and nowhere else. `builder`'s feature matrix emptied, and 39 tests failed with
`No compatible store for x86_64-linux` in CI run 35194109526. Issue #47.
"""

from __future__ import annotations

from pathlib import Path

from pynixd.store.local_daemon import _sandbox_fallback_arguments
from pynixd_store.store_layout import StoreLayout


def test_a_diverted_store_turns_the_fallback_off() -> None:
    layout = StoreLayout.chroot(Path("/tmp/pynixd-session-stores/builder"))
    assert layout.store_dir != layout.real_store_dir, "the shape under test"
    assert _sandbox_fallback_arguments(layout) == ["--option", "sandbox-fallback", "false"]


def test_the_store_of_the_machine_keeps_it() -> None:
    """The fallback is right where the builder writes to the store it reads.

    A machine with no namespaces still builds its own store without a chroot,
    so turning this off everywhere would refuse builds that work.
    """
    layout = StoreLayout.chroot(None)
    assert layout.store_dir == layout.real_store_dir, "the shape under test"
    assert _sandbox_fallback_arguments(layout) == []


def test_a_relocated_store_keeps_it() -> None:
    """`NIX_STORE_DIR` moves the store path itself, so nothing is diverted."""
    layout = StoreLayout.relocated_store(
        Path("/tmp/pynixd-relocated/store"),
        Path("/tmp/pynixd-relocated/var"),
    )
    assert _sandbox_fallback_arguments(layout) == []
