"""Shared constants and stash keys for conftest submodules."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests._conftest.nix_config import for_test_store

# ── Pyinstrument availability ─────────────────────────────────────

try:
    from pyinstrument import Profiler as Profiler
    from pyinstrument.renderers import ConsoleRenderer as ConsoleRenderer

    HAS_PYINSTRUMENT = True
except ImportError:
    Profiler = None  # type: ignore[assignment,misc]
    ConsoleRenderer = None  # type: ignore[assignment,misc]
    HAS_PYINSTRUMENT = False

# ── Pytest stash keys ─────────────────────────────────────────────

_log_dir_key = pytest.StashKey[Path]()

# ── Store paths ───────────────────────────────────────────────────

STORE_PREFIX = Path("/tmp/pynixd-stores")
SESSION_STORE_PREFIX = Path("/tmp/pynixd-session-stores")
HELLO = ("--file", "<nixpkgs>", "hello")
"""A real package with a real closure, as installables of `nix`.

`<nixpkgs>` and not `nixpkgs#hello`: the flake registry resolves that to
an unpinned channel, fetched over the network on every machine, so the
tests needed a network and built whatever the channel said that day. A
guest has no network, and the channel's `hello` sent it to build 572
derivations from the bootstrap seed. Issue #57.
"""
TMP_PREFIX = "/tmp/pynixd-test/"
"""Where each test's `tmp_path` goes: a directory of its own, so the sweep
after each test lists the suite's leftovers and not the host's `/tmp`.
Measured with 14,013 entries in `/tmp`: a glob of `/tmp/pynixd-test-*` after
each test cost 5.5 of 27 seconds. Still 17 characters, so `NAME_BUDGET`
does not move."""
# Anchored on this file, and not on the working directory. The suite ran
# from `pynixd/` alone until issue nixidae/nixidae#166, and `nix build --file tests/nix`
# then resolved against the checkout root and reported that the path does
# not exist. 30 tests failed that way, and none of them said why.
TEST_NIX = Path(__file__).resolve().parent.parent / "nix"

# ── Nix config ────────────────────────────────────────────────────

DEFAULT_SSH_OPTS = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

DEFAULT_NIX_CONFIG = for_test_store()
SESSION_NIX_CONFIG = for_test_store(
    experimental_features=(
        "nix-command",
        "flakes",
        "read-only-local-store",
        "ca-derivations",
        "dynamic-derivations",
        "recursive-nix",
    ),
)

# ── Feature probe fallback ────────────────────────────────────────

_NO_PROBE_FEATURE_MATRIX: dict[str, set[str]] = {
    "x86_64-linux": {
        "nixos-test",
        "benchmark",
        "big-parallel",
        "kvm",
        "ca-derivations",
        "recursive-nix",
    },
    "aarch64-linux": {
        "nixos-test",
        "benchmark",
        "big-parallel",
        "ca-derivations",
        "recursive-nix",
    },
}

# ── Session defaults ──────────────────────────────────────────────

_default_store_ids = {"local", "builder"}
SESSION_SSH_PORT = 0
SESSION_HTTP_PORT = 0
SESSION_HTTP_USER = "testuser"
SESSION_HTTP_PASS = "testpass"
