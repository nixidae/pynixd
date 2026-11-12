"""Nix's `multiple-outputs-substitute-failure.sh`, which skips on `needLocalStore`, held to three arms.

The script needs `--no-require-sigs`, which only a store the client opens itself takes, so that arm is
the reference. Through pynixd, with nix-daemon and with none, `require-sigs` is off in the store's own
configuration (`for_test_store`), as the client cannot turn it off there.
"""

from __future__ import annotations

import contextlib
import re
from enum import StrEnum
from typing import TYPE_CHECKING

import anyio
import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.store import LocalDBStore
from tests._conftest.helpers import rmtree_robust
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc
from tests.functional.test_build_without_sandbox import _keep_store_dirs

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from pynixd.nix_config import NixConfig

__all__ = ["_keep_store_dirs"]


class Arm(StrEnum):
    local = "local"
    daemon = "daemon"
    pynixd = "pynixd"


# `multiple-outputs.nix`'s `independent`, with the builder's shell alone: each output is a file.
BUILDS = """{
  independent = derivation {
    name = "multiple-outputs-independent";
    system = builtins.currentSystem;
    builder = "/bin/sh";
    outputs = [ "first" "second" ];
    args = [ "-c" "echo first > $first; echo second > $second" ];
  };
}"""


def _config() -> NixConfig:
    config = for_test_store(substituters=())
    config.sandbox = "false"
    return config


@contextlib.asynccontextmanager
async def _store(tmp_path: Path, arm: Arm) -> AsyncIterator[tuple[str, dict[str, str], NixConfig | dict[str, str]]]:
    """A fresh store for *arm* in `tmp_path/store`: the `--store` value, the client's environment and its config."""
    store, state, logs = tmp_path / "store", tmp_path / "var/nix", tmp_path / "var/log/nix"
    env = {"NIX_STORE_DIR": str(store), "NIX_STATE_DIR": str(state), "NIX_LOG_DIR": str(logs)}
    if arm is Arm.local:
        yield "local", env, _config()
        return
    socket = tmp_path / f"{arm}.sock"
    spec = make_test_spec(
        store_id="local",
        store_path=tmp_path / arm,
        no_probe=True,
        nix_config=_config(),
        daemon=arm is Arm.daemon,
        store_dir=store,
        state_dir=state,
        log_dir=logs,
    )
    env["NIX_STATE_DIR"] = str(tmp_path / "client")
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None):
        yield f"unix://{socket}", env, {"substituters": ""}


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_corrupt_output_fails_its_substitution(tmp_path: Path, arm: Arm) -> None:
    """`multiple-outputs-substitute-failure.sh:12`, `:27`: one output with a wrong `NarHash` fails `^*` with "hash mismatch"."""
    builds = tmp_path / "multiple-outputs.nix"
    builds.write_text(BUILDS)
    cache = tmp_path / "cache"
    async with _store(tmp_path, Arm.local) as (url, env, config):
        _rc, out, _err, _both = await run_subproc(
            [NIX_BIN, "build", "--store", url, "--file", builds, "independent^*", "--no-link", "--print-out-paths"],
            env=dict(env),
            nix_config=config,
        )
        outputs = out.split()
        assert len(outputs) == 2, out
        await run_subproc(
            [NIX_BIN, "copy", "--store", url, "--to", f"file://{cache}", *outputs], env=dict(env), nix_config=config
        )
    # `clearStore`: the arm starts from an empty store in the same store directory.
    for directory in ("store", "var"):
        rmtree_robust(tmp_path / directory)
    (second,) = (p for p in outputs if p.endswith("-second"))
    narinfo = cache / f"{second.rsplit('/', 1)[1][:32]}.narinfo"
    narinfo.write_text(
        re.sub(r"^NarHash:.*$", "NarHash: sha256:" + "0" * 52, narinfo.read_text(), count=1, flags=re.MULTILINE)
    )

    sigs = ["--no-require-sigs"] if arm is Arm.local else []
    async with _store(tmp_path, arm) as (url, env, config):
        with anyio.fail_after(60):
            got, _out, err, _both = await run_subproc(
                [
                    NIX_BIN,
                    "build",
                    "--store",
                    url,
                    "-j",
                    "0",
                    "--file",
                    builds,
                    "independent^*",
                    "--no-link",
                    "--substituters",
                    f"file://{cache}",
                    *sigs,
                    "--substitute",
                ],
                expected_retcode=None,
                env=dict(env),
                nix_config=config,
            )
    assert (got, "hash mismatch" in err) == (1, True), err
