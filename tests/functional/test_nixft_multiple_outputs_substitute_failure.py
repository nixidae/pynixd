"""Nix's `multiple-outputs-substitute-failure.sh`, which skips on `needLocalStore`, held to three arms (`_nixft.py`).

The script needs `--no-require-sigs`, which only a store the client opens itself takes. Through pynixd,
with nix-daemon and with none, `require-sigs` is off in the store's own configuration
(`for_test_store`), as the client cannot turn it off there.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import anyio
import pytest

from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, run_subproc
from tests.functional._nixft import Arm, arm_store, clear_store, file_url
from tests.functional.test_build_without_sandbox import _keep_store_dirs

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd.nix_config import NixConfig

__all__ = ["_keep_store_dirs"]


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


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_corrupt_output_fails_its_substitution(tmp_path: Path, arm: Arm) -> None:
    """`multiple-outputs-substitute-failure.sh:12`, `:27`: one output with a wrong `NarHash` fails `^*` with "hash mismatch"."""
    builds = tmp_path / "multiple-outputs.nix"
    builds.write_text(BUILDS)
    cache = tmp_path / "cache"
    async with arm_store(tmp_path, Arm.local, _config()) as store:
        _rc, out, _err, _both = await run_subproc(
            [
                NIX_BIN,
                "build",
                "--store",
                store.url,
                "--file",
                builds,
                "independent^*",
                "--no-link",
                "--print-out-paths",
            ],
            env=dict(store.env),
            nix_config=store.config,
        )
        outputs = out.split()
        assert len(outputs) == 2, out
        await run_subproc(
            [NIX_BIN, "copy", "--store", store.url, "--to", file_url(cache), *outputs],
            env=dict(store.env),
            nix_config=store.config,
        )
    clear_store(tmp_path)
    (second,) = (p for p in outputs if p.endswith("-second"))
    narinfo = cache / f"{second.rsplit('/', 1)[1][:32]}.narinfo"
    narinfo.write_text(
        re.sub(r"^NarHash:.*$", "NarHash: sha256:" + "0" * 52, narinfo.read_text(), count=1, flags=re.MULTILINE)
    )

    sigs = ["--no-require-sigs"] if arm is Arm.local else []
    async with arm_store(tmp_path, arm, _config()) as store:
        with anyio.fail_after(60):
            got, _out, err, _both = await run_subproc(
                [
                    *(NIX_BIN, "build", "--store", store.url, "-j", "0", "--file", builds, "independent^*"),
                    *("--no-link", "--substituters", file_url(cache), *sigs, "--substitute"),
                ],
                expected_retcode=None,
                env=dict(store.env),
                nix_config=store.config,
            )
    assert (got, "hash mismatch" in err) == (1, True), err
