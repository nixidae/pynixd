"""pynixd builds on darwin as Nix's `DarwinDerivationBuilder` does, with no Nix daemon (nixidae/nixidae#1367).

Each case builds one derivation twice: on the daemon route, which is Nix's own darwin builder, and on a store
with no daemon at all. darwin builds no `?root=` store (`derivation-builder.cc:1785`), so each store is a
`NIX_STORE_DIR` store, and the client names the same directory. The suites run in Linux guests; this file runs
on a darwin host only.
"""

from __future__ import annotations

import contextlib
import json
import platform
import sqlite3
import sys
from typing import TYPE_CHECKING, Any

import pytest

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.store_dir import real_store_dir, set_real_store_dir, set_store_dir, store_dir
from pynixd import Server
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from tests._conftest.helpers import rmtree_robust
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator
    from pathlib import Path

pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="Nix's darwin builder is the reference")

SYSTEM = "aarch64-darwin" if platform.machine() == "arm64" else "x86_64-darwin"

#: The client's default substituters name the machine's daemon, which refuses this store's paths
#: (nixidae/nixidae#1193).
ALONE = {"substituters": ""}

#: What only the full profile refuses: a host file outside `sandbox-paths`.
PROBE = """echo start
if read l < /private/etc/passwd; then echo host=read; else echo host=refused; fi
echo built > $out"""


@pytest.fixture(autouse=True)
def _keep_store_dirs() -> Iterator[None]:
    """A server sets the process's store directories (`instance.py:113`); the session's server needs its own back."""
    saved, saved_real = store_dir(), real_store_dir()
    yield
    set_store_dir(saved)
    set_real_store_dir(saved_real)


def _drv(name: str, body: str, extra: str = "") -> str:
    return (
        f'derivation {{ name = "{name}"; system = builtins.currentSystem; builder = "/bin/sh"; {extra} '
        f"args = [ (builtins.toFile \"{name}.sh\" ''\n{body}\n'') ]; }}"
    )


@contextlib.asynccontextmanager
async def _serve(tmp_path: Path, daemon: bool, sandbox: str) -> AsyncIterator[tuple[list[str], dict[str, str], Path]]:
    """A fresh `NIX_STORE_DIR` store: Nix's builder with *daemon*, pynixd's with no daemon at all.

    One directory for both, so every store path is the same; each store replaces the other's.
    """
    # The kernel matches the profile against real paths, and `/tmp` is `/private/tmp`.
    base = tmp_path.resolve() / sandbox
    rmtree_robust(base)
    store, state, logs = base / "store", base / "var/nix", base / "var/log/nix"
    path = tmp_path / f"{'daemon' if daemon else 'pynixd'}-{sandbox}.sock"
    nix_config = for_test_store(substituters=())
    nix_config.sandbox = sandbox
    spec = make_test_spec(
        store_id="local",
        store_path=base,
        no_probe=True,
        # The probe-less default names Linux systems.
        feature_matrix={SYSTEM: set()},
        nix_config=nix_config,
        # With no daemon, `make_test_spec` routes every answer on trial to pynixd.
        op_routes={"BuildDerivation": OpRoute.daemon if daemon else OpRoute.pynixd},
        daemon=daemon,
        store_dir=store,
        state_dir=state,
        log_dir=logs,
        # `<state>/daemon-socket/pynixd-nix` under a test's directory passes darwin's 104 bytes.
        socket_path=tmp_path / f"{sandbox}.nix",
    )
    env = {"NIX_STORE_DIR": str(store), "NIX_STATE_DIR": str(base / "client"), "NIX_LOG_DIR": str(logs)}
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=path, ssh_port=None, http_port=None):
        yield [str(NIX_BIN), "--store", f"unix://{path}", "--impure"], env, state


def _rows(state: Path) -> tuple[list[tuple[Any, ...]], list[tuple[str, str]]]:
    db = sqlite3.connect(f"file:{state}/db/db.sqlite?mode=ro", uri=True)
    try:
        valid = db.execute(
            "SELECT path, hash, narSize, deriver, ultimate, sigs, ca FROM ValidPaths ORDER BY path"
        ).fetchall()
        refs = db.execute(
            "SELECT a.path, b.path FROM Refs JOIN ValidPaths a ON a.id = referrer "
            "JOIN ValidPaths b ON b.id = reference ORDER BY a.path, b.path"
        ).fetchall()
    finally:
        db.close()
    return valid, refs


async def _build(tmp_path: Path, daemon: bool, sandbox: str, expr: str, *, expected: int = 0) -> dict[str, Any]:
    async with _serve(tmp_path, daemon, sandbox) as (nix, env, state):
        rc, out, err, _both = await run_subproc(
            [*nix[:1], "build", *nix[1:], "--no-link", "--print-out-paths", "--print-build-logs", "--expr", expr],
            expected_retcode=expected,
            env=dict(env),
            nix_config=ALONE,
        )
        seen: dict[str, Any] = {"rc": rc, "log": [line for line in err.splitlines() if line.startswith("darwin-")]}
        if rc == 0:
            path = out.strip()
            _rc, info, _err, _both = await run_subproc(
                [*nix[:1], "path-info", *nix[1:], "--json", path], env=dict(env), nix_config=ALONE
            )
            info_json = json.loads(info)
            for value in info_json.values() if isinstance(info_json, dict) else info_json:
                value.pop("registrationTime", None)
            seen["info"] = info_json
            seen["contents"] = (state.parent.parent / "store" / path.rsplit("/", 1)[-1]).read_text()
        else:
            seen["error"] = err[err.index("error:") :].replace(str(state.parent.parent), "<base>")
    seen["rows"] = _rows(state)
    return seen


@pytest.mark.parametrize(("sandbox", "host"), [("true", "refused"), ("false", "read")])
async def test_a_build_with_no_daemon_matches_nixs_darwin_builder(tmp_path: Path, sandbox: str, host: str) -> None:
    """The full profile refuses a host file, the minimal one does not; the output registers the same."""
    expr = _drv("darwin-probe", PROBE)
    here = await _build(tmp_path, False, sandbox, expr)
    there = await _build(tmp_path, True, sandbox, expr)
    assert there["log"][-1] == f"darwin-probe> host={host}", there["log"]
    assert here["log"] == there["log"]
    assert here["contents"] == there["contents"] == "built\n"
    assert here["info"] == there["info"]
    assert here["rows"] == there["rows"]


async def test_an_impure_host_dependency_outside_the_allowed_prefixes_fails_in_nix_words(tmp_path: Path) -> None:
    """`getPathsInSandbox` (`derivation-builder.cc:536`) throws with or without the sandbox."""
    expr = _drv("darwin-impure", "echo built > $out", '__impureHostDeps = [ "/usr/libexec" ];')
    here = await _build(tmp_path, False, "relaxed", expr, expected=1)
    there = await _build(tmp_path, True, "relaxed", expr, expected=1)
    assert "but it was not in allowed-impure-host-deps" in there["error"], there["error"]
    assert here["error"] == there["error"]


async def test_a_sandbox_profile_under_sandbox_true_is_refused_in_nix_words(tmp_path: Path) -> None:
    """`makeDerivationBuilder` (`derivation-builder.cc:1765`): only `relaxed` takes a derivation's profile."""
    expr = _drv("darwin-profile", "echo built > $out", '__sandboxProfile = "(allow default)";')
    here = await _build(tmp_path, False, "true", expr, expected=1)
    there = await _build(tmp_path, True, "true", expr, expected=1)
    assert "specifies a sandbox profile" in there["error"], there["error"]
    assert here["error"] == there["error"]
