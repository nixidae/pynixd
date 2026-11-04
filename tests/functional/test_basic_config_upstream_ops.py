"""What a basic config still sends to the daemon (nixidae/nixidae#1155).

The basic config: one local store with `pynixd_writes` on and every answer
of `LocalDBStore.OWNED` on the `pynixd` route, and one `file://` cache that
pynixd stands for. The client names that cache as its substituter. The
workload is what a user of one machine does: substitute and build, query,
copy out, add, optimise, verify, collect.

`pynixd_upstream_ops_total` counts every op pynixd sends to the store's
daemon, from every site, so the set it holds after the workload is what
pynixd does not do yet. `UPSTREAM` declares that set with the reason for each
entry, and the test fails in both directions: an op that leaves the daemon,
and an op that reaches it again.
"""

from __future__ import annotations

import traceback
from collections import Counter
from typing import TYPE_CHECKING, Any

import pytest
from prometheus_client import REGISTRY

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.operations import STANDARD_OPERATIONS
from pynixd import Server
from pynixd.config import LocalBinaryCacheSpec
from pynixd.connection import Connection
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore, Store
from tests._conftest.import_lab import NIX_STORE
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from pathlib import Path

#: `Server` serves the store with this id; under another id it adds the machine's own.
STORE = "local"

DEP = (
    'derivation { name = "basic-dep"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "echo dep > $out" ]; }'
)
TOP = (
    f"let dep = {DEP}; in "
    'derivation { name = "basic-top"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "echo ${dep} > $out" ]; }'
)

UPSTREAM: dict[str, str] = {}
"""Each op the daemon still receives under the basic config, and why."""


async def run(*args: str) -> str:
    """Run a client; its stderr reaches the test log, so a failure says why."""
    _rc, stdout, _stderr, _both = await run_subproc(list(args))
    return stdout.strip()


@pytest.fixture
def sites(monkeypatch: pytest.MonkeyPatch) -> dict[str, Counter[str]]:
    """For each op sent to the store's daemon, the pynixd frames that sent it."""
    seen: dict[str, Counter[str]] = {}
    send = Connection.send_request

    async def recording(conn: Connection, request: Any) -> None:
        if conn.store_id == STORE:
            frames = [
                f"{frame.filename.rsplit('/pynixd/', 1)[-1]}:{frame.lineno} {frame.name}"
                for frame in traceback.extract_stack()[:-1]
                if "/pynixd/" in frame.filename and not frame.filename.endswith("connection.py")
            ]
            seen.setdefault(request.name, Counter())[" <- ".join(reversed(frames[-4:]))] += 1
        await send(conn, request)

    monkeypatch.setattr(Connection, "send_request", recording)
    return seen


def _counts(name: str) -> dict[str, float]:
    return {
        sample.labels["op"]: sample.value
        for metric in REGISTRY.collect()
        if metric.name == name.removesuffix("_total")
        for sample in metric.samples
        if sample.name == name and sample.labels.get("store_id") == STORE
    }


def _since(name: str, before: dict[str, float]) -> dict[str, float]:
    return {op: n - before.get(op, 0.0) for op, n in _counts(name).items() if n > before.get(op, 0.0)}


async def test_a_basic_config_sends_the_daemon_only_what_pynixd_does_not_do(
    tmp_path: Path, sites: dict[str, Counter[str]]
) -> None:
    root = tmp_path / "store"
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "nix-cache-info").write_text("StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 30\n")
    socket = tmp_path / "basic.sock"
    spec = make_test_spec(
        store_id=STORE,
        store_path=root,
        no_probe=True,
        nix_config=for_test_store(substituters=()),
        pynixd_writes=True,
        op_routes={op.name: OpRoute.pynixd for op in STANDARD_OPERATIONS if op.code in LocalDBStore.OWNED},
    )
    stores: dict[StoreId, Store] = {
        StoreId(STORE): LocalDBStore(spec),
        StoreId("cache"): LocalBinaryCacheSpec(store_id=StoreId("cache"), path=cache, trusted=True).to_store("cache"),
    }
    nix, url = str(NIX_BIN), f"unix://{socket}?root={root}"
    common = ("--store", url, "--option", "substituters", f"file://{cache}")
    before_sent = _counts("pynixd_upstream_ops_total")
    before_fell = _counts("pynixd_op_fallthrough_total")

    # Filled before pynixd first asks: a miss is remembered, as Nix's `narinfo-cache-negative-ttl`.
    other = str(tmp_path / "other")
    dep = await run(nix, "build", "--store", other, "--impure", "--no-link", "--print-out-paths", "--expr", DEP)
    await run(nix, "copy", "--store", other, "--to", f"file://{cache}", dep)

    async with Server(stores=stores, unix_path=socket, ssh_port=None, http_port=None):
        top = await run(
            nix,
            "build",
            *common,
            "--impure",
            "--out-link",
            str(tmp_path / "result"),
            "--print-out-paths",
            "--expr",
            TOP,
        )
        await run(nix, "path-info", *common, "--recursive", top)
        await run(NIX_STORE, "--store", url, "--query", "--referrers", dep)
        await run(nix, "copy", *common, "--to", f"file://{tmp_path / 'out'}", top)
        added = tmp_path / "added"
        added.write_text("added\n")
        await run(nix, "store", "add-file", *common, str(added))
        await run(nix, "store", "optimise", *common)
        await run(NIX_STORE, "--store", url, "--verify", "--check-contents")
        await run(nix, "store", "gc", *common)

    sent = _since("pynixd_upstream_ops_total", before_sent)
    fell = _since("pynixd_op_fallthrough_total", before_fell)
    assert "BuildDerivation" not in fell, f"pynixd's sandbox builds basic-top: {fell}"
    assert set(sent) == set(UPSTREAM), f"sent to the daemon: {sent}; fell through: {fell}; from: {sites}"
