"""A build's output reaches a fresh store through the GitHub Actions cache, and nothing else does.

Two pynixd daemons with no Nix daemon under them, as on a runner, share the fake cache service:
the first builds and uploads, the second substitutes with `--max-jobs 0`. nixidae/nixidae#1377.
The service is pynixd's reading of the protocol; the installer workflow's `cache-read` job runs
the same against GitHub's.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.config import GhaCacheSpec
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from tests._conftest.gha_cache_fake import serve_fake_gha_cache
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from pathlib import Path

DEP = 'derivation { name = "gha-dep"; system = builtins.currentSystem; builder = "/bin/sh"; args = [ "-c" "echo dep > $out" ]; }'
TOP = (
    f"let dep = {DEP}; in "
    'derivation { name = "gha-top"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "echo ${dep} > $out" ]; }'
)


async def test_a_built_output_reaches_a_fresh_store_through_the_cache(tmp_path: Path) -> None:
    async with serve_fake_gha_cache() as fake:
        credentials = fake.credentials(tmp_path / "gha-cache.json")
        built: list[str] = []
        for name in ("writer", "reader"):
            socket = tmp_path / f"{name}.sock"
            spec = make_test_spec(
                store_id="local",
                store_path=tmp_path / name,
                no_probe=True,
                nix_config=for_test_store(substituters=()),
                op_routes={"BuildDerivation": OpRoute.pynixd},
            )
            cache = GhaCacheSpec(credentials_file=credentials).to_store("gha")
            stores = {StoreId("local"): LocalDBStore(spec), StoreId("gha"): cache}
            async with Server(stores=stores, unix_path=socket, ssh_port=None, http_port=None):
                build = [NIX_BIN, "build", "--store", f"unix://{socket}", "--impure", "--no-link", "--print-out-paths"]
                if name == "reader":
                    build += ["--max-jobs", "0"]
                _rc, out, _err, _both = await run_subproc([*build, "--expr", TOP])
                built.append(out.strip())
                counts = await cache.wait_for_uploads()
            if name == "writer":
                # Both outputs, each once: one entry per path, with its narinfo and NAR in it.
                assert (counts.uploaded, counts.failed) == (2, 0)
                assert fake.calls["CreateCacheEntry"] == 2
                assert len(fake.final_keys()) == 2
            else:
                assert counts.uploaded == 0, "a substituted path was uploaded again"
        assert built[0] == built[1]
        assert built[0]
