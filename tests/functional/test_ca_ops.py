"""Content-Addressed (CA) derivation operation tests."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
import structlog

from pynixd.store import LocalSocketStore
from tests._conftest.nix_config import for_ca_derivations, for_dynamic_derivations
from tests.conftest import (
    CLIENT_BIN,
    SESSION_STORE_PREFIX,
    STORE_PREFIX,
    TEST_NIX,
    make_test_spec,
    rmtree_robust,
    run_subproc,
    server_uri,
)

if TYPE_CHECKING:
    import pyinstrument

    from pynixd import Server

log = structlog.get_logger(__name__)


CA_NIX_CONFIG = for_ca_derivations(
    substituters=(
        "https://nixkube.cachix.org/",
        "unix:///nix/var/nix/daemon-socket/socket?root=/",
    ),
    trusted_public_keys=("nixkube.cachix.org-1:H8UE0jlI9pxHexK/NhDmEoLDarJXp1WTymQrsajlh7M=",),
)


@pytest.fixture
async def ca_env(pynixd_server: Server):
    """Set up a pynixd server with CA-derivations enabled."""
    return pynixd_server, server_uri(pynixd_server)


@pytest.mark.ca_derivations
async def test_ca_simple_build_root_store(
    profiler: pyinstrument.Profiler,
) -> None:
    """Build a CA floating derivation directly against the managed daemon (no pynixd)."""
    store_path = STORE_PREFIX / "ca-root-test"
    rmtree_robust(store_path)

    store = LocalSocketStore(
        make_test_spec(store_id="ca-root", store_path=store_path, nix_config=CA_NIX_CONFIG),
    )

    try:
        await store.ensure_daemon()

        cmd = [
            str(CLIENT_BIN),
            "build",
            "--store",
            str(store_path),
            "--impure",
            "--file",
            str(TEST_NIX),
            "ca.simple",
            "--no-link",
            "--print-out-paths",
        ]
        rc, stdout, _stderr, stdboth = await run_subproc(cmd, nix_config=CA_NIX_CONFIG)
        assert rc == 0, f"CA simple build failed:\n{stdboth}"
        out_path = stdout.strip()
        assert out_path.startswith("/nix/store/"), f"Unexpected output path: {out_path}"
        log.info("ca_simple_output", path=out_path)
    finally:
        await store.close()


@pytest.mark.ca_derivations
async def test_ca_multi_output_build_root_store(
    profiler: pyinstrument.Profiler,
) -> None:
    """Build a CA derivation with multiple outputs directly against the managed daemon."""
    store_path = STORE_PREFIX / "ca-root-multi"
    rmtree_robust(store_path)

    store = LocalSocketStore(
        make_test_spec(store_id="ca-root-multi", store_path=store_path, nix_config=CA_NIX_CONFIG),
    )

    try:
        await store.ensure_daemon()

        cmd = [
            str(CLIENT_BIN),
            "build",
            "--store",
            str(store_path),
            "--impure",
            "--file",
            str(TEST_NIX),
            "ca.multi_output",
            "--no-link",
            "--print-out-paths",
        ]
        rc, stdout, _stderr, stdboth = await run_subproc(cmd, nix_config=CA_NIX_CONFIG)
        assert rc == 0, f"CA multi-output build failed:\n{stdboth}"
        paths = stdout.strip().splitlines()
        assert len(paths) == 2, f"Expected 2 output paths, got {len(paths)}: {paths}"
        for p in paths:
            assert p.startswith("/nix/store/"), f"Unexpected output path: {p}"
        log.info("ca_multi_output_paths", paths=paths)
    finally:
        await store.close()


@pytest.mark.ca_derivations
async def test_ca_depends_on_ca_root_store(
    profiler: pyinstrument.Profiler,
) -> None:
    """Build a CA derivation that depends on another CA derivation."""
    store_path = STORE_PREFIX / "ca-root-depends"
    rmtree_robust(store_path)

    store = LocalSocketStore(
        make_test_spec(store_id="ca-root-depends", store_path=store_path, nix_config=CA_NIX_CONFIG),
    )

    try:
        await store.ensure_daemon()

        cmd = [
            str(CLIENT_BIN),
            "build",
            "--store",
            str(store_path),
            "--impure",
            "--file",
            str(TEST_NIX),
            "ca.depends_on_ca",
            "--no-link",
            "--print-out-paths",
        ]
        rc, stdout, _stderr, stdboth = await run_subproc(cmd, nix_config=CA_NIX_CONFIG)
        assert rc == 0, f"CA depends-on-CA build failed:\n{stdboth}"
        out_path = stdout.strip()
        assert out_path.startswith("/nix/store/"), f"Unexpected output path: {out_path}"
        log.info("ca_depends_on_ca_output", path=out_path)
    finally:
        await store.close()


@pytest.mark.ca_derivations
async def test_non_ca_depends_on_ca_root_store(
    profiler: pyinstrument.Profiler,
) -> None:
    """Build a non-CA (deferred) derivation that depends on a CA derivation."""
    store_path = STORE_PREFIX / "ca-root-non-ca-depends"
    rmtree_robust(store_path)

    store = LocalSocketStore(
        make_test_spec(store_id="ca-root-non-ca-depends", store_path=store_path, nix_config=CA_NIX_CONFIG),
    )

    try:
        await store.ensure_daemon()

        cmd = [
            str(CLIENT_BIN),
            "build",
            "--store",
            str(store_path),
            "--impure",
            "--file",
            str(TEST_NIX),
            "ca.non_ca_depends_on_ca",
            "--no-link",
            "--print-out-paths",
        ]
        rc, stdout, _stderr, stdboth = await run_subproc(cmd, nix_config=CA_NIX_CONFIG)
        assert rc == 0, f"Non-CA depends-on-CA build failed:\n{stdboth}"
        out_path = stdout.strip()
        assert out_path.startswith("/nix/store/"), f"Unexpected output path: {out_path}"
        log.info("non_ca_depends_on_ca_output", path=out_path)
    finally:
        await store.close()


@pytest.mark.ca_derivations
async def test_ca_simple_via_pynixd(profiler: pyinstrument.Profiler, ca_env) -> None:
    """Build a CA floating derivation through pynixd.

    Store operations triggered:
    - AddMultipleToStore: Adds multiple paths to store
    - BuildPaths: Builds derivation paths
    - BuildPathsWithResults: Builds derivation paths with results
    - QueryMissing: Queries missing paths
    - QueryValidPaths: Queries valid paths
    """
    _server, uri = ca_env

    cmd = [
        str(CLIENT_BIN),
        "build",
        "--eval-store",
        "auto",
        "--store",
        uri,
        "--impure",
        "--file",
        str(TEST_NIX),
        "ca.simple",
        "--no-link",
        "--print-out-paths",
    ]
    rc, stdout, stderr, stdboth = await run_subproc(
        cmd,
        nix_config=CA_NIX_CONFIG,
        expected_retcode=0,
    )
    log.info("ca_simple_via_pynixd", rc=rc, stdout=stdout, stderr=stderr)
    assert rc == 0, f"CA simple build via pynixd failed:\n{stdboth}"


@pytest.mark.ca_derivations
async def test_ca_multi_output_via_pynixd(
    profiler: pyinstrument.Profiler,
    ca_env,
) -> None:
    """Build a CA derivation with multiple outputs through pynixd.

    Store operations triggered:
    - AddMultipleToStore: Adds multiple paths to store
    - BuildPaths: Builds derivation paths
    - BuildPathsWithResults: Builds derivation paths with results
    - QueryMissing: Queries missing paths
    - QueryValidPaths: Queries valid paths
    """
    _server, uri = ca_env

    cmd = [
        str(CLIENT_BIN),
        "build",
        "--eval-store",
        "auto",
        "--store",
        uri,
        "--impure",
        "--file",
        str(TEST_NIX),
        "ca.multi_output",
        "--no-link",
        "--print-out-paths",
    ]
    rc, stdout, stderr, stdboth = await run_subproc(
        cmd,
        nix_config=CA_NIX_CONFIG,
        expected_retcode=0,
    )
    log.info("ca_multi_output_via_pynixd", rc=rc, stdout=stdout, stderr=stderr)
    assert rc == 0, f"CA multi-output build via pynixd failed:\n{stdboth}"
    paths = stdout.strip().splitlines()
    assert len(paths) == 2, f"Expected 2 outputs, got {len(paths)}: {paths}"


@pytest.mark.ca_derivations
async def test_ca_depends_on_ca_via_pynixd(
    profiler: pyinstrument.Profiler,
    ca_env,
) -> None:
    """Build a CA derivation that depends on another CA derivation through pynixd.

    Store operations triggered:
    - AddMultipleToStore: Adds multiple paths to store
    - BuildPaths: Builds derivation paths
    - BuildPathsWithResults: Builds derivation paths with results
    - QueryMissing: Queries missing paths
    - QueryValidPaths: Queries valid paths
    """
    _server, uri = ca_env

    cmd = [
        str(CLIENT_BIN),
        "build",
        "--eval-store",
        "auto",
        "--store",
        uri,
        "--impure",
        "--file",
        str(TEST_NIX),
        "ca.depends_on_ca",
        "--no-link",
        "--print-out-paths",
    ]
    rc, stdout, stderr, stdboth = await run_subproc(
        cmd,
        nix_config=CA_NIX_CONFIG,
        expected_retcode=0,
    )
    log.info("ca_depends_on_ca_via_pynixd", rc=rc, stdout=stdout, stderr=stderr)
    assert rc == 0, f"CA depends-on-CA build via pynixd failed:\n{stdboth}"
    assert stdout.strip().startswith("/nix/store/"), f"Unexpected output: {stdout}"


@pytest.mark.ca_derivations
async def test_non_ca_depends_on_ca_via_pynixd(
    profiler: pyinstrument.Profiler,
    ca_env,
) -> None:
    """Build a deferred (non-CA) derivation that depends on a CA derivation through pynixd.

    Store operations triggered:
    - AddMultipleToStore: Adds multiple paths to store
    - BuildPaths: Builds derivation paths
    - BuildPathsWithResults: Builds derivation paths with results
    - QueryMissing: Queries missing paths
    - QueryValidPaths: Queries valid paths
    """
    _server, uri = ca_env

    cmd = [
        str(CLIENT_BIN),
        "build",
        "--eval-store",
        "auto",
        "--store",
        uri,
        "--impure",
        "--file",
        str(TEST_NIX),
        "ca.non_ca_depends_on_ca",
        "--no-link",
        "--print-out-paths",
    ]
    rc, stdout, stderr, stdboth = await run_subproc(
        cmd,
        nix_config=CA_NIX_CONFIG,
        expected_retcode=0,
    )
    log.info("non_ca_depends_on_ca_via_pynixd", rc=rc, stdout=stdout, stderr=stderr)
    assert rc == 0, f"Non-CA depends-on-CA build via pynixd failed:\n{stdboth}"
    assert stdout.strip().startswith("/nix/store/"), f"Unexpected output: {stdout}"


@pytest.mark.ca_derivations
async def test_ca_query_derivation_output_map_via_pynixd(
    profiler: pyinstrument.Profiler,
    ca_env,
) -> None:
    """Build CA derivation through pynixd then query its output map.

    Store operations triggered:
    - AddMultipleToStore: Adds multiple paths to store
    - AddTempRoot: Adds temporary root
    - BuildPaths: Builds derivation paths
    - BuildPathsWithResults: Builds derivation paths with results
    - IsValidPath: Checks if path exists
    - NarFromPath: Gets NAR from path
    - QueryDerivationOutputMap: Queries derivation output map
    - QueryMissing: Queries missing paths
    - QueryPathInfo: Queries path info
    - QueryValidPaths: Queries valid paths
    """
    _server, uri = ca_env

    # Build the CA derivation first
    build_cmd = [
        str(CLIENT_BIN),
        "build",
        "--eval-store",
        "auto",
        "--store",
        uri,
        "--impure",
        "--file",
        str(TEST_NIX),
        "ca.simple",
        "--no-link",
        "--print-out-paths",
    ]
    rc, _stdout, _stderr, stdboth = await run_subproc(
        build_cmd,
        nix_config=CA_NIX_CONFIG,
        expected_retcode=0,
    )
    assert rc == 0, f"CA build via pynixd failed:\n{stdboth}"

    # Get the .drv path
    eval_cmd = [
        str(CLIENT_BIN),
        "eval",
        "--store",
        uri,
        "--impure",
        "--file",
        str(TEST_NIX),
        "ca.simple.drvPath",
        "--raw",
    ]
    rc, drv_out, _, _ = await run_subproc(eval_cmd, nix_config=CA_NIX_CONFIG)
    assert rc == 0, f"CA drvPath eval failed:\n{_}"
    drv_path = drv_out.strip()

    # Query the output map — exercises QueryDerivationOutputMap (op 41)
    info_cmd = [
        str(CLIENT_BIN),
        "path-info",
        "--store",
        uri,
        "--json",
        f"{drv_path}^*",
    ]
    rc, info_out, _, _ = await run_subproc(info_cmd, nix_config=CA_NIX_CONFIG)
    assert rc == 0, f"path-info failed:\n{_}"

    info = json.loads(info_out)
    assert len(info) > 0, f"No output paths found for {drv_path}"
    for data in info.values():
        assert "ca" in data, f"Expected 'ca' field in {data}"


@pytest.mark.ca_derivations
async def test_ca_query_derivation_output_map_root_store(
    profiler: pyinstrument.Profiler,
) -> None:
    """Build CA derivation then verify its output map via nix path-info."""
    store_path = STORE_PREFIX / "ca-root-qdom"
    rmtree_robust(store_path)

    store = LocalSocketStore(
        make_test_spec(store_id="ca-root-qdom", store_path=store_path, nix_config=CA_NIX_CONFIG),
    )

    await store.ensure_daemon()

    # Build the CA derivation
    cmd = [
        str(CLIENT_BIN),
        "build",
        "--store",
        str(store_path),
        "--impure",
        "--file",
        str(TEST_NIX),
        "ca.simple",
        "--no-link",
        "--print-out-paths",
    ]
    rc, stdout, stderr, stdboth = await run_subproc(cmd, nix_config=CA_NIX_CONFIG)
    assert rc == 0, f"CA build failed:\n{stdboth}"
    out_path = stdout.strip()
    assert out_path.startswith("/nix/store/")

    # Get the .drv path
    cmd = [
        str(CLIENT_BIN),
        "eval",
        "--store",
        str(store_path),
        "--impure",
        "--file",
        str(TEST_NIX),
        "ca.simple.drvPath",
        "--raw",
    ]
    rc, drv_out, stderr, stdboth = await run_subproc(cmd, nix_config=CA_NIX_CONFIG)
    assert rc == 0, f"CA drvPath eval failed:\n{stdboth}"
    drv_path = drv_out.strip()

    # Query the CA derivation's output map via nix path-info.
    # This exercises QueryDerivationOutputMap (op 41) and RegisterDrvOutput (op 42).
    cmd = [
        str(CLIENT_BIN),
        "path-info",
        "--store",
        str(store_path),
        "--json",
        f"{drv_path}^*",
    ]
    rc, stdout, stderr, stdboth = await run_subproc(cmd, nix_config=CA_NIX_CONFIG)
    assert rc == 0, f"path-info failed:\n{stderr}"

    info = json.loads(stdout)
    assert out_path in info, f"Expected {out_path} in {info}"
    assert "ca" in info[out_path], f"Expected 'ca' field in {info[out_path]}"


@pytest.mark.ca_derivations
async def test_dynamic_drv_trampoline(profiler: pyinstrument.Profiler, dyn_env) -> None:
    """Build a nested dynamic derivation (producingDrv^out^out) through pynixd.

    This exercises the trampoline: producingDrv's output is hello.drv,
    and the scheduler should detect this and automatically build the
    inner hello derivation. The final output should be hello's store
    path, not the intermediate .drv.

    Store operations triggered:
    - AddTempRoot: Adds temporary root
    - AddToStore: Adds to store
    - BuildPaths: Builds derivation paths
    - BuildPathsWithResults: Builds derivation paths with results
    - IsValidPath: Checks if path exists
    - NarFromPath: Gets NAR from path
    - QueryDerivationOutputMap: Queries derivation output map
    - QueryMissing: Queries missing paths
    - QueryPathInfo: Queries path info
    """
    _server, uri = dyn_env

    # First, get producingDrv's .drv path
    eval_cmd = [
        str(CLIENT_BIN),
        "eval",
        "--option",
        "builders",
        "",
        "--store",
        uri,
        "--impure",
        "--file",
        str(TEST_NIX),
        "dyn.producingDrv.drvPath",
        "--raw",
    ]
    rc, drv_out, _, _ = await run_subproc(eval_cmd, nix_config=DYN_NIX_CONFIG)
    assert rc == 0, "drvPath eval failed"
    drv_path = drv_out.strip()

    # Build producingDrv^out^out (nested: the out output of the
    # out output's .drv)
    build_cmd = [
        str(CLIENT_BIN),
        "build",
        "--option",
        "builders",
        "",
        "--store",
        uri,
        f"{drv_path}^out^out",
        "--no-link",
        "--print-out-paths",
    ]
    rc, stdout, _stderr, stdboth = await run_subproc(
        build_cmd,
        nix_config=DYN_NIX_CONFIG,
        expected_retcode=0,
    )
    assert rc == 0, f"Dynamic trampoline build failed:\n{stdboth}"
    out_path = stdout.strip()
    assert out_path.startswith("/nix/store/"), f"Unexpected output: {out_path}"
    assert not out_path.endswith(".drv"), f"Expected non-.drv output from trampoline, got: {out_path}"


DYN_NIX_CONFIG = for_dynamic_derivations(
    substituters=(
        "https://nixkube.cachix.org/",
        "unix:///nix/var/nix/daemon-socket/socket?root=/",
    ),
    trusted_public_keys=("nixkube.cachix.org-1:H8UE0jlI9pxHexK/NhDmEoLDarJXp1WTymQrsajlh7M=",),
)


@pytest.fixture
async def dyn_env(pynixd_server: Server):
    """Set up a pynixd server with dynamic-derivations enabled."""
    return pynixd_server, server_uri(pynixd_server)


@pytest.mark.ca_derivations
async def test_dynamic_drv_producing_via_pynixd(
    profiler: pyinstrument.Profiler,
    dyn_env,
) -> None:
    """Build producingDrv (text-hashed CA whose output IS a .drv) through pynixd.

    producingDrv is a text-hashed CA derivation: its outputHashMode is "text"
    and the output content IS a .drv file. This tests that pynixd correctly
    handles the full lifecycle: build, realisation registration, and
    QueryDerivationOutputMap for text-hashed CA derivations.

    Store operations triggered:
    - AddTempRoot: Adds temporary root
    - AddToStore: Adds to store
    - BuildPaths: Builds derivation paths
    - BuildPathsWithResults: Builds derivation paths with results
    - IsValidPath: Checks if path exists
    - NarFromPath: Gets NAR from path
    - QueryDerivationOutputMap: Queries derivation output map
    - QueryMissing: Queries missing paths
    - QueryPathInfo: Queries path info
    """
    _server, uri = dyn_env

    # Build producingDrv + hello together (both needed for producingDrv's input_srcs)
    build_cmd = [
        str(CLIENT_BIN),
        "build",
        "--option",
        "builders",
        "",
        "--store",
        uri,
        "--impure",
        "--file",
        str(TEST_NIX),
        "dyn.producingDrv",
        "--no-link",
        "--print-out-paths",
    ]
    rc, stdout, _stderr, stdboth = await run_subproc(
        build_cmd,
        nix_config=DYN_NIX_CONFIG,
        expected_retcode=0,
    )
    assert rc == 0, f"producingDrv build via pynixd failed:\n{stdboth}"
    producing_out = stdout.strip()
    assert producing_out.startswith("/nix/store/"), f"Unexpected output path: {producing_out}"

    # The output of producingDrv IS a .drv file
    assert producing_out.endswith(".drv"), f"Expected .drv output, got: {producing_out}"

    # Verify it's parseable as a derivation
    pynixd_local_path = SESSION_STORE_PREFIX / "local"
    full_path = pynixd_local_path / producing_out.lstrip("/")
    if full_path.exists():
        content = full_path.read_text()
        assert content.startswith("Derive("), f"Output should be derivation ATerm, got: {content[:80]}"

    # Query the derivation output map to verify realisation registration
    eval_cmd = [
        str(CLIENT_BIN),
        "eval",
        "--store",
        uri,
        "--impure",
        "--file",
        str(TEST_NIX),
        "dyn.producingDrv.drvPath",
        "--raw",
    ]
    rc, drv_out, _, _ = await run_subproc(eval_cmd, nix_config=DYN_NIX_CONFIG)
    assert rc == 0, "producingDrv drvPath eval failed"
    drv_path = drv_out.strip()

    info_cmd = [
        str(CLIENT_BIN),
        "path-info",
        "--store",
        uri,
        "--json",
        f"{drv_path}^*",
    ]
    rc, info_out, _, _ = await run_subproc(info_cmd, nix_config=DYN_NIX_CONFIG)
    assert rc == 0, "path-info for producingDrv failed"

    info = json.loads(info_out)
    assert producing_out in info, f"Expected {producing_out} in output map, got: {list(info.keys())}"

    log.info("producingDrv_via_pynixd", output=producing_out)


@pytest.mark.ca_derivations
async def test_text_hashed_ca_build_root_store(
    profiler: pyinstrument.Profiler,
) -> None:
    """Build a text-hashed CA derivation (outputHashMode=text) directly."""
    store_path = STORE_PREFIX / "ca-root-text"
    rmtree_robust(store_path)

    store = LocalSocketStore(
        make_test_spec(store_id="ca-root-text", store_path=store_path, nix_config=CA_NIX_CONFIG),
    )

    try:
        await store.ensure_daemon()

        cmd = [
            str(CLIENT_BIN),
            "build",
            "--store",
            str(store_path),
            "--impure",
            "--file",
            str(TEST_NIX),
            "ca.text_hashed",
            "--no-link",
            "--print-out-paths",
        ]
        rc, stdout, _stderr, stdboth = await run_subproc(cmd, nix_config=CA_NIX_CONFIG)
        assert rc == 0, f"Text-hashed CA build failed:\n{stdboth}"
        out_path = stdout.strip()
        assert out_path.startswith("/nix/store/"), f"Unexpected output path: {out_path}"
        log.info("text_hashed_ca_output", path=out_path)

        full_path = store_path / out_path.lstrip("/")
        assert full_path.exists(), f"Output file missing: {full_path}"
        content = full_path.read_text().strip()
        assert content.startswith("text-content-"), f"Unexpected content: {content!r}"
    finally:
        await store.close()


@pytest.mark.ca_derivations
async def test_text_hashed_ca_build_via_pynixd(
    profiler: pyinstrument.Profiler,
    ca_env,
) -> None:
    """Build a text-hashed CA derivation through pynixd proxy.

    Store operations triggered:
    - AddTempRoot: Adds temporary root
    - AddToStore: Adds to store
    - BuildPaths: Builds derivation paths
    - BuildPathsWithResults: Builds derivation paths with results
    - IsValidPath: Checks if path exists
    - NarFromPath: Gets NAR from path
    - QueryDerivationOutputMap: Queries derivation output map
    - QueryMissing: Queries missing paths
    - QueryPathInfo: Queries path info
    """
    _server, uri = ca_env

    build_cmd = [
        str(CLIENT_BIN),
        "build",
        "--option",
        "builders",
        "",
        "--store",
        uri,
        "--impure",
        "--file",
        str(TEST_NIX),
        "ca.text_hashed",
        "--no-link",
        "--print-out-paths",
    ]
    rc, stdout, _stderr, stdboth = await run_subproc(
        build_cmd,
        nix_config=CA_NIX_CONFIG,
        expected_retcode=0,
    )
    assert rc == 0, f"Text-hashed CA build via pynixd failed:\n{stdboth}"
    out_path = stdout.strip()
    assert out_path.startswith("/nix/store/"), f"Unexpected output path: {out_path}"
    log.info("text_hashed_ca_via_pynixd", path=out_path)

    # Get the .drv path
    eval_cmd = [
        str(CLIENT_BIN),
        "eval",
        "--store",
        uri,
        "--impure",
        "--file",
        str(TEST_NIX),
        "ca.text_hashed.drvPath",
        "--raw",
    ]
    rc, drv_out, _, _ = await run_subproc(eval_cmd, nix_config=CA_NIX_CONFIG)
    assert rc == 0, "CA drvPath eval failed"
    drv_path = drv_out.strip()

    # Query the output map — exercises QueryDerivationOutputMap for text-hashed CA
    info_cmd = [
        str(CLIENT_BIN),
        "path-info",
        "--store",
        uri,
        "--json",
        f"{drv_path}^*",
    ]
    rc, info_out, _, _ = await run_subproc(info_cmd, nix_config=CA_NIX_CONFIG)
    assert rc == 0, "path-info failed"

    info = json.loads(info_out)
    assert len(info) > 0, f"No output paths found for {drv_path}"
    assert out_path in info, f"Expected {out_path} in {info}"
    assert "ca" in info[out_path], f"Expected 'ca' field in {info[out_path]}"


@pytest.mark.ca_derivations
async def test_dynamic_drv_wrapper_via_pynixd(
    profiler: pyinstrument.Profiler,
    dyn_env,
) -> None:
    """Build the wrapper derivation (depends on dynamic drv^out^out) through pynixd.

    The wrapper derivation uses DrvWithVersion format with dynamic_input_drvs.
    Its env contains a DownstreamPlaceholder that must be resolved to the
    actual output path after the inner build completes.

    Store operations triggered:
    - BuildPaths: Builds wrapper + producingDrv + hello chain
    - All operations from test_dynamic_drv_trampoline
    - Derivation resolution for DownstreamPlaceholder replacement
    - AddToStore: Resolved wrapper .drv written to both stores
    """
    _server, uri = dyn_env
    build_cmd = [
        str(CLIENT_BIN),
        "build",
        "--option",
        "builders",
        "",
        "--store",
        uri,
        "--impure",
        "--file",
        str(TEST_NIX),
        "dyn.wrapper",
        "--no-link",
        "--print-out-paths",
    ]
    rc, stdout, _stderr, stdboth = await run_subproc(
        build_cmd,
        nix_config=DYN_NIX_CONFIG,
        expected_retcode=0,
    )
    assert rc == 0, f"Wrapper build via pynixd failed:\n{stdboth}"
    out_path = stdout.strip()
    assert out_path.startswith("/nix/store/"), f"Unexpected output: {out_path}"
    assert not out_path.endswith(".drv"), f"Wrapper output should not be a .drv, got: {out_path}"

    log.info("wrapper_via_pynixd", path=out_path)


@pytest.mark.ca_derivations
async def test_deferred_dynamic_producer_via_pynixd(dyn_env) -> None:
    """Issue #9: build a deferred producer before resolving its consumer."""
    _, uri = dyn_env
    rc, stdout, _, stdboth = await run_subproc(
        [
            str(CLIENT_BIN),
            "build",
            "--option",
            "builders",
            "",
            "--store",
            uri,
            "--impure",
            "--file",
            str(TEST_NIX),
            "dyn.deferredWrapper",
            "--no-link",
            "--print-out-paths",
        ],
        nix_config=DYN_NIX_CONFIG,
        expected_retcode=0,
    )
    assert rc == 0, stdboth
    rc, content, _, stdboth = await run_subproc(
        [str(CLIENT_BIN), "store", "cat", "--store", uri, stdout.strip()],
        nix_config=DYN_NIX_CONFIG,
    )
    assert rc == 0, stdboth
    assert content == "hello\nhello\n"


@pytest.mark.ca_derivations
async def test_deep_dynamic_drv_chain_via_pynixd(
    profiler: pyinstrument.Profiler,
    dyn_env,
) -> None:
    """The 5-deep dynamic derivation chain (deepWrapper) fails through pynixd, as through Nix.

    deepWrapper consumes producer!out!out!out!out!out, five levels of
    SingleDerivedPath::Built nesting, where test_dynamic_drv_wrapper_via_pynixd
    covers two. Each level peels one childMap layer and rebuilds the remainder,
    so a passing build proves recursive resolution, unparsing and rebuilding
    at depth. Issue #10.
    """
    _server, uri = dyn_env
    build_cmd = [
        str(CLIENT_BIN),
        "build",
        "--option",
        "builders",
        "",
        "--store",
        uri,
        "--impure",
        "--file",
        str(TEST_NIX),
        "dyn.deepWrapper",
        "--no-link",
        "--print-out-paths",
    ]
    # `producer^out` is `target.drv`, whose `out` is a plain file, so the
    # next `^out` asks a file for a derivation. Measured with the stock
    # nix-daemon 2.35.3: the build fails with this message.
    # nixidae/nixidae#1005.
    rc, _stdout, _stderr, stdboth = await run_subproc(
        build_cmd,
        nix_config=DYN_NIX_CONFIG,
        expected_retcode=1,
    )
    assert rc == 1, f"Deep dynamic chain past a plain output built via pynixd:\n{stdboth}"
    assert "is not a valid derivation path" in stdboth, stdboth
