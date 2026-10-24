"""Build each corpus case twice, and compare the two stores.

One run goes through `pynixd.goals`. The other goes through the goal system of
Nix, which huggorm calls in process. The two runs use two separated chroot
stores, and the test compares what each build added to its own store.

Three rules shape the design.

**Instantiate identically, build differently.** Both arms get their `.drv`
from the same `nix-instantiate` invocation shape, against their own store.
Evaluation is deterministic, so the two `.drv` paths must agree, and the test
states that before it builds anything. It keeps the build as the only
difference between the arms.

**Compare the delta, not the store.** One arm is driven by a real
`nix-daemon` and the other by an in-process `LocalStore`. Each seeds its own
store in its own way, and neither way says anything about a goal system.
Comparing what the build *added* removes that from the question.

**Run the arms one at a time.** huggorm reads both stores directly, and arm
A's store belongs to a running `nix-daemon` while the server is up. Every
direct read of that store happens before the server starts or after it stops.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, cast

import anyio
import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.goals.engine import GoalEngine
from pynixd.goals.results import result_succeeded
from pynixd.instance import Server
from pynixd.serde import (
    BuildMode,
    BuildPathsWithResultsRequest,
    DerivedPath as SerdeDerivedPath,
)
from pynixd.store.local_daemon import LocalStore
from pynixd.store.local_db import LocalDBStore
from tests._conftest.config import make_test_spec
from tests.differential.conftest import DifferentialRoots
from tests.differential.corpus import CA_CORPUS, CORPUS, Case
from tests.differential.snapshot import StoreSnapshot, compare, delta, take_snapshot

# huggorm is a sibling source, and the runner of this suite puts it on the
# path. A checkout of pynixd alone does not have it.
#
# `importorskip`, and not a plain import. The conftest of this directory skips
# every test here when huggorm is absent, and it never got the chance: pytest
# imports a module to collect it, so the absent package ended the run with a
# collection error before any gate spoke.
#
# Last among the imports, so that one `noqa` covers the rule rather than six.
huggorm = pytest.importorskip("huggorm", reason="huggorm is the oracle of this suite and is not installed")

if TYPE_CHECKING:
    from collections.abc import Collection
    from pathlib import Path

# The settings both arms instantiate and build under. They are the settings
# that make an unprivileged chroot store work at all: no build users, so Nix
# builds as the calling user, and no substituters, so a missing path is a
# build in both arms rather than a fetch in one of them.
#
# That second one is not a detail. `ultimate` in a snapshot separates a path a
# store built from a path it fetched, and a corpus that allowed substitution
# would make the two arms disagree on it for a reason that is about timing
# rather than about either engine.
_BASE_NIX_CONFIG = {
    "build-users-group": "",
    "substituters": "",
    "require-sigs": "false",
}


def _nix_config_env(case: Case) -> dict[str, str]:
    settings = dict(_BASE_NIX_CONFIG)
    if case.experimental_features:
        settings["extra-experimental-features"] = " ".join(case.experimental_features)
    return {
        **os.environ,
        "NIX_CONFIG": "\n".join(f"{key} = {value}" for key, value in settings.items()),
    }


async def _instantiate(root: Path, case: Case) -> str:
    """Write the `.drv` of *case* into the chroot store at *root*, and name it."""
    process = await anyio.run_process(
        [
            "nix-instantiate",
            "--store",
            f"local://?root={root}",
            "--expr",
            case.expression,
        ],
        env=_nix_config_env(case),
        check=False,
    )
    if process.returncode != 0:
        message = process.stderr.decode(errors="replace")
        raise AssertionError(f"instantiating {case.name} into {root} failed:\n{message}")
    return process.stdout.decode().strip().splitlines()[-1].split("!")[0]


def _apply_oracle_settings(case: Case | None) -> None:
    """Give the in-process Nix the settings `_nix_config_env` gives a `nix` process.

    The settings are process-wide, so they are set before each store opens: a
    store reads its configuration when it opens. `nix.conf` comes first, as it
    does for the `nix-daemon` of the other arm.
    """
    huggorm.load_config()
    for name, value in _BASE_NIX_CONFIG.items():
        huggorm.set_setting(name, value)
    for feature in case.experimental_features if case is not None else ():
        huggorm.enable_experimental_feature(feature)


async def _read_store(root: Path) -> StoreSnapshot:
    """Snapshot the chroot store at *root*, through huggorm."""
    _apply_oracle_settings(None)
    async with huggorm.AsyncSession(f"local://?root={root}") as session:
        return await take_snapshot(session.store())


async def _build_with_pynixd(root: Path, drv_path: str, case: Case) -> Any:
    """Realise every output of *drv_path* through pynixd's goal engine."""
    # `no_probe`, because pynixd otherwise builds a `probe-system-*` and a
    # `probe-feature-*` derivation for each system and feature while the server
    # starts, and those land in this arm's store and in no other. The first run
    # of this suite reported eight of them as paths "only in pynixd".
    #
    # They are infrastructure of the proxy and not output of a goal system, so
    # leaving them out is the answer rather than filtering them afterwards. The
    # static matrix that replaces them covers aarch64-linux and x86_64-linux.
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        extra_env=_nix_config_env(case),
        no_probe=True,
    )
    async with Server(
        stores={StoreId("local"): LocalDBStore(spec)},
        ssh_port=0,
        http_port=0,
        unix_path=root.parent / "pynixd.sock",
    ) as server:
        engine = GoalEngine(server.ctx)
        # `!` and not `^`: the request carries the wire spelling of a derived
        # path, and `parse_derived_path_legacy` is what reads it.
        request = BuildPathsWithResultsRequest(
            derived_paths=cast("Any", {SerdeDerivedPath(value=f"{drv_path}!*")}),
            build_mode=BuildMode.NORMAL,
        )
        return await engine.build_paths_with_results(request)


async def _build_with_pynixd_fleet(
    local_root: Path,
    builder_root: Path,
    drv_path: str,
    case: Case,
    builder_class: type[LocalStore] = LocalDBStore,
) -> Any:
    """Realise *drv_path* through pynixd, with the build placed on a second store.

    This is the shape pynixd exists for. The `local` store is the one a client
    talks to, and `no_schedule` keeps every build off it, so the scheduler has
    to place the work on `builder`. The outputs are then somebody's job to
    bring back, and whose job that is, and whether the result matches what Nix
    would have produced, is the question this arm asks.

    `builder_class` chooses what kind of backend the fleet holds, and the
    choice decides which reassembly path runs.
    `Scheduler._direct_import_localdb_outputs` returns at its first line unless
    *both* stores are a `LocalDBStore`, so a plain `LocalStore` backend takes
    the other path -- the one every backend that is not a local daemon takes.
    """
    common = {"extra_env": _nix_config_env(case), "no_probe": True}
    local_spec = make_test_spec(store_id="local", store_path=local_root, no_schedule=True, **common)
    builder_spec = make_test_spec(store_id="builder", store_path=builder_root, **common)
    async with Server(
        stores={
            StoreId("local"): LocalDBStore(local_spec),
            StoreId("builder"): builder_class(builder_spec),
        },
        ssh_port=0,
        http_port=0,
        unix_path=local_root.parent / "pynixd.sock",
    ) as server:
        engine = GoalEngine(server.ctx)
        request = BuildPathsWithResultsRequest(
            derived_paths=cast("Any", {SerdeDerivedPath(value=f"{drv_path}!*")}),
            build_mode=BuildMode.NORMAL,
        )
        # No wait for the outputs to arrive, and that is deliberate.
        # `Scheduler.execute_build` marks a build complete before it calls
        # `_collect_outputs`, which reads as a race, so this arm carried a two
        # second sleep. Removing the sleep changed nothing across a full run,
        # because `_collect_outputs` is awaited inside the same coroutine and
        # the server teardown drains it. A workaround for a problem that will
        # not reproduce is a workaround that hides the next one.
        return await engine.build_paths_with_results(request)


async def _build_with_nix(root: Path, drv_path: str, case: Case) -> Any:
    """Realise every output of *drv_path* through the goal system of Nix."""
    _apply_oracle_settings(case)
    async with huggorm.AsyncSession(f"local://?root={root}") as session:
        store = session.store()
        derived = await store.parse_derived_path(f"{drv_path}^*")
        return await store.build_paths_with_results([derived])


async def _instantiate_both(root_a: Path, root_b: Path, case: Case) -> str:
    """Write the `.drv` of *case* into both stores, and return the one path.

    Evaluation is deterministic, so the two have to agree. Saying so here is
    what keeps the build the only difference between the arms.
    """
    drv_a = await _instantiate(root_a, case)
    drv_b = await _instantiate(root_b, case)
    assert drv_a == drv_b, (
        f"the two arms instantiated {case.name} to different derivations, so the "
        f"comparison would say nothing: {drv_a} against {drv_b}"
    )
    return drv_a


def _assert_outcomes_agree(case: Case, response_a: Any, results_b: Any) -> None:
    """Both engines reached the outcome the case declares.

    Without this the failing cases pass for the wrong reason: a store gains no
    path when a build fails, and it gains no path when no build runs either.
    """
    pynixd_succeeded = all(result_succeeded(item.result) for item in response_a.results)
    nix_succeeded = all(result.success() for result in results_b)
    assert pynixd_succeeded == case.expect_success, (
        f"{case.name}: pynixd reported success={pynixd_succeeded}, and the case "
        f"declares success={case.expect_success}.\n  {response_a.results}"
    )
    assert nix_succeeded == case.expect_success, (
        f"{case.name}: Nix reported success={nix_succeeded}, and the case "
        f"declares success={case.expect_success}.\n  {results_b}"
    )


def _assert_stores_agree(
    case: Case,
    added_a: StoreSnapshot,
    added_b: StoreSnapshot,
    *,
    ignore_fields: Collection[str] = (),
) -> None:
    """The two arms added the same paths, with the same facts about each.

    A comparison of two empty sets proves nothing, so a case that is meant to
    succeed has to have left something behind in both.
    """
    if case.expect_success:
        assert added_a.paths, f"{case.name}: pynixd reported success and added no path to its store"
        assert added_b.paths, f"{case.name}: Nix reported success and added no path to its store"

    difference = compare(added_a, added_b, ignore_fields=ignore_fields)
    assert not difference, (
        f"{case.name}: the two goal systems left different stores.\n"
        f"This case probes: {case.probes}\n\n"
        f"{difference.describe('pynixd', 'nix')}"
    )


@pytest.mark.parametrize("case", (*CORPUS, *CA_CORPUS), ids=lambda case: case.name)
async def test_both_engines_leave_the_same_store(case: Case, differential_roots: DifferentialRoots) -> None:
    """pynixd's goal engine and Nix's goal system agree on what they built.

    One store on the pynixd side, so this asks about the goal graph alone.
    `test_a_distributed_build_reassembles_the_same_store` asks the fleet
    question.
    """
    roots = differential_roots
    drv = await _instantiate_both(roots.pynixd, roots.nix, case)

    before_a = await _read_store(roots.pynixd)
    response_a = await _build_with_pynixd(roots.pynixd, drv, case)
    after_a = await _read_store(roots.pynixd)

    before_b = await _read_store(roots.nix)
    results_b = await _build_with_nix(roots.nix, drv, case)
    after_b = await _read_store(roots.nix)

    _assert_outcomes_agree(case, response_a, results_b)
    _assert_stores_agree(case, delta(before_a, after_a), delta(before_b, after_b))


@pytest.mark.parametrize("case", (*CORPUS, *CA_CORPUS), ids=lambda case: case.name)
async def test_a_distributed_build_reassembles_the_same_store(
    case: Case,
    differential_roots: DifferentialRoots,
) -> None:
    """A build placed on a second store still leaves the client's store correct.

    This is the question pynixd exists to answer. Every build runs on
    `builder`, and the store a client talks to is `local`, which schedules
    nothing. So each output has to travel, and the test asks whether what
    arrives is what Nix would have produced -- the same NAR hash, the same
    size, the same references, the same deriver and the same addressing.

    References are the part most worth watching, and the `chain` and `diamond`
    cases are the ones that carry them. The scheduler used to reassemble by
    copying `ValidPaths` and `Refs` rows out of the builder's SQLite database,
    where a reference to a path that had not arrived yet inserted nothing and
    reported nothing. `Scheduler._pull_outputs` streams the closure over the
    wire now, so a reference cannot go missing -- issue nixidae/nixidae#197.
    """
    roots = differential_roots
    drv = await _instantiate_both(roots.pynixd, roots.nix, case)

    before_a = await _read_store(roots.pynixd)
    before_builder = await _read_store(roots.builder)
    response_a = await _build_with_pynixd_fleet(roots.pynixd, roots.builder, drv, case)
    after_a = await _read_store(roots.pynixd)
    after_builder = await _read_store(roots.builder)

    before_b = await _read_store(roots.nix)
    results_b = await _build_with_nix(roots.nix, drv, case)
    after_b = await _read_store(roots.nix)

    _assert_outcomes_agree(case, response_a, results_b)

    added_a = delta(before_a, after_a)
    added_builder = delta(before_builder, after_builder)

    if case.expect_success:
        # The test is only about a fleet if the work really left the client's
        # store. Without this, a `no_schedule` that stopped working would make
        # this test a copy of the one above and nothing would report it.
        assert added_builder.paths, (
            f"{case.name}: the builder store gained no path, so the build did not "
            f"go there. `no_schedule` on the local store is what should have "
            f"forced it, and the scheduler placed the work somewhere else."
        )
        # Say where the output went. "the client store gained nothing" and
        # "nothing was built anywhere" are different failures.
        assert added_a.paths, (
            f"{case.name}: pynixd reported success and the client's store gained no path, "
            f"while the builder store gained {len(added_builder)}. The build ran and the "
            f"outputs never travelled."
        )
        # Every path the client gained has to have come from the builder. This
        # is the reassembly stated as a fact rather than assumed.
        stranded = sorted(set(added_a.paths) - set(added_builder.paths))
        assert not stranded, (
            f"{case.name}: the client's store holds {len(stranded)} path(s) that the "
            f"builder never produced:\n  " + "\n  ".join(stranded)
        )

        # A path the client received is not a path the client built, and Nix
        # says so with `ultimate`. `newInfo.ultimate = true` is set in
        # `unix/build/derivation-builder.cc` alone -- the local builder -- and
        # every copy path clears it. A real `nix build --builders` therefore
        # leaves `ultimate = false` in the store that receives the output, and
        # pynixd has to agree.
        #
        # This is asserted rather than ignored. The SQLite shortcut that
        # `_pull_outputs` replaced copied the row verbatim, so it said `true`
        # and marked the client's store as the builder of a path it never
        # built. Dropping the field from the comparison would let that back in
        # unnoticed.
        not_received = sorted(path for path, facts in added_a.paths.items() if facts.ultimate)
        assert not not_received, (
            f"{case.name}: the client's store calls {len(not_received)} received path(s) "
            f"`ultimate`, which claims it built them:\n  " + "\n  ".join(not_received)
        )

    # `ultimate` is dropped from the comparison for the reason just asserted:
    # this arm received its outputs and the Nix arm built them in place, so the
    # field cannot agree and the assertion above pins the value instead.
    _assert_stores_agree(case, added_a, delta(before_b, after_b), ignore_fields=("ultimate",))


# Two cases and not the whole corpus. The question here is not which shapes a
# goal graph gets right -- `test_both_engines_leave_the_same_store` answers
# that. It is whether an output reaches a client at all when the build ran on a
# backend, and one derivation plus one reference between two derivations is
# enough to ask it.
_FLEET_WIRE_SUBSET = tuple(case for case in CORPUS if case.name in {"single", "chain"})


async def _copy_from_pynixd(
    socket_path: Path,
    local_root: Path,
    client_root: Path,
    paths: list[str],
    case: Case,
) -> None:
    """Fetch *paths* out of a running pynixd with a real `nix` client."""
    uri = f"unix://{socket_path}?root={local_root}"
    process = await anyio.run_process(
        [
            "nix",
            "copy",
            "--extra-experimental-features",
            "nix-command",
            "--no-check-sigs",
            "--from",
            uri,
            "--to",
            f"local://?root={client_root}",
            *paths,
        ],
        env=_nix_config_env(case),
        check=False,
    )
    if process.returncode != 0:
        message = process.stderr.decode(errors="replace")
        raise AssertionError(
            f"a client could not fetch the outputs of {case.name} from pynixd.\n"
            f"  from: {uri}\n  paths: {paths}\n{message}"
        )


@pytest.mark.parametrize("case", _FLEET_WIRE_SUBSET, ids=lambda case: case.name)
async def test_a_client_fetches_a_backend_built_output_through_pynixd(
    case: Case,
    differential_roots: DifferentialRoots,
) -> None:
    """A build that ran on a backend reaches a client, and matches Nix.

    This asks the fleet question where pynixd answers it, which is the wire and
    not the disk of the local store.

    An earlier version of this test read the local store directly and called a
    missing path a defect. It is not one. `DaemonProxy.store_for_output_path`
    looks a path up in `ctx.output_locations`, and `QueryValidPaths` reports a
    backend-resident output as valid on the strength of that. The local store
    holding nothing is the design: pynixd records where an output is and serves
    it from there, and `Scheduler._direct_import_localdb_outputs` is an
    optimisation for the one case where both stores are a `LocalDBStore`.

    So the builder here is a plain `LocalStore`, which is the path every
    backend that is not a local daemon takes, and the client is a real `nix`
    process copying out of pynixd over its Unix socket. What the client ends up
    with is compared against what Nix produced.

    Issue nixidae/nixidae#199 has two layers, and the first one is fixed. `nix copy`
    realises its installables against the source store before it copies, so a
    store path installable arrives as an opaque derived path in a `BuildPaths`
    request -- and `DaemonProxy.execute` hands that to the goal engine before
    any of its mapping code runs. `EnsureDerivedPathGoal._ensure_opaque` asked
    the local store alone, so it failed every backend-resident path.

    The second layer is why path mapping alone could not fix it.
    `UDSRemoteStore::narFromPath` calls `Store::narFromPath`, which reads the
    store **directory** through `LocalFSStore::getFSAccessor`. A `unix://`
    client therefore never sends `NarFromPath`, and `NarFromPathHandler`
    cannot serve it a path that lives on a backend. `Scheduler._pull_outputs`
    puts the output in the local store, which is what this test proves.
    """
    roots = differential_roots
    drv = await _instantiate_both(roots.pynixd, roots.nix, case)

    # Nix first, so the paths it produced are what the client then asks pynixd
    # for. Asking for exactly those is a sharper question than asking pynixd
    # what it has.
    before_b = await _read_store(roots.nix)
    results_b = await _build_with_nix(roots.nix, drv, case)
    added_b = delta(before_b, await _read_store(roots.nix))
    wanted = sorted(added_b.paths)

    common = {"extra_env": _nix_config_env(case), "no_probe": True}
    local_spec = make_test_spec(store_id="local", store_path=roots.pynixd, no_schedule=True, **common)
    builder_spec = make_test_spec(store_id="builder", store_path=roots.builder, **common)
    before_builder = await _read_store(roots.builder)
    socket_path = roots.pynixd.parent / "pynixd.sock"
    async with Server(
        stores={
            StoreId("local"): LocalDBStore(local_spec),
            StoreId("builder"): LocalStore(builder_spec),
        },
        ssh_port=0,
        http_port=0,
        unix_path=socket_path,
    ) as server:
        engine = GoalEngine(server.ctx)
        response_a = await engine.build_paths_with_results(
            BuildPathsWithResultsRequest(
                derived_paths=cast("Any", {SerdeDerivedPath(value=f"{drv}!*")}),
                build_mode=BuildMode.NORMAL,
            ),
        )
        _assert_outcomes_agree(case, response_a, results_b)
        await _copy_from_pynixd(socket_path, roots.pynixd, roots.client, wanted, case)

    added_builder = delta(before_builder, await _read_store(roots.builder))
    assert added_builder.paths, (
        f"{case.name}: the builder store gained no path, so the build did not go there "
        f"and this test is not about a fleet"
    )

    # Every path the client holds, compared against the store Nix left. The
    # client store started empty, so its whole contents are the delta.
    #
    # `ultimate` is dropped here, and only here. It marks a path the store
    # built itself, and Nix clears it on every copy -- `Store::copyPaths`,
    # `Store::addMultipleToStore` and `Store::copyStorePath` each set
    # `ultimate = false` on the way in. The Nix arm built its path in place and
    # the client received a copy of the pynixd one, so the two can never agree
    # on this field, whatever pynixd does. The other two tests of this module
    # compare a built store against a built store and keep it.
    client_store = await _read_store(roots.client)
    _assert_stores_agree(case, client_store, added_b, ignore_fields=("ultimate",))
