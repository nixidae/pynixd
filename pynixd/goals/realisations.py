"""The output path that a realisation names, and not the derivation.

`Store::queryPartialDerivationOutputMap` answers the path of each output of a
derivation. For an input-addressed output the derivation names that path. For
a deferred, floating or impure output the derivation names nothing, and the
store answers instead: a realisation maps
`DrvOutput{staticOutputHashes(drv)[name], name}` to the path that the build
made.

**A derivation that names no output path is not therefore unbuilt.** Two goals
of pynixd read the derivation alone and drew that conclusion, and each one was
wrong in its own way:

- `EnsureDerivedPathGoal` built the derivation again. Issue nixidae/nixidae#224.
- `QueryMissingPlanGoal` answered `willBuild`, and `nix-daemon` answers an
  empty set, because `misc.cc:217` reads the same map and finds every output
  valid.

Issue nixidae/nixidae#214.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog

from nix_daemon_protocol.exceptions import DaemonProtocolError
from pynixd_store.drv_hash import output_hashes

from ..exceptions import BackendError, CacheFault
from ..serde import DrvOutput, KeyedDrvOutput, QueryRealisationRequest, Realisation, StorePath
from ..store import is_binary_cache

if TYPE_CHECKING:
    from collections.abc import Iterable

    from pynixd_store.drv_parser import Derivation
    from pynixd_store.store_path import StorePath

    from ..serde import QueryRealisationResponse
    from ..store.base import Store

log = structlog.get_logger(__name__)


async def realisations_of(
    parsed: Derivation,
    wanted: Iterable[str],
    store: Store,
    drv_path: StorePath,
) -> dict[str, Realisation] | None:
    """The realisation of each wanted output, keyed by its `DrvOutput` id.

    The id is `sha256:<digest>!<output name>`, which is the string form that
    `DrvOutput::to_string` writes and the wire carries.

    Answers `None` when any wanted output has no realisation, and also when
    the closure gives no hash. A dynamic input derivation does that, and so
    does an input derivation that the store does not hold. The caller then
    knows nothing about the outputs, and it must not read the empty answer as
    "no output is realised".

    This does not say that the path is valid. A realisation stays in the store
    after a garbage collection removes the path it names, so the caller checks
    the path as well.
    """
    names = sorted(wanted)
    if not names:
        return None
    hashes = await output_hashes(parsed, store.read_derivation)
    if hashes is None:
        return None

    answer: dict[str, Realisation] = {}
    for output_name in names:
        digest = hashes.get(output_name)
        if digest is None:
            return None
        key = f"sha256:{digest}!{output_name}"
        realisation = await _query_one(store, DrvOutput(key), drv_path)
        if realisation is None:
            return None
        answer[key] = realisation
    return answer


@dataclass(frozen=True, slots=True)
class RealisationSearch:
    """What the substituters answered for the wanted outputs.

    *found* is `None` when an output has no realisation. *substituter_failed*
    says that a substituter failed while no other one held that output: Nix's
    `DrvOutputSubstitutionGoal` then ends `ecFailed`, not `ecNoSubstituters`
    (`drv-output-substitution-goal.cc:61`), and the derivation goal fails
    without `--fallback` (`derivation-goal.cc:124`).
    """

    found: dict[str, Realisation] | None
    substituter_failed: bool = False


async def substitutable_realisations(
    parsed: Derivation,
    wanted: Iterable[str],
    substituters: Iterable[Store],
    local: Store,
    drv_path: StorePath,
    *,
    by_path: bool,
) -> RealisationSearch:
    """The realisation of each wanted output, from the first of *substituters* that holds it.

    `DrvOutputSubstitutionGoal::init` (`drv-output-substitution-goal.cc:30`)
    and `queryMissing` (`misc.cc:250`) ask the substituters in priority order
    and take the first answer; a tie keeps the listed order. A substituter
    that fails is skipped, as Nix prints the error and goes on. The ids are
    those `realisations_of` gives.
    *by_path* is the layout of the local store's Nix (`BinaryCacheStore.query_realisation`).
    """
    names = sorted(wanted)
    if not names:
        return RealisationSearch(None)
    hashes = await output_hashes(parsed, local.read_derivation)
    if hashes is None:
        return RealisationSearch(None)
    ordered = sorted(substituters, key=lambda store: store.priority)
    answer: dict[str, Realisation] = {}
    for output_name in names:
        digest = hashes.get(output_name)
        if digest is None:
            return RealisationSearch(None)
        key = DrvOutput(f"sha256:{digest}!{output_name}")
        realisation, failed = await _first_holding(ordered, key, drv_path, by_path=by_path)
        if realisation is None:
            log.debug(
                "substitutable_realisation_missing",
                drv_output=str(key),
                drv_path=str(drv_path),
                by_path=by_path,
                substituter_failed=failed,
                substituters=[str(store.store_id) for store in ordered],
            )
            return RealisationSearch(None, substituter_failed=failed)
        answer[str(key)] = realisation
    return RealisationSearch(answer)


async def _first_holding(
    substituters: Iterable[Store], key: DrvOutput, drv_path: StorePath, *, by_path: bool
) -> tuple[Realisation | None, bool]:
    failed = False
    for store in substituters:
        try:
            if is_binary_cache(store):
                found = await store.query_realisation(key, drv_path, by_path=by_path)
            else:
                found = await _query_one(store, key, drv_path)
        except (CacheFault, BackendError, DaemonProtocolError, OSError, EOFError):
            log.warning(
                "substituter_realisation_failed", store_id=str(store.store_id), drv_output=str(key), exc_info=True
            )
            failed = True
            continue
        if found is not None:
            return found, False
    return None, failed


async def _query_one(store: Store, key: DrvOutput, drv_path: StorePath) -> Realisation | None:
    # **Both shapes go in, and the codec writes the one the peers agreed
    # on.** `realisation-with-path-not-hash` decides whether the wire
    # carries `"<drvHash>!<output>"` as one string or a derivation path
    # and an output name as two. This code does not know which connection
    # it will take, and it does not have to: `needs_features` and
    # `unless_features` on the fields of the request pick one and drop the
    # other. Issue #14.
    response = await store.execute(
        QueryRealisationRequest(
            drv_output=key,
            keyed_drv_output=KeyedDrvOutput(drv_path=StorePath(str(drv_path)), output_name=key.output_name),
        ),
    )
    realisation = _the_realisation(response, key)
    if realisation is None or realisation.out_path is None:
        # One line that says which shape answered and what it held, so a
        # run states whether the query missed or the codec did. Issue #14.
        log.debug(
            "realisation_missing",
            drv_output=str(key),
            drv_path=str(drv_path),
            present=response.present,
            old_shape_count=len(response.realisations),
        )
        return None
    return realisation


def _the_realisation(response: QueryRealisationResponse, drv_output: DrvOutput) -> Realisation | None:
    """The one realisation of the answer, whichever shape carried it.

    Nix 2.34 answers a set, and the master branch answers an
    `optional<UnkeyedRealisation>`: a tag, and the body when the tag is 1
    (`daemon.cc:1024`). The caller asked about one output either way.

    **The feature shape carries no id, and this puts *drv_output* back.** The
    request already named the output, so the answer does not name it again.
    A `Realisation` of pynixd carries its own id, and `_register_realisations`
    reads `id.output_name`, so an answer with an empty id would register the
    output under no name at all. The caller built that id to ask the
    question, so it is the right one to give back. Issue #14.
    """
    if response.realisation is not None:
        return Realisation(
            id=drv_output,
            out_path=response.realisation.out_path,
            signatures=sorted(response.realisation.signatures),
        )
    return next(iter(response.realisations), None)
