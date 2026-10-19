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

from typing import TYPE_CHECKING

import structlog

from ..drv_hash import output_hashes
from ..serde import DrvOutput, KeyedDrvOutput, QueryRealisationRequest, Realisation, StorePath

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ..drv_parser import Derivation
    from ..serde import QueryRealisationResponse
    from ..store.base import Store
    from ..store_path import StorePath

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
        # **Both shapes go in, and the codec writes the one the peers agreed
        # on.** `realisation-with-path-not-hash` decides whether the wire
        # carries `"<drvHash>!<output>"` as one string or a derivation path
        # and an output name as two. This code does not know which connection
        # it will take, and it does not have to: `needs_features` and
        # `unless_features` on the fields of the request pick one and drop the
        # other. Issue #14.
        response = await store.execute(
            QueryRealisationRequest(
                drv_output=DrvOutput(key),
                keyed_drv_output=KeyedDrvOutput(drv_path=StorePath(str(drv_path)), output_name=output_name),
            ),
        )
        realisation = _the_realisation(response, DrvOutput(key))
        if realisation is None or realisation.out_path is None:
            # One line that says which shape answered and what it held, so a
            # run states whether the query missed or the codec did. Issue #14.
            log.debug(
                "realisation_missing",
                drv_output=key,
                drv_path=str(drv_path),
                present=response.present,
                old_shape_count=len(response.realisations),
            )
            return None
        answer[key] = realisation
    return answer


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
