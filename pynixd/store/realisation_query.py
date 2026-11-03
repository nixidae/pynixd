"""A realisation asked of a store over the worker protocol: Nix's `queryRealisation` on a remote store."""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from ..serde import KeyedDrvOutput, QueryRealisationRequest, Realisation, StorePath

if TYPE_CHECKING:
    from ..serde import DrvOutput, QueryRealisationResponse
    from .base import Store

log = structlog.get_logger(__name__)


async def query_realisation_over_wire(store: Store, key: DrvOutput, drv_path: StorePath) -> Realisation | None:
    """The realisation of *key* that *store* answers to `QueryRealisation` (op 43), or `None`."""
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
