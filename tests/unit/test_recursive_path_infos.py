"""Recursive Nix answers Determinate Nix's op 50 one path at a time, through the restriction.

`DaemonProxy.honourable_features` names `queryPathInfos` to a build when one backend names it, so a
builder's client may send op 50. `Store::queryPathInfos` of Determinate Nix calls `queryPathInfo` for
each path, and `RestrictedStore` answers that one (`restricted-store.cc:179`).
"""

from __future__ import annotations

from pathlib import Path

from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.query_path_info import QueryPathInfoRequest, QueryPathInfoResponse
from nix_daemon_protocol.query_path_infos import QueryPathInfosRequest, QueryPathInfosResponse
from nix_daemon_protocol.signature import Signature
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.wire_ops import WireRequest, WireResponse
from nix_daemon_protocol.wire_time import Time
from pynixd.recursive import RestrictedProxy
from pynixd_store.builder.restriction import RestrictionContext

ALLOWED = StorePath("/nix/store/00000000000000000000000000000000-allowed")
HIDDEN = StorePath("/nix/store/11111111111111111111111111111111-hidden")
MISSING = StorePath("/nix/store/22222222222222222222222222222222-missing")


def _info() -> UnkeyedValidPathInfo:
    return UnkeyedValidPathInfo(
        deriver=StorePath("/nix/store/33333333333333333333333333333333-x.drv"),
        nar_hash=NARHash(hash="abc123"),
        references=set(),
        registration_time=Time(ts=1700000000),
        nar_size=8,
        ultimate=True,
        sigs={Signature(value="cache:sig")},
        ca=ContentAddress(value=""),
    )


class _Proxy(RestrictedProxy):
    """A restricted proxy whose store holds ALLOWED and HIDDEN, and no session."""

    def __init__(self, restriction: RestrictionContext) -> None:
        self.restriction = restriction

    async def _next(self, request: WireRequest) -> WireResponse:
        assert isinstance(request, QueryPathInfoRequest)
        if request.path in (ALLOWED, HIDDEN):
            return QueryPathInfoResponse(valid=True, info=_info())
        return QueryPathInfoResponse(valid=False)


async def test_op_50_answers_only_the_allowed_valid_paths_censored() -> None:
    restriction = RestrictionContext({ALLOWED, MISSING}, None, Path("/nix/store"), submitting=False)

    response = await _Proxy(restriction).execute(QueryPathInfosRequest(paths={ALLOWED, HIDDEN, MISSING}))

    assert isinstance(response, QueryPathInfosResponse)
    assert [info.path for info in response.infos] == [ALLOWED]
    info = response.infos[0].info
    assert (info.deriver, info.registration_time, info.ultimate, info.sigs) == (None, 0, False, set())
