"""A builder receives each path after the paths it references.

nix-daemon registers the paths of `AddMultipleToStore` in arrival order and
refuses a path whose reference it does not hold yet. The local closure query
orders by `ValidPaths.id`, and on dynhetz `boehm-gc-8.2.12-dev` (id 600118)
references `boehm-gc-8.2.12` (id 600120). Every sg1 node refused the batch
with `path '...-boehm-gc-8.2.12' is not valid`, on every retry.
Issue nixidae/nixidae#897.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from nix_daemon_protocol.valid_path_info import ValidPathInfo
from pynixd.daemon_extensions.query_closure_with_info import QueryClosureWithInfoResponse
from pynixd.serde import (
    ContentAddress,
    NARHash,
    QueryValidPathsResponse,
    StorePath,
    Time,
    UnkeyedValidPathInfo,
)
from pynixd.store import transfer


def _path(name: str) -> StorePath:
    return StorePath(path=f"/nix/store/{'0' * 32}-{name}")


def _info(name: str, *references: str) -> ValidPathInfo:
    return ValidPathInfo(
        path=_path(name),
        info=UnkeyedValidPathInfo(
            deriver=None,
            nar_hash=NARHash(hash="0" * 64),
            references={_path(name), *map(_path, references)},
            registration_time=Time(ts=1),
            nar_size=96,
            ultimate=True,
            sigs=set(),
            ca=ContentAddress(value=""),
        ),
    )


class _Source:
    store_id = "source"

    def __init__(self, infos: list[ValidPathInfo]) -> None:
        self._infos = infos

    async def execute(self, _request: object, client: object = None) -> QueryClosureWithInfoResponse:  # noqa: ARG002
        return QueryClosureWithInfoResponse(infos=self._infos)


class _Destination:
    store_id = "destination"

    async def execute(self, _request: object) -> QueryValidPathsResponse:
        return QueryValidPathsResponse(paths=set())


async def test_a_reference_registered_later_is_sent_first(monkeypatch: pytest.MonkeyPatch) -> None:
    """The closure in `ValidPaths.id` order: the referrer before its reference."""
    sent: list[str] = []

    async def record(_src: object, _dst: object, to_transfer: list[ValidPathInfo], *_: object) -> None:
        sent.extend(str(info.path).rsplit("-", 1)[-1] for info in to_transfer)

    monkeypatch.setattr(transfer, "_stream_checked", record)
    closure = [_info("top", "dev"), _info("dev", "lib"), _info("lib", "libc"), _info("libc")]
    await transfer.stream_paths_store_to_store(
        cast("Any", _Source(closure)), cast("Any", _Destination()), [_path("top")]
    )
    assert sent == ["libc", "lib", "dev", "top"]
