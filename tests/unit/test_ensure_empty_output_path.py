"""The empty path of a content-addressed output never reaches the wire.

`Derivation.output_paths` answers `StorePath("")` for an output that the
derivation does not name a path for. A content-addressed output is that case:
the name asks the store for a realisation, and the path is known only after
the build.

`EnsureDerivedPathGoal._try_substitute_known_outputs` takes a path to the
wire, so it must drop that entry. It did not, and the daemon behind pynixd
then closed the connection with no error word. `daemon.cc:701` parses the
store path of `EnsurePath` **before** `logger->startWork()`, so
`canSendStderr` is still false when `parseStorePath` throws `BadStorePath`,
and `daemon.cc:1213` rethrows an error it cannot report. The client read the
end of the file, and `nix build` failed with `IncompleteReadError`.
`ca:build-cache` and `ca:issue-13247` both failed that way. Issue nixidae/nixidae#234.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from pynixd.derived_path import DerivedPath
from pynixd.goals.ensure import EnsureDerivedPathGoal
from pynixd.serde import BuildMode, IsValidPathRequest, IsValidPathResponse
from pynixd.store_path import StorePath

if TYPE_CHECKING:
    from pynixd.goals.engine import GoalEngine

_DRV = "/nix/store/00000000000000000000000000000000-content-addressed.drv"
_PATH = StorePath("/nix/store/11111111111111111111111111111111-known")


class RecordingStore:
    """A store that answers `IsValidPath` and records every path it is asked."""

    def __init__(self) -> None:
        self.paths: list[str] = []

    async def execute(self, request: Any, **_kwargs: Any) -> Any:
        if isinstance(request, IsValidPathRequest):
            self.paths.append(str(request.path))
            return IsValidPathResponse(valid=True)
        raise AssertionError(f"unexpected request {type(request).__name__}")


def _goal(store: RecordingStore) -> EnsureDerivedPathGoal:
    engine = SimpleNamespace(ctx=SimpleNamespace(local_store=store))
    return EnsureDerivedPathGoal(
        engine=cast("GoalEngine", engine),
        derived_path=DerivedPath(f"{_DRV}!out"),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )


@pytest.mark.anyio
async def test_an_output_with_no_path_asks_the_store_nothing() -> None:
    """Every output of a content-addressed derivation is this case."""
    store = RecordingStore()

    result = await _goal(store)._try_substitute_known_outputs({"out": StorePath("")})

    assert result is None
    assert store.paths == []


@pytest.mark.anyio
async def test_a_named_path_beside_an_empty_one_still_goes_through() -> None:
    """The filter drops the empty entry alone, and not the whole map."""
    store = RecordingStore()

    result = await _goal(store)._try_substitute_known_outputs({"out": StorePath(""), "dev": _PATH})

    assert result is not None
    assert result.resolved_outputs == {"dev": _PATH}
    assert store.paths == [str(_PATH)]


@pytest.mark.anyio
async def test_an_absent_path_asks_the_store_nothing() -> None:
    """`None` and the empty path both mean "the build decides"."""
    store = RecordingStore()

    assert await _goal(store)._try_substitute_known_outputs({"out": None}) is None
    assert store.paths == []
