"""QueryMissing counts the substitution closure and honours substitute=false.

`misc.cc:318-325` walks the references of every substitutable path, so a
client reads the whole missing closure in "N paths will be fetched".
`misc.cc:92` answers no substitutes with `useSubstitutes` off, and
`misc.cc:240`, `:271-292` build a derivation whose substitution is not
allowed, or one of whose wanted outputs no substituter has. pynixd counted
one path, read neither option, and left a partly substitutable derivation's
outputs in `willSubstitute`. nixidae/nixidae#1011.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from pynixd.drv_parser import Derivation, DrvOutput
from pynixd.goals.engine import GoalEngine
from pynixd.goals.query_missing import QueryMissingPlanGoal
from pynixd.serde import (
    DerivedPath as SerdeDerivedPath,
    IsValidPathResponse,
    QueryMissingRequest,
    SetOptionsRequest,
    TimeSpan,
    Verbosity,
)
from pynixd.substitution_queue import SubstitutionAvailability

if TYPE_CHECKING:
    from pynixd.context import PynixdContext
    from pynixd.store_path import StorePath

HELLO = "/nix/store/00000000000000000000000000000001-hello"
GLIBC = "/nix/store/00000000000000000000000000000002-glibc"
GONE = "/nix/store/00000000000000000000000000000003-gone"
DRV = "/nix/store/00000000000000000000000000000004-multi.drv"
OUT = "/nix/store/00000000000000000000000000000005-multi"
DEV = "/nix/store/00000000000000000000000000000006-multi-dev"


class Store:
    def __init__(self, derivations: dict[str, Derivation] | None = None) -> None:
        self.derivations = derivations or {}

    async def execute(self, _request: Any, **_kwargs: Any) -> Any:
        return IsValidPathResponse(valid=False)

    async def read_derivation(self, path: StorePath | str) -> Derivation | None:
        return self.derivations.get(str(path))


class Queue:
    def __init__(self, known: dict[str, SubstitutionAvailability]) -> None:
        self.known = known

    async def can_substitute(self, path: StorePath) -> SubstitutionAvailability:
        return self.known.get(str(path), SubstitutionAvailability.unavailable())


def _available(size: int, *references: str) -> SubstitutionAvailability:
    return SubstitutionAvailability(available=True, nar_size=size, download_size=size, references=frozenset(references))


def _options(*, use_substitutes: bool) -> SetOptionsRequest:
    return SetOptionsRequest(
        keep_failed=False,
        keep_going=False,
        try_fallback=False,
        verbosity=Verbosity.ERROR,
        max_build_jobs=1,
        max_silent_time=TimeSpan(0),
        obsolete_use_build_hook=True,
        build_verbosity=Verbosity.ERROR,
        obsolete_log_type=0,
        obsolete_print_build_trace=0,
        build_cores=1,
        use_substitutes=use_substitutes,
        overrides={},
    )


async def _plan(
    path: str, known: dict[str, SubstitutionAvailability], store: Store | None = None, client: Any = None
) -> Any:
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(local_store=store or Store(), scheduler=SimpleNamespace(substitution_queue=Queue(known))),
    )
    request = QueryMissingRequest(derived_paths=cast("set[SerdeDerivedPath]", {SerdeDerivedPath(value=path)}))
    return await QueryMissingPlanGoal(GoalEngine(ctx), request, client).result()


def _multi() -> Derivation:
    return Derivation(
        outputs=[
            DrvOutput(output_name="dev", path=DEV, hash_algo="", hash_value=""),
            DrvOutput(output_name="out", path=OUT, hash_algo="", hash_value=""),
        ],
    )


@pytest.mark.anyio
async def test_the_references_of_a_substitute_are_counted():
    response = await _plan(HELLO, {HELLO: _available(10, HELLO, GLIBC, GONE), GLIBC: _available(100)})

    assert {str(path) for path in response.will_substitute} == {HELLO, GLIBC}
    assert {str(path) for path in response.unknown} == {GONE}
    assert response.nar_size == 110


@pytest.mark.anyio
async def test_substitute_false_substitutes_nothing():
    client = SimpleNamespace(options=_options(use_substitutes=False))

    response = await _plan(HELLO, {HELLO: _available(10)}, client=client)

    assert not response.will_substitute
    assert {str(path) for path in response.unknown} == {HELLO}


@pytest.mark.anyio
async def test_one_unsubstitutable_output_builds_the_derivation_and_fetches_nothing():
    response = await _plan(f"{DRV}!dev,out", {OUT: _available(10)}, Store({DRV: _multi()}))

    assert {str(path) for path in response.will_build} == {DRV}
    assert not response.will_substitute
    assert response.nar_size == 0


@pytest.mark.anyio
async def test_a_derivation_that_forbids_substitutes_builds():
    derivation = _multi()
    derivation.env["allowSubstitutes"] = ""

    response = await _plan(f"{DRV}!out", {OUT: _available(10)}, Store({DRV: derivation}))

    assert {str(path) for path in response.will_build} == {DRV}
    assert not response.will_substitute


class ValidStore(Store):
    def __init__(self, derivations: dict[str, Derivation], valid: set[str]) -> None:
        super().__init__(derivations)
        self.valid = valid

    async def execute(self, request: Any, **_kwargs: Any) -> Any:
        return IsValidPathResponse(valid=str(request.path) in self.valid)


def _dynamic_format() -> Derivation:
    """A `DrvWithVersion("xp-dyn-drv", ...)` derivation with a known output path."""
    return Derivation(outputs=[DrvOutput(output_name="out", path=OUT, hash_algo="", hash_value="")], is_dynamic=True)


@pytest.mark.anyio
async def test_a_dynamic_format_derivation_whose_output_is_valid_plans_nothing():
    """`misc.cc:203-225` reads the outputs of every derivation, the format aside."""
    store = ValidStore({DRV: _dynamic_format()}, valid={OUT})

    response = await _plan(f"{DRV}!out", {}, store)

    assert not response.will_build


@pytest.mark.anyio
async def test_a_dynamic_format_derivation_whose_output_is_substitutable_is_fetched():
    store = ValidStore({DRV: _dynamic_format()}, valid=set())

    response = await _plan(f"{DRV}!out", {OUT: _available(10)}, store)

    assert not response.will_build
    assert {str(path) for path in response.will_substitute} == {OUT}
