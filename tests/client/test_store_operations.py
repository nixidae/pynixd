"""Nix's own client code drives `nix daemon` and pynixd with the same generated operations (nixidae/nixidae#1249).

huggorm binds libstore's `Store`, so each operation here is the call a Nix
command makes, with a typed result. Each example starts both daemons on empty
stores, applies one list of operations to both, and compares each result or
error. hypothesis shrinks a divergence to the shortest list that shows it.

A difference that makes pynixd better is not loosened here: it gets a
`NIX-DEVIATION` or `NIX-DEFECT` marker and a test of its own.
"""

from __future__ import annotations

import itertools
import os
from pathlib import Path

import anyio
import huggorm  # type: ignore[import-not-found]  # the type gate has no huggorm; see default.nix
import pytest
from hypothesis import HealthCheck, Phase, given, settings
from hypothesis.database import DirectoryBasedExampleDatabase

from tests.client.arms import open_arms
from tests.client.ops import Op, operations, run

_examples = itertools.count()
_SLOW = [HealthCheck.too_slow, HealthCheck.data_too_large]
# Not `Phase.explain`: it ran one failing example 509 times, 306 s, to say which part of it mattered.
_PHASES = [Phase.explicit, Phase.reuse, Phase.generate, Phase.target, Phase.shrink]


def _database() -> DirectoryBasedExampleDatabase | None:
    # A shrunk failure outlives the guest only under `/artifacts`.
    logs = os.environ.get("PYNIXD_TEST_LOG_DIR")
    return None if logs is None else DirectoryBasedExampleDatabase(Path(logs) / "hypothesis")


# The standard set: the same few lists every run, and no example database, because `/work` is a store copy.
STANDARD = settings(
    max_examples=8, derandomize=True, database=None, deadline=None, suppress_health_check=_SLOW, phases=_PHASES
)
EXTENDED = settings(max_examples=300, database=_database(), deadline=None, suppress_health_check=_SLOW, phases=_PHASES)


async def _compare(ops: list[Op]) -> None:
    example = next(_examples)
    async with open_arms(example) as arms, huggorm.AsyncSession() as session:
        uris = [f"unix://{arms.control}", f"unix://{arms.pynixd}"]
        stores = {uri: session.store(uri) for uri in uris}
        divergence = await run(stores, ops, arms.control.parent / f"data{example}")
    if divergence is not None:
        pytest.fail(divergence.describe(), pytrace=False)


@STANDARD
@given(ops=operations)
def test_the_daemons_answer_the_same_sample(ops: list[Op]) -> None:
    anyio.run(_compare, ops)


@pytest.mark.extended
@EXTENDED
@given(ops=operations)
def test_the_daemons_answer_the_same(ops: list[Op]) -> None:
    anyio.run(_compare, ops)
