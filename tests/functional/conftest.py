"""A shadow mismatch fails the functional suite.

The test stores run the answers on trial in shadow (`TEST_OP_ROUTES`): the
client gets the daemon's answer, so no test sees pynixd's. The counter does,
and this reads it once every test has run.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from prometheus_client import REGISTRY

if TYPE_CHECKING:
    from collections.abc import Iterator


def _disagreements() -> dict[tuple[str, str, str], float]:
    """`(store, op, outcome)` of every shadow that did not match, and how often."""
    found: dict[tuple[str, str, str], float] = {}
    for metric in REGISTRY.collect():
        if metric.name != "pynixd_op_shadow":
            continue
        for sample in metric.samples:
            outcome = sample.labels.get("outcome", "")
            if sample.name.endswith("_total") and outcome in ("mismatch", "error") and sample.value:
                key = (sample.labels["store_id"], sample.labels["op"], outcome)
                found[key] = sample.value
    return found


@pytest.fixture(scope="session", autouse=True)
def _shadows_agree() -> Iterator[None]:
    before = _disagreements()
    yield
    grown = {key: value for key, value in _disagreements().items() if value > before.get(key, 0.0)}
    assert not grown, f"pynixd's shadow disagreed with the daemon (see op_shadow_* in the logs): {grown}"
