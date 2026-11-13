"""Experiments: one declaration per switch, and `pynixd state` lists what a store turned on.

The list derives from `Experiment` marks on the store spec's fields (`pynixd/experiments.py`). These tests
hold the declarations to the files they name and to the table in `docs/architecture.md`, in both directions.
nixidae/nixidae#1318.
"""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, cast

from pydantic import BaseModel, Field

from nix_daemon_protocol.ids import StoreId
from pynixd import state as state_collector
from pynixd.config import BuildVmSpec, LocalSocketStoreSpec
from pynixd.context import PynixdContext
from pynixd.experiments import Experiment, active, declared
from pynixd.op_routes import OpRoute
from pynixd.sessions import ClientSessions
from pynixd.state_models import ExperimentRow, StateReport

ROOT = Path(__file__).resolve().parents[2]
TABLE_ROW = re.compile(r"^\| `(\w+)` \|", re.MULTILINE)


class _Toy(BaseModel):
    plain: bool = False
    flag: Annotated[bool, Experiment(replaces="off", issue=1, tests=("t.py",))] = False
    table: Annotated[dict[str, int], Experiment(replaces="empty", issue=2, tests=("t.py",), details=("plain",))] = (
        Field(default_factory=dict)
    )


def _spec(**kwargs: object) -> LocalSocketStoreSpec:
    return LocalSocketStoreSpec.model_validate({"store_id": "local", "store_path": "/tmp/x", **kwargs})


def test_declared_reads_only_the_marked_fields() -> None:
    assert list(declared(_Toy)) == ["flag", "table"]


def test_active_lists_a_switch_off_its_default_and_its_details() -> None:
    assert active(_Toy(), "s") == []
    assert active(_Toy(flag=True, table={"a": 1}, plain=True), "s") == [
        ExperimentRow(store="s", switch="flag", value=True, issue=1),
        ExperimentRow(store="s", switch="table", value={"a": 1}, details={"plain": True}, issue=2),
    ]


def test_the_store_spec_declares_the_five_experiments() -> None:
    assert list(declared(LocalSocketStoreSpec)) == ["op_routes", "pynixd_writes", "daemon", "read_only", "build_vm"]


def test_every_declared_test_file_exists() -> None:
    missing = [
        f"{switch}: {path}"
        for switch, experiment in declared(LocalSocketStoreSpec).items()
        for path in experiment.tests
        if not (ROOT / path).is_file()
    ]
    assert missing == []
    assert all(experiment.tests and experiment.issue > 0 for experiment in declared(LocalSocketStoreSpec).values())


def test_the_architecture_doc_lists_each_experiment_once() -> None:
    text = (ROOT / "docs" / "architecture.md").read_text(encoding="utf-8")
    section = text.split("## Experiments", 1)[1].split("\n## ", 1)[0]
    assert TABLE_ROW.findall(section) == list(declared(LocalSocketStoreSpec))


def test_a_default_store_turns_nothing_on() -> None:
    assert active(_spec(), "local") == []


def test_a_store_reports_what_it_turned_on() -> None:
    spec = _spec(
        op_routes={"QueryReferrers": OpRoute.pynixd},
        pynixd_writes=True,
        daemon=False,
        build_vm=BuildVmSpec(),
    )
    rows = {row.switch: row for row in active(spec, "local")}
    assert list(rows) == ["op_routes", "pynixd_writes", "daemon", "build_vm"]
    assert rows["op_routes"].value == {"QueryReferrers": "pynixd"}
    assert rows["daemon"].value is False
    assert rows["build_vm"].value == BuildVmSpec().model_dump(mode="json")
    assert rows["daemon"].issue == 1155


def test_read_only_carries_immutable() -> None:
    (row,) = active(_spec(read_only=True, immutable=True), "local")
    assert (row.switch, row.value, row.details) == ("read_only", True, {"immutable": True})


def test_state_lists_the_experiments_of_every_store() -> None:
    a = ExperimentRow(store="a", switch="daemon", value=False, issue=1155)
    b = ExperimentRow(store="b", switch="read_only", value=True, details={"immutable": False}, issue=1309)
    stores = {StoreId("a"): SimpleNamespace(experiments=[a]), StoreId("b"): SimpleNamespace(experiments=[b])}
    ctx = SimpleNamespace(scheduler=None, stores=stores, sessions=ClientSessions())
    report = state_collector.collect(cast("PynixdContext", ctx), ["experiments"])
    assert report == StateReport(experiments=[a, b])
    assert StateReport.model_validate_json(report.model_dump_json()) == report
