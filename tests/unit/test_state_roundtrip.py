"""Every `pynixd state` section survives the daemon's answer. nixidae/nixidae#1421.

The daemon dumps the report with `exclude_none=True`; the CLI parses it with the
same models. A field typed `X | None` with no default then fails the parse:
`ProvisionedRow.notice_s` broke `pynixd state` on dynhetz while a builder ran.
The samples are built from the models, so a new section or field is covered.
"""

from __future__ import annotations

import types
import typing

import pytest
from pydantic import BaseModel, JsonValue

from pynixd.state_models import StateReport

NESTING = 2
"""How deep `federated` repeats the report in a sample."""


def _sample(annotation: object, depth: int, *, fill: bool) -> object:  # noqa: PLR0911 -- one branch per kind of type
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if annotation is JsonValue:
        return "x"
    if origin is typing.Literal:
        return args[0]
    if origin in {typing.Union, types.UnionType}:
        members = [arg for arg in args if arg is not type(None)]
        return _sample(members[-1], depth, fill=fill)
    if origin is list:
        return [_sample(args[0], depth, fill=fill)]
    if origin is dict:
        return {_sample(args[0], depth, fill=fill): _sample(args[1], depth, fill=fill)}
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return _model(annotation, depth + 1, fill=fill)
    if annotation is bool:
        return True
    if annotation is int:
        return 2
    if annotation is float:
        return 1.5
    if annotation is str:
        return "x"
    pytest.fail(f"no sample for {annotation!r}: teach _sample the type")


def _model(model: type[BaseModel], depth: int, *, fill: bool) -> BaseModel:
    """*model* with every required field set, and each optional one set when *fill*, else None.

    The report's own sections are always set: an empty report tests nothing.
    """
    values: dict[str, object] = {}
    for name, field in model.model_fields.items():
        optional = type(None) in typing.get_args(field.annotation)
        if model is StateReport and depth > NESTING:
            continue
        if optional and not fill and model is not StateReport:
            values[name] = None
            continue
        values[name] = _sample(field.annotation, depth, fill=fill)
    return model.model_validate(values)


@pytest.mark.parametrize("fill", [False, True], ids=["optional-none", "optional-set"])
def test_the_cli_reads_what_the_daemon_sends(fill: bool) -> None:
    report = _model(StateReport, 0, fill=fill)
    sent = report.model_dump_json(exclude_none=True)
    assert StateReport.model_validate_json(sent) == report
