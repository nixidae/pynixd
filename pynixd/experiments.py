"""Experiments: a store spec field that turns on what pynixd does beyond its default.

A field is an experiment when its annotation carries an `Experiment`:
`Annotated[bool, Experiment(...)]`. The field's name is the switch, and its
default is the behaviour the experiment replaces. `declared` lists the
experiments of a spec type, `active` the ones a spec has turned on, and
`pynixd state` reports `active` for each store. A switch is active when its
configured value differs from its default: an `op_routes` that is empty is
off, even under `daemon = false`, where every operation takes `pynixd`.
nixidae/nixidae#1318.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .state_models import ExperimentRow

if TYPE_CHECKING:
    from pydantic import BaseModel

__all__ = ["Experiment", "active", "declared"]


@dataclass(frozen=True, slots=True)
class Experiment:
    """The declaration of one experiment, carried in its field's annotation."""

    replaces: str
    """What pynixd does with the switch at its default."""
    issue: int
    """The nixidae/nixidae issue that holds the experiment's plan and measurements."""
    tests: tuple[str, ...]
    """The test files that cover it, relative to `pynixd/`."""
    details: tuple[str, ...] = ()
    """Fields of the same spec that qualify the switch, reported with it (`immutable` for `read_only`)."""


def declared(spec_type: type[BaseModel]) -> dict[str, Experiment]:
    """Each experiment of *spec_type*, by its switch."""
    return {
        name: marks[0]
        for name, field in spec_type.model_fields.items()
        if (marks := [m for m in field.metadata if isinstance(m, Experiment)])
    }


def active(spec: BaseModel, store: str) -> list[ExperimentRow]:
    """The experiments *spec*, the spec of *store*, turns on, in field order."""
    fields = type(spec).model_fields
    rows = []
    for name, experiment in declared(type(spec)).items():
        if getattr(spec, name) == fields[name].get_default(call_default_factory=True):
            continue
        values = spec.model_dump(mode="json", include={name, *experiment.details})
        rows.append(
            ExperimentRow(
                store=store,
                switch=name,
                value=values[name],
                details={detail: values[detail] for detail in experiment.details},
                issue=experiment.issue,
            )
        )
    return rows
