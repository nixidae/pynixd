"""The bytes that pynixd writes when an input derivation did not build.

`inputsRealised` at `derivation-building-goal.cc:111` of Nix builds this
message. It colours the store path magenta and the reason red, and it adds
the "Output paths:" section that `showKnownOutputs` writes at
`derivation-building-goal.cc:53`.

pynixd wrote none of the colour, wrote no section, and put seven spaces
after the line feed. Those seven spaces belong to the log line alone:
`_as_an_error` adds them when it writes the line, so the text of the answer
carried them twice. `tests/parity/test_wire_parity.py::...[failure]` read
the difference from a real `nix-daemon`.

Refs nixidae/nixidae#214.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from pynixd.derived_path import DerivedPath
from pynixd.drv_parser import Derivation
from pynixd.goals.ensure import (
    EnsureDerivedPathGoal,
    _as_an_error,  # noqa: PLC2701 -- the exact bytes are the unit under test
    _show_known_outputs,  # noqa: PLC2701 -- the exact bytes are the unit under test
)
from pynixd.goals.results import goal_failure, goal_success
from pynixd.serde import BuildMode, BuildResultStatus, StorePath
from pynixd.store_path import DrvOutput

if TYPE_CHECKING:
    from pynixd.goals.engine import GoalEngine

TOP_DRV = "/nix/store/00000000000000000000000000000001-top.drv"
OUT = "/nix/store/11111111111111111111111111111111-top"
DEV = "/nix/store/22222222222222222222222222222222-top-dev"
IN_DRV = "/nix/store/33333333333333333333333333333333-in.drv"
OTHER_DRV = "/nix/store/44444444444444444444444444444444-other.drv"

MAGENTA = "\x1b[35;1m"
RED = "\x1b[31;1m"
NORMAL = "\x1b[0m"


def _derivation(*outputs: DrvOutput) -> Derivation:
    return Derivation(
        outputs=list(outputs),
        platform="x86_64-linux",
        builder="/bin/sh",
        args=[],
        env={},
    )


def _output(name: str, path: str) -> DrvOutput:
    return DrvOutput(hash_algo="", hash_value="", output_name=name, path=path)


def _goal() -> EnsureDerivedPathGoal:
    return EnsureDerivedPathGoal(
        engine=cast("GoalEngine", object()),
        derived_path=DerivedPath(f"{TOP_DRV}!out"),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )


async def _refusal(parsed: Derivation) -> str:
    answer = await _goal()._refuse_a_failed_input(  # noqa: SLF001 -- the message is the unit under test
        [goal_failure("the input failed", BuildResultStatus.PERMANENT_FAILURE)],
        StorePath(TOP_DRV),
        parsed,
    )
    if answer is None:
        raise AssertionError("a failed input gives a refusal")
    return str(answer.result.error_msg)


@pytest.mark.anyio
async def test_the_message_carries_the_colours_of_nix() -> None:
    """`Magenta(drvPath)` and `ANSI_RED "1 dependency failed" ANSI_NORMAL`."""
    message = await _refusal(_derivation(_output("out", OUT)))

    assert message.startswith(f"Cannot build '{MAGENTA}{TOP_DRV}{NORMAL}'.\n")
    assert f"Reason: {RED}1 dependency failed{NORMAL}." in message


@pytest.mark.anyio
async def test_the_line_feed_carries_no_indent() -> None:
    """`_as_an_error` adds the seven spaces, and the answer carries none."""
    message = await _refusal(_derivation(_output("out", OUT)))

    assert "\nReason:" in message
    assert "\n       Reason:" not in message
    assert "\n       " not in message
    assert "\n       Reason:" in _as_an_error(message)


@pytest.mark.anyio
async def test_the_message_names_each_output_path() -> None:
    """`showKnownOutputs` writes one indented line for each known path."""
    message = await _refusal(_derivation(_output("out", OUT), _output("dev", DEV)))

    assert message.endswith(f"\nOutput paths:\n  {MAGENTA}{OUT}{NORMAL}\n  {MAGENTA}{DEV}{NORMAL}")


@pytest.mark.anyio
async def test_a_derivation_with_no_known_output_path_gets_no_section() -> None:
    """A floating output has no path, and `expectedOutputPaths` stays empty."""
    message = await _refusal(_derivation(_output("out", "")))

    assert "Output paths:" not in message
    assert message.endswith("failed\x1b[0m.")


def test_the_section_orders_each_path() -> None:
    """`StorePathSet` is a set, and it orders each path by its base name."""
    section = _show_known_outputs(_derivation(_output("out", DEV), _output("dev", OUT)))

    assert section == f"\nOutput paths:\n  {MAGENTA}{OUT}{NORMAL}\n  {MAGENTA}{DEV}{NORMAL}"


def test_a_derivation_with_no_output_gets_no_section() -> None:
    assert _show_known_outputs(_derivation()) == ""


@pytest.mark.anyio
async def test_an_input_that_succeeded_gives_no_refusal() -> None:
    answer = await _goal()._refuse_a_failed_input(  # noqa: SLF001 -- the message is the unit under test
        [goal_success()],
        StorePath(TOP_DRV),
        _derivation(_output("out", OUT)),
    )

    assert answer is None


async def _count_under_keep_going(nodes: list[tuple[str, tuple[str, ...]]]) -> str:
    goal = _goal()
    goal._watchers.append(cast("Any", SimpleNamespace(options=SimpleNamespace(keep_going=1))))  # noqa: SLF001 -- the client of the goal
    answer = await goal._refuse_a_failed_input(  # noqa: SLF001 -- the message is the unit under test
        [goal_failure("the input failed", BuildResultStatus.PERMANENT_FAILURE) for _ in nodes],
        StorePath(TOP_DRV),
        _derivation(_output("out", OUT)),
        nodes=nodes,
    )
    if answer is None:
        raise AssertionError("a failed input gives a refusal")
    return str(answer.result.error_msg)


@pytest.mark.anyio
async def test_two_outputs_of_one_input_are_one_dependency() -> None:
    """`addWaiteeDerivedPath` makes one goal for `in.drv^out,dev`. nixidae/nixidae#1005."""
    message = await _count_under_keep_going([(IN_DRV, ()), (IN_DRV, ())])

    assert f"{RED}1 dependency failed{NORMAL}" in message


@pytest.mark.anyio
async def test_two_inputs_are_two_dependencies() -> None:
    message = await _count_under_keep_going([(IN_DRV, ()), (OTHER_DRV, ())])

    assert f"{RED}2 dependencies failed{NORMAL}" in message
