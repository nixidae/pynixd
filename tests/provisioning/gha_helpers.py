"""What the GitHub Actions phases share: the fake's view of its runs. nixidae/nixidae#1473."""

from __future__ import annotations

import json
import shlex
from typing import TYPE_CHECKING, Any

from cloud_helpers import until

if TYPE_CHECKING:
    from collections.abc import Mapping

    from vivarium_runner import Machine

RUN_NAME_PREFIX = "pynixd-builder "


def fake(settings: Mapping[str, str], path: str) -> str:
    return f"http://127.0.0.1:{settings['gha_port']}{path}"


async def runs(vm: Machine, settings: Mapping[str, str]) -> list[dict[str, Any]]:
    """Every run the fake dispatched, oldest first, with its inputs and its guest."""
    return list(json.loads(await vm.succeed(f"curl --silent --fail {fake(settings, '/_fake/runs')}")))


def pool_of(run: Mapping[str, Any]) -> str:
    return str(json.loads(str(run["display_title"]).removeprefix(RUN_NAME_PREFIX))["pynixd-pool"])


async def pool_runs(vm: Machine, settings: Mapping[str, str], pool: str) -> list[dict[str, Any]]:
    return [run for run in await runs(vm, settings) if pool_of(run) == pool]


async def completed(vm: Machine, settings: Mapping[str, str], run_id: int, timeout: float) -> dict[str, Any]:
    """Run *run_id* once it has completed."""
    found: dict[str, Any] = {}

    async def done() -> bool:
        nonlocal found
        [found] = [run for run in await runs(vm, settings) if run["id"] == run_id]
        return found["status"] == "completed"

    await until(done, timeout)
    return found


async def refuse_cancel(vm: Machine, settings: Mapping[str, str], refuse: bool) -> None:
    body = shlex.quote(json.dumps({"refuse": refuse}))
    await vm.succeed(
        "curl --silent --fail --header 'Content-Type: application/json' "
        f"--data {body} {fake(settings, '/_fake/refuse-cancel')}"
    )


async def set_ticket_key(vm: Machine, settings: Mapping[str, str], key: str) -> None:
    body = shlex.quote(json.dumps({"PYNIXD_TICKET_KEY": key}))
    await vm.succeed(
        f"curl --silent --fail --header 'Content-Type: application/json' --data {body} {fake(settings, '/_fake/secrets')}"
    )
