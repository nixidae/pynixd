"""`pynixd state` joins a build's requests to the clients that run them.

A build row names its requests by id; a client row names the request it runs
now and the client's pid. Diagnosing a stuck build needed root and
`sys.remote_exec` for that join. nixidae/nixidae#767.
"""

from __future__ import annotations

import time
from types import SimpleNamespace
from typing import Any, cast

from rich.console import Console

from nix_daemon_protocol.ids import RequestId
from pynixd.cli.render import render_state
from pynixd.state import _clients_section
from pynixd.state_models import BuildRow, ClientRow, StateReport


def _proxy(*, pid: int | None, request_id: RequestId | None) -> SimpleNamespace:
    return SimpleNamespace(
        transport="unix",
        username="lillecarl",
        pid=pid,
        request_id=request_id,
        connected_at=time.monotonic(),
        current_op=("BuildPathsRequest", time.monotonic()),
    )


def test_a_client_names_its_pid_and_its_request() -> None:
    ctx = SimpleNamespace(sessions=SimpleNamespace(proxies=lambda: [_proxy(pid=4242, request_id=RequestId(134))]))

    [row] = _clients_section(cast("Any", ctx))

    assert (row.pid, row.request) == (4242, 134)


def test_a_client_between_requests_names_none() -> None:
    ctx = SimpleNamespace(sessions=SimpleNamespace(proxies=lambda: [_proxy(pid=None, request_id=None)]))

    [row] = _clients_section(cast("Any", ctx))

    assert (row.pid, row.request) == (None, None)


def test_the_tables_carry_the_join_and_the_failures() -> None:
    report = StateReport(
        builds=[
            BuildRow(
                id=344,
                drv="/nix/store/x.drv",
                state="pending",
                age_s=1,
                requests={134: 16},
                store_failures={"sg1-n1": 1},
            )
        ],
        clients=[ClientRow(transport="unix", user="lillecarl", pid=4242, request=134, connected_s=1)],
    )
    console = Console(record=True, width=240)
    render_state(report, console)
    text = console.export_text()

    assert "4242" in text
    assert "sg1-n1" in text
