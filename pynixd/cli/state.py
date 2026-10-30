"""pynixd state — daemon state, as tables on a terminal and JSON otherwise."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import anyio
import structlog

from ..daemon_extensions.pynixd_state import PynixdStateRequest
from ..state import KNOWN_SECTIONS
from ..state_models import StateReport
from .base import add_socket_argument, load_settings, open_pynixd, setup_logging

if TYPE_CHECKING:
    import argparse


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register the ``state`` subcommand on the root argument parser."""
    parser = subparsers.add_parser("state", help="Daemon state: tables on a terminal, JSON otherwise")
    parser.add_argument(
        "--section",
        action="append",
        default=[],
        choices=KNOWN_SECTIONS,
        help="Show only this section (repeatable; default: all)",
    )
    parser.add_argument(
        "--federated",
        action="store_true",
        help="Ask each configured store for its own state too",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON even on a terminal (it is the default when piped)",
    )
    add_socket_argument(parser)
    parser.set_defaults(func=state_main)


def state_main(args: argparse.Namespace) -> None:
    """Entry point for ``pynixd state`` — connect to daemon and ask."""
    anyio.run(_state_main, args)


async def _state_main(args: argparse.Namespace) -> None:
    settings = load_settings()
    setup_logging(settings)
    log = structlog.get_logger(__name__)

    async with open_pynixd(settings, args.socket, PynixdStateRequest.name) as store:
        try:
            resp = await store.execute(
                PynixdStateRequest(wants=args.section, federated=args.federated),
            )
        except Exception:
            log.exception("state_failed")
            raise

    report = StateReport.model_validate_json(resp.payload)
    if args.json or not sys.stdout.isatty():
        print(report.model_dump_json(indent=2, exclude_none=True))  # noqa: T201
        return
    # Lazy, against CLAUDE.md's rule, by the user's decision on 2026-10-12:
    # `__main__` imports every CLI module at daemon start, and Rich costs
    # 25-30 ms of import there for a view only this command shows.
    from .render import render_state  # noqa: PLC0415

    render_state(report)
