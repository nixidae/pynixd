"""pynixd gha-cache wait — return once the GitHub Actions cache has every queued upload."""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio

from ..daemon_extensions.pynixd_gha_cache_wait import PynixdGhaCacheWaitRequest
from .base import add_socket_argument, load_settings, open_pynixd, setup_logging

if TYPE_CHECKING:
    import argparse


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register the ``gha-cache`` subcommand on the root argument parser."""
    parser = subparsers.add_parser("gha-cache", help="The GitHub Actions cache store")
    actions = parser.add_subparsers(dest="gha_cache_action", required=True)
    wait = actions.add_parser("wait", help="Wait for every queued upload; print what each cache did as JSON")
    add_socket_argument(wait)
    wait.set_defaults(func=wait_main)


def wait_main(args: argparse.Namespace) -> None:
    anyio.run(_wait_main, args)


async def _wait_main(args: argparse.Namespace) -> None:
    settings = load_settings()
    setup_logging(settings)
    async with open_pynixd(settings, args.socket, PynixdGhaCacheWaitRequest.name) as store:
        resp = await store.execute(PynixdGhaCacheWaitRequest())
    print(resp.payload)  # noqa: T201
