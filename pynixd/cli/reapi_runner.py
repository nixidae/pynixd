"""pynixd reapi-runner — a Buildbarn runner that realises a Nix closure first."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import anyio

from ..reapi_runner import serve
from .base import load_settings, setup_logging

if TYPE_CHECKING:
    import argparse


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register the ``reapi-runner`` subcommand on the root argument parser."""
    parser = subparsers.add_parser(
        "reapi-runner",
        help="Serve bb_worker as a Buildbarn runner: make the closure of an action's program valid, then call bb_runner",
    )
    parser.add_argument("--listen", type=Path, required=True, help="Unix socket bb_worker calls")
    parser.add_argument("--upstream", type=Path, required=True, help="Unix socket of bb_runner")
    parser.add_argument(
        "--daemon",
        type=Path,
        default=Path("/nix/var/nix/daemon-socket/socket"),
        help="Nix daemon socket that makes the closure valid (default: %(default)s)",
    )
    parser.set_defaults(func=reapi_runner_main)


def reapi_runner_main(args: argparse.Namespace) -> None:
    """Entry point for ``pynixd reapi-runner``."""
    setup_logging(load_settings())
    anyio.run(serve, args.listen, args.upstream, args.daemon)
