"""pynixd gc — trigger garbage collection on stores."""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
import structlog

from ..daemon_extensions.pynixd_collect_garbage import PynixdCollectGarbageRequest
from ..serde.protocol import PynixdGCAction
from .base import add_socket_argument, load_settings, open_pynixd, setup_logging

if TYPE_CHECKING:
    import argparse


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register the ``gc`` subcommand on the root argument parser."""
    parser = subparsers.add_parser("gc", help="Trigger garbage collection on stores")
    parser.add_argument(
        "--store",
        help="Only run GC on this store (default: all stores)",
        default=None,
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually run GC (default is dry-run)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Delete at most N paths, weight order first (default: the whole plan)",
    )
    parser.add_argument(
        "--show-paths",
        action="store_true",
        help="List every planned path, sorted (default: count and bytes only)",
    )
    add_socket_argument(parser)
    parser.set_defaults(func=gc_main)


def gc_main(args: argparse.Namespace) -> None:
    """Entry point for ``pynixd gc`` — connect to daemon and issue a GC request."""
    anyio.run(_gc_main, args)


async def _gc_main(args: argparse.Namespace) -> None:
    settings = load_settings()
    setup_logging(settings)
    log = structlog.get_logger(__name__)

    action = PynixdGCAction.EXECUTE if args.execute else PynixdGCAction.DRY_RUN
    if args.limit is not None and args.limit < 0:
        parser_error = f"--limit takes a count, and {args.limit} is not one"
        raise SystemExit(f"pynixd gc: error: {parser_error}")

    store = await open_pynixd(settings, args.socket, PynixdCollectGarbageRequest.name)

    try:
        resp = await store.execute(
            PynixdCollectGarbageRequest(
                action=action,
                has_limit=args.limit is not None,
                limit=args.limit or 0,
            )
        )
    except Exception:
        log.exception("gc_failed")
        raise
    finally:
        await store.close()

    for msg in resp.logs.messages:
        text = getattr(msg, "text", None) or getattr(msg, "msg", None)
        if text:
            print(text)  # noqa: T201

    label = "dry-run" if action == PynixdGCAction.DRY_RUN else "gc"
    if args.show_paths:
        # Sorted, not weight order: the response carries the set, and the
        # weights stay on the daemon. Per-path sizes would ride the wire;
        # that change is bigger than this flag.
        for path in sorted(str(path) for path in resp.store_paths):
            print(path)  # noqa: T201
    if resp.store_paths:
        if action == PynixdGCAction.DRY_RUN:
            # A sum of logical sizes: sparse files count their holes and
            # shared files count per link, the way the daemon counts. An
            # upper bound on what deleting frees, never a measurement.
            print(f"dry-run: {len(resp.store_paths)} paths, up to {resp.bytes} bytes")  # noqa: T201
        else:
            print(f"gc: {len(resp.store_paths)} paths, {resp.bytes} bytes freed")  # noqa: T201
    else:
        print(f"{label}: no paths eligible")  # noqa: T201
