"""The one place that chooses who writes the store: pynixd's engine or the Nix daemon.

A write states what is known before a byte of its body arrives (`Write`):
a NAR or a dump has no replay, so the choice comes first. The op's route
(`op_routes.py`) decides whether the engine may take it at all. `REASONS`
then gives, in order, what still sends it to the daemon. The test stores, a
deployment and an experiment differ only in the routes they load and the
table the router holds. `docs/design/store-engine.md`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import structlog

from nix_daemon_protocol.operations import STANDARD_OPERATIONS

from . import metrics
from .constants import proto
from .content_address import ContentAddress, Method, content_addressed_path, parse_method_algo
from .derived_path import check_output_name
from .op_routes import OpRoute

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from nix_daemon_protocol.valid_path_info import ValidPathInfo

    from .store.add import ImportSettings
    from .store.engine import StoreEngine

log = structlog.get_logger(__name__)

_OP_CODES = {op.name: op.code for op in STANDARD_OPERATIONS}

FRAMED_NAR = proto(1, 23)
"""`daemon.cc:943`: from 1.23 the NAR of AddToStoreNar comes framed."""

FRAMED_DUMP = proto(1, 25)
"""`daemon.cc:406`: from 1.25 AddToStore names its method and references, and frames the dump."""


@dataclass(frozen=True)
class Dump:
    """The header of an AddToStore dump."""

    name: str
    method_algo: str
    references: frozenset[str]


@dataclass(frozen=True)
class Write:
    """One write as the router sees it. *op* names the route it follows.

    An op of several paths asks once with no *info*, and once per path with it.
    """

    op: str
    version: int
    repair: bool = False
    info: ValidPathInfo | None = None
    dump: Dump | None = None


@dataclass(frozen=True)
class Facts:
    """What the reasons read of the store."""

    writable: bool
    settings: ImportSettings


def _database(write: Write, facts: Facts) -> str | None:
    return None if facts.writable else "the store database is not writable"


def _framing(write: Write, facts: Facts) -> str | None:
    if write.dump is not None:
        return "the client sends the dump unframed" if write.version < FRAMED_DUMP else None
    return "the client sends the NAR unframed" if write.version < FRAMED_NAR else None


def _repair(write: Write, facts: Facts) -> str | None:
    return "repair" if write.repair else None


def _auto_optimise(write: Write, facts: Facts) -> str | None:
    return "auto-optimise-store" if facts.settings.auto_optimise_store else None


def _fsync(write: Write, facts: Facts) -> str | None:
    return "fsync-store-paths" if facts.settings.fsync_store_paths else None


def _git_path(write: Write, facts: Facts) -> str | None:
    if write.info is not None and str(write.info.info.ca).startswith("fixed:git:"):
        return "a Git content address"
    return None


def _dump(write: Write, facts: Facts) -> str | None:
    """A dump whose refusal or hash pynixd does not reproduce: Nix's message comes after `startWork`."""
    dump = write.dump
    if dump is None:
        return None
    try:
        method, algo = parse_method_algo(dump.method_algo)
        check_output_name(dump.name)
        # The digest's value does not change whether Nix makes a path of it.
        content_addressed_path(ContentAddress(method, algo, b""), dump.name, dump.references, "/nix/store")
    except ValueError as error:
        return str(error)
    if method is Method.GIT:
        return "a Git content address"
    return None


def pynixd_import(info: ValidPathInfo) -> Write:
    """A NAR pynixd itself fetched or received: a substitution, or the HTTP cache's upload.

    It follows AddToStoreNar's route, so a deployment that keeps that op on
    the daemon keeps these imports there too.
    """
    return Write(op="AddToStoreNar", version=FRAMED_NAR, info=info)


REASONS: tuple[Callable[[Write, Facts], str | None], ...] = (
    _database,
    _framing,
    _repair,
    _auto_optimise,
    _fsync,
    _git_path,
    _dump,
)
"""What sends a write the engine may take to the daemon, first match wins."""


def first_reason(
    write: Write, facts: Facts, reasons: Sequence[Callable[[Write, Facts], str | None]] = REASONS
) -> str | None:
    for reason in reasons:
        why = reason(write, facts)
        if why is not None:
            return why
    return None


@dataclass
class WriteRouter:
    """Chooses the engine or the daemon for each write of one local store."""

    routes: Mapping[int, OpRoute]
    engine: StoreEngine
    store_id: str
    reasons: Sequence[Callable[[Write, Facts], str | None]] = field(default=REASONS)

    def route(self, op: str) -> OpRoute:
        return self.routes.get(_OP_CODES[op], OpRoute.daemon)

    async def why_the_daemon(self, write: Write) -> str | None:
        """Why the daemon takes *write*, or `None` when the engine does."""
        if self.route(write.op) is not OpRoute.pynixd:
            return f"{write.op} takes the daemon route"
        facts = Facts(writable=self.engine.writable, settings=await self.engine.settings())
        return first_reason(write, facts, self.reasons)

    async def engine_takes(self, write: Write) -> bool:
        """Whether the engine takes *write*. A write on the `pynixd` route that falls through is logged and counted."""
        if self.route(write.op) is not OpRoute.pynixd:
            return False
        reason = await self.why_the_daemon(write)
        if reason is None:
            return True
        subject = str(write.info.path) if write.info is not None else write.dump.name if write.dump else None
        log.info("write_fallthrough", op=write.op, subject=subject, reason=reason)
        metrics.OP_FALLTHROUGH.labels(store_id=self.store_id, op=write.op).inc()
        return False
