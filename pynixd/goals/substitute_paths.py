"""EnsurePath (op 10) and QueryValidPaths with `substitute` (op 31), on pynixd's substitution goals.

`Store::ensurePath` (`build/entry-points.cc:99`) runs one path substitution
goal. `Store::substitutePaths` (`store-api.cc:724`), which the daemon runs
for a QueryValidPaths with `substitute` (`daemon.cc:334`), runs one for each
missing path that is not a derivation. Both are the same work, so the route
of EnsurePath decides both: on `pynixd`, the engine's `SubstitutePathGoal`
fetches, and the NAR goes in through the route of AddToStoreNar
(`SubstitutionQueue.substitute`). Outside the test stores the route is
`daemon`, and the daemon substitutes as before.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from nix_daemon_protocol.ensure_path import EnsurePathRequest, EnsurePathResponse
from nix_daemon_protocol.logs import LogNext
from pynixd_store.hintfmt import magenta
from pynixd_store.store_path import StorePath

from ..exceptions import BackendError
from ..op_routes import OpRoute
from ..store.local_db import LocalDBStore
from ..trust import warning_text
from .query_missing import daemon_substitutes
from .results import result_succeeded

if TYPE_CHECKING:
    from collections.abc import Iterable

    from nix_daemon_protocol.query_valid_paths import QueryValidPathsRequest

    from ..connection import ClientConn
    from ..store import Store
    from ..substitution_queue import SubstitutionQueue
    from .engine import GoalEngine, Hold
    from .substitute import SubstituteAttempt

log = structlog.get_logger(__name__)

ENSURE_PATH_OP = EnsurePathRequest.op


async def pynixd_substitutes(store: Store, client: ClientConn | None, queue: SubstitutionQueue | None) -> bool:
    """Whether pynixd, and not the daemon, substitutes into *store* for *client*.

    The route of EnsurePath must be `pynixd`, and pynixd's backends must
    stand for every substituter the client can reach (`daemon_substitutes`).
    """
    if not isinstance(store, LocalDBStore) or store.routes.get(ENSURE_PATH_OP) is not OpRoute.pynixd:
        return False
    return queue is not None and not await daemon_substitutes(client, queue)


def no_substituter(path: object) -> str:
    """The failure of a substitution goal that got no path (`substitution-goal.cc:160`)."""
    return f"path '{magenta(path)}' is required, but there is no substituter that can build it"


async def ensure_path(
    engine: GoalEngine, request: EnsurePathRequest, client: ClientConn | None, hold: Hold | None
) -> EnsurePathResponse:
    """`Store::ensurePath`: substitute the path, or fail with the words of `tryThrowBuildError`.

    A miss and a failed fetch end in the same error, as in Nix. A failed
    fetch logs its cause first (`substitution-goal.cc:275`).
    """
    path = StorePath(str(request.path))
    attempt = (await substitute(engine, [path], client, hold))[path]
    if not result_succeeded(attempt.result.result):
        raise BackendError(no_substituter(path))
    return EnsurePathResponse(value=1)


async def substitute_paths(
    engine: GoalEngine, request: QueryValidPathsRequest, client: ClientConn | None, hold: Hold | None
) -> QueryValidPathsRequest:
    """`Store::substitutePaths`, and the query that follows it, which a local store answers without the flag.

    `LocalStore::queryValidPaths` ignores `substitute` (`local-store.cc:844`).
    A failure is a warning and not an error (`store-api.cc:738`): the answer
    then leaves that path out.
    """
    wanted = [StorePath(str(path)) for path in request.paths if not str(path).endswith(".drv")]
    attempts = await substitute(engine, wanted, client, hold)
    failed = sorted(str(path) for path, attempt in attempts.items() if not _missed_or_done(attempt))
    if failed and client is not None:
        # `Store::buildPaths` (`build/entry-points.cc:36`) throws the one
        # failure itself, or one error that names every failed path.
        names = ", ".join(f"'{path}'" for path in failed)
        text = no_substituter(failed[0]) if len(failed) == 1 else f"build of {magenta(names)} failed"
        await client.send(LogNext(text=warning_text(text)))
    return request.model_copy(update={"substitute": 0})


async def substitute(
    engine: GoalEngine, paths: Iterable[StorePath], client: ClientConn | None, hold: Hold | None
) -> dict[StorePath, SubstituteAttempt]:
    """Run the goals, which write each copy and the cause of each failed fetch to *client* (`substitution-goal.cc:275`).

    A path that no substituter has logs nothing: Nix's goal ends quietly
    with `ecNoSubstituters`.
    """
    options = client.options if client is not None else None
    substitutes = options is None or bool(options.use_substitutes)
    ids = await engine.substituter_ids_for(client)
    attempts = await engine.substitute_paths(paths, hold=hold, substitutes=substitutes, ids=ids, client=client)
    for path, attempt in attempts.items():
        if not _missed_or_done(attempt):
            log.info("substitute_failed", path=str(path), reason=attempt.result.result.error_msg)
    return attempts


def _missed_or_done(attempt: SubstituteAttempt) -> bool:
    """A path no substituter has is no failure: `Store::queryMissing` calls it unknown (`misc.cc:311`)."""
    return not attempt.found or result_succeeded(attempt.result.result)
