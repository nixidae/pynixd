"""Substitution goals backed by configured stores."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar

import structlog

from ..serde import BuildResultStatus, IsValidPathRequest
from ..store_path import StorePath
from .goal import ExecutionGoal
from .results import GoalResult, goal_failure, goal_success

if TYPE_CHECKING:
    from nix_daemon_protocol.valid_path_info import ValidPathInfo

    from .engine import GoalEngine

log = structlog.get_logger(__name__)

UNSIGNED_REFUSAL = "lacks a signature by a trusted key"
"""The words of `LocalStore::addToStore` when no trusted key signed a path (`local-store.cc:1049`)."""


@dataclass(frozen=True)
class SubstituteAttempt:
    """Outcome of a single path substitution attempt."""

    found: bool
    result: GoalResult


class SubstitutePathGoal(ExecutionGoal[SubstituteAttempt]):
    """Substitute one store path and its reference closure."""

    may_reach_a_root_goal: ClassVar[bool] = False
    """A substitute goal reaches substitute goals alone, and no root goal.

    `Goal.may_reach_a_root_goal` gives the rule. Issue nixidae/nixidae#246.
    """

    def __init__(self, engine: GoalEngine, path: StorePath, substituter_ids: tuple[str, ...]) -> None:
        """Initialize the goal with the path to substitute and eligible substituter IDs."""
        super().__init__(engine)
        self.path = path
        self.substituter_ids = substituter_ids

    async def _run(self) -> SubstituteAttempt:
        log.debug("substitute_path_start", path=str(self.path), substituters=self.substituter_ids)
        if await self._is_valid_local_path(self.path):
            log.debug("substitute_path_already_valid", path=str(self.path))
            result = goal_success()
            result.produced_paths.add(self.path)
            return SubstituteAttempt(found=True, result=result)

        scheduler = self.engine.ctx.scheduler
        if scheduler is None:
            return SubstituteAttempt(
                found=False,
                result=goal_failure(
                    "pynixd: substitution requires a configured scheduler",
                    BuildResultStatus.UNKNOWN,
                ),
            )

        candidate = await scheduler.substitution_queue.get_substituter(self.path)
        if candidate is None:
            log.info("substitute_path_miss", path=str(self.path))
            return SubstituteAttempt(
                found=False,
                result=goal_failure(
                    f"pynixd: no substituter has path: {self.path}",
                    BuildResultStatus.UNKNOWN,
                ),
            )

        reference_goals: list[SubstitutePathGoal] = []
        for reference in sorted(_references(candidate.path_info), key=str):
            if reference == self.path:
                continue
            reference_goals.append(await self.engine.get_substitute_path_goal(reference, self.substituter_ids))
        log.info(
            "substitute_path_hit",
            path=str(self.path),
            store_id=candidate.store.store_id,
            references=len(reference_goals),
        )

        reference_results = await self.run_children(reference_goals)
        for reference_result in reference_results:
            if not reference_result.found:
                # No substituter has the reference, so none can provide this
                # path: Nix's `ecNoSubstituters`, after which the derivation
                # builds (`derivation-goal.cc:124`).
                return SubstituteAttempt(
                    found=False,
                    result=goal_failure(
                        f"pynixd: cannot substitute {self.path}; missing reference",
                        BuildResultStatus.UNKNOWN,
                    ),
                )
            if not _result_succeeded(reference_result.result):
                return SubstituteAttempt(found=True, result=reference_result.result)

        log.info("substitute_path_import_start", path=str(self.path), store_id=candidate.store.store_id)
        import_result = await scheduler.substitution_queue.substitute(self.path)
        if not import_result.substituted:
            # A substitute the daemon refused for its signature is one Nix
            # never takes (`substitution-goal.cc:116`): no substituter, not a
            # failed copy.
            unsigned = UNSIGNED_REFUSAL in (import_result.error or "")
            return SubstituteAttempt(
                found=not unsigned,
                result=goal_failure(
                    f"pynixd: failed to substitute {self.path}: {import_result.error}",
                    BuildResultStatus.TRANSIENT_FAILURE,
                ),
            )

        result = goal_success()
        log.info("substitute_path_import_done", path=str(self.path), store_id=candidate.store.store_id)
        result.produced_paths.add(self.path)
        result.resolved_outputs["out"] = self.path
        return SubstituteAttempt(found=True, result=result)

    async def _is_valid_local_path(self, path: StorePath) -> bool:
        response = await self.engine.ctx.local_store.execute(
            IsValidPathRequest(path=StorePath(path=str(path))), mark=False
        )
        return bool(response.valid)


def substituter_fingerprint(substituter_ids: tuple[str, ...]) -> str:
    """Return a deterministic hash fingerprint for a set of substituter IDs."""
    payload = "\0".join(substituter_ids).encode()
    return hashlib.sha256(payload).hexdigest()


def _references(info: ValidPathInfo) -> set[StorePath]:
    return {StorePath(str(path)) for path in info.info.references}


def _result_succeeded(result: GoalResult) -> bool:
    try:
        return BuildResultStatus(result.result.status).is_success
    except ValueError:
        return False
