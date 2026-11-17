"""GitHub Actions jobs as builders: one `workflow_dispatch` run per machine. nixidae/nixidae#1473.

The run's job installs pynixd from the installer tarball and registers as a
builder by its join ticket. The template's `runs_on` picks the runner, so any
service that runs GitHub Actions jobs serves. The job holds itself open until
the controller cancels the run, or its own guard finds it idle.

A run has no tags. The workflow's `run-name` carries the launch's tags as
JSON after `RUN_NAME_PREFIX`, and the runs list answers it as
`display_title`, so a restarted provisioner finds its runs. Inputs show on the
run's page, so the join ticket travels sealed under the repository's
`PYNIXD_TICKET_KEY`, bound to those tags.
"""

from __future__ import annotations

import datetime
import json
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Final

import aiohttp
import anyio
from yarl import URL

from .provider import Machine, MachineState, NotFound, ProviderError
from .ticket import seal, ticket_key

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ..config import GhaSettings, GhaTemplateSettings

RUN_NAME_PREFIX: Final = "pynixd-builder "
"""The workflow's `run-name` is this, then the `tags` input."""

_STATES: Final = {
    "requested": MachineState.PENDING,
    "queued": MachineState.PENDING,
    "pending": MachineState.PENDING,
    "waiting": MachineState.PENDING,
    "in_progress": MachineState.RUNNING,
    "completed": MachineState.ENDED,
}
_CLEAN_ENDS: Final = frozenset({"success", "cancelled"})
_TIMEOUT: Final = aiohttp.ClientTimeout(total=60)
_PER_PAGE: Final = 100

type Run = dict[str, Any]
"""A workflow run as GitHub's REST API answers it: its schema types it, not Python."""


def tags_text(tags: Mapping[str, str]) -> str:
    """The `tags` input: the run name's JSON and the sealed ticket's context, one spelling for both."""
    return json.dumps(dict(sorted(tags.items())), separators=(",", ":"))


def run_tags(display_title: str) -> dict[str, str] | None:
    """The tags a run name carries; `None` for a run this provider did not dispatch."""
    if not display_title.startswith(RUN_NAME_PREFIX):
        return None
    try:
        tags = json.loads(display_title.removeprefix(RUN_NAME_PREFIX))
    except ValueError:
        return None
    if not isinstance(tags, dict) or not all(isinstance(v, str) for v in tags.values()):
        return None
    return {str(key): value for key, value in tags.items()}


def machine(run: Run) -> Machine | None:
    """A machine from a workflow run; `None` for a run with no tags."""
    tags = run_tags(str(run.get("display_title") or ""))
    if tags is None:
        return None
    state = _STATES.get(str(run["status"]), MachineState.PENDING)
    conclusion = run.get("conclusion")
    error = None
    if state is MachineState.ENDED and conclusion not in _CLEAN_ENDS:
        error = f"the run ended {conclusion}"
    return Machine(provider_id=str(run["id"]), state=state, tags=tags, error=error)


def dispatch_inputs(template: GhaTemplateSettings, tags: Mapping[str, str], sealed: str) -> dict[str, str]:
    """The `workflow_dispatch` inputs of one launch. `ci/workflows/builder.nix` declares the same names."""
    return {
        "tags": tags_text(tags),
        "ticket": sealed,
        "runs_on": json.dumps(template.runs_on),
        "installer": template.installer,
        "idle_seconds": f"{template.idle_seconds:g}",
        "bootstrap_minutes": str(template.bootstrap_minutes),
    }


def created_since(now: datetime.datetime, seconds: float) -> str:
    """The runs list's `created` filter, to the day: the URL stays the same all day, so its ETag answers."""
    return f">={(now - datetime.timedelta(seconds=seconds)).date().isoformat()}"


class GhaProvider:
    name = "gha"

    def __init__(self, settings: GhaSettings) -> None:
        self._settings = settings
        self._base = f"{settings.api_url.rstrip('/')}/repos/{settings.repository}/actions"
        self._http = aiohttp.ClientSession(
            timeout=_TIMEOUT,
            headers={"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"},
        )
        self._pages: dict[str, tuple[str, Run]] = {}
        """Each runs page by URL with its ETag: GitHub counts no `304 Not Modified` against the rate limit."""

    async def _token(self) -> str:
        return (await anyio.Path(self._settings.token_file).read_text()).strip()

    async def _call(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str] | None = None,
        **kwargs: Any,  # noqa: ANN401 -- aiohttp's request options
    ) -> tuple[int, Any, str | None]:  # noqa: ANN401 -- the API's JSON, read by each caller
        """One API call: its status, its JSON body (`None` when it has none) and its ETag."""
        sent = {"Authorization": f"Bearer {await self._token()}", **(headers or {})}
        try:
            async with self._http.request(method, url, headers=sent, **kwargs) as response:
                text = await response.text()
                return response.status, json.loads(text) if text else None, response.headers.get("ETag")
        except (aiohttp.ClientError, OSError, TimeoutError, ValueError) as error:
            raise ProviderError(f"gha: {method} {url}: {error}") from error

    def _template(self, name: str) -> GhaTemplateSettings:
        try:
            return self._settings.templates[name]
        except KeyError:
            raise NotFound(f"gha: no template {name!r}") from None

    async def launch(self, template: str, tags: Mapping[str, str], user_data: str) -> Machine:
        spec = self._template(template)
        key = ticket_key(await anyio.Path(self._settings.ticket_key_file).read_text())
        sealed = seal(user_data, key, tags_text(tags))
        status, body, _ = await self._call(
            "POST",
            f"{self._base}/workflows/{self._settings.workflow}/dispatches",
            json={"ref": self._settings.ref, "inputs": dispatch_inputs(spec, tags, sealed), "return_run_details": True},
        )
        if status == HTTPStatus.NOT_FOUND:
            raise NotFound(f"gha: no workflow {self._settings.workflow} in {self._settings.repository}")
        if status != HTTPStatus.OK:
            # A 204 started a run and named none: an API without `return_run_details`. Its guard ends it.
            raise ProviderError(f"gha: dispatch answered {status}: {body}")
        return Machine(provider_id=str(body["workflow_run_id"]), state=MachineState.PENDING, tags=dict(tags))

    async def terminate(self, provider_id: str) -> None:
        status, body, _ = await self._call("POST", f"{self._base}/runs/{provider_id}/cancel")
        # 409: the run has completed. 404: GitHub knows no such run. Neither costs anything.
        if status not in {HTTPStatus.ACCEPTED, HTTPStatus.CONFLICT, HTTPStatus.NOT_FOUND}:
            raise ProviderError(f"gha: cancel {provider_id} answered {status}: {body}")

    async def machines(self, tags: Mapping[str, str]) -> list[Machine]:
        created = created_since(datetime.datetime.now(datetime.UTC), self._settings.history_seconds)
        found: list[Machine] = []
        pages: dict[str, tuple[str, Run]] = {}
        page = 1
        while True:
            runs = await self._runs_page(created, page, pages)
            for run in runs:
                one = machine(run)
                if one is not None and all(one.tags.get(key) == value for key, value in tags.items()):
                    found.append(one)
            if len(runs) < _PER_PAGE:
                break
            page += 1
        # Only this listing's pages: yesterday's URLs never come back.
        self._pages = pages
        return found

    async def _runs_page(self, created: str, page: int, pages: dict[str, tuple[str, Run]]) -> list[Run]:
        url = str(
            URL(f"{self._base}/workflows/{self._settings.workflow}/runs").with_query(
                event="workflow_dispatch", created=created, per_page=_PER_PAGE, page=page
            )
        )
        cached = self._pages.get(url)
        status, body, etag = await self._call("GET", url, {} if cached is None else {"If-None-Match": cached[0]})
        if status == HTTPStatus.NOT_MODIFIED and cached is not None:
            pages[url] = cached
            body = cached[1]
        elif status != HTTPStatus.OK:
            raise ProviderError(f"gha: listing runs answered {status}: {body}")
        elif etag is not None:
            pages[url] = (etag, body)
        return list(body["workflow_runs"])

    async def close(self) -> None:
        await self._http.close()
