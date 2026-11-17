"""GitHub's Actions REST API in front of vivarium's cloud: the real `GhaProvider` dispatches guests. nixidae/nixidae#1473.

One repository and one workflow: the evaluated value of
`ci/workflows/builder.nix`, read from a JSON file. A dispatch checks the
inputs as GitHub does, evaluates the job's expressions (`inputs.*`,
`secrets.*`, `fromJSON`), and boots the vivarium template that the job's first
`runs-on` label names, with `gha_runner.py` as its user data. That runner
reports the job's conclusion here; a cancel deletes the guest.

`python fake_gha.py PORT REPORT_HOST WORKFLOW_JSON SECRETS_JSON TOKEN_FILE RUNNER_PY`,
on a guest with `vivarium.cloud = true`. `/_fake/*` is the test's own way in.
"""

from __future__ import annotations

import datetime
import hashlib
import itertools
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import aiohttp
from aiohttp import web

CLOUD: Final = "/run/vivarium/cloud.sock"
REPOSITORY: Final = "nixidae/nixidae"
WORKFLOW: Final = "pynixd-builder.yml"
EXPRESSION: Final = re.compile(r"\$\{\{\s*(.+?)\s*\}\}")
_STATUS: Final = {"pending": "queued", "running": "in_progress", "terminated": "completed", "failed": "completed"}

type Json = Any
"""The workflow value and the API's bodies: GitHub's schema types them, not Python."""


class ExpressionError(ValueError):
    """An expression this fake does not evaluate: the workflow needs more of GitHub than the fake has."""


def evaluate(expression: str, context: dict[str, dict[str, str]]) -> Json:
    if inner := re.fullmatch(r"fromJSON\((.+)\)", expression):
        return json.loads(str(evaluate(inner[1], context)))
    if name := re.fullmatch(r"(inputs|secrets)\.(\w+)", expression):
        return context[name[1]].get(name[2], "")
    raise ExpressionError(f"the fake evaluates no {expression!r}")


def render(value: Json, context: dict[str, dict[str, str]]) -> Json:
    """A workflow value with its expressions evaluated. A whole-value expression keeps its type, as GitHub's does."""
    if not isinstance(value, str):
        return value
    if whole := EXPRESSION.fullmatch(value):
        return evaluate(whole[1], context)
    return EXPRESSION.sub(lambda part: str(evaluate(part[1], context)), value)


def checked_inputs(declared: dict[str, Json], given: dict[str, Json]) -> dict[str, str]:
    """The dispatch's inputs with defaults, or `ValueError` with GitHub's reason."""
    if unexpected := sorted(set(given) - set(declared)):
        raise ValueError(f"Unexpected inputs provided: {unexpected}")
    inputs = {}
    for name, spec in declared.items():
        if name in given:
            inputs[name] = str(given[name])
        elif spec.get("required"):
            raise ValueError(f"Required input '{name}' not provided")
        else:
            inputs[name] = str(spec.get("default", ""))
    return inputs


def job_of(workflow: Json, context: dict[str, dict[str, str]], report: str) -> tuple[str, Json]:
    """The template the job runs on and `gha_runner.py`'s job: each step's command, environment and timeout."""
    [job] = workflow["jobs"].values()
    labels = render(job["runs-on"], context)
    steps = []
    for step in job["steps"]:
        if "run" not in step or "if" in step or "uses" in step:
            raise ExpressionError(f"the fake runs only plain `run` steps: {step.get('name')!r}")
        steps.append(
            {
                "name": step.get("name", step["run"]),
                "run": step["run"],
                "env": {key: str(render(value, context)) for key, value in (step.get("env") or {}).items()},
                "timeout_minutes": render(step.get("timeout-minutes"), context),
            }
        )
    return str(labels[0] if isinstance(labels, list) else labels), {"report": report, "steps": steps}


@dataclass
class Run:
    id: int
    display_title: str
    instance: str | None
    created_at: str
    inputs: dict[str, str]
    conclusion: str | None = None
    """What the runner reported, or `cancelled`."""
    status: str = "queued"

    def view(self) -> Json:
        return {
            "id": self.id,
            "name": "pynixd: builder",
            "display_title": self.display_title,
            "event": "workflow_dispatch",
            "status": self.status,
            "conclusion": self.conclusion if self.status == "completed" else None,
            "created_at": self.created_at,
        }


class FakeGitHub:
    def __init__(  # noqa: PLR0913 -- the command line's arguments
        self,
        cloud: aiohttp.ClientSession,
        report: str,
        workflow: Json,
        secrets: dict[str, str],
        token: str,
        runner: str,
    ) -> None:
        self.cloud = cloud
        self.report = report
        self.workflow = workflow
        self.secrets = secrets
        self.refuse_cancel = False
        """GitHub down for the controller: a cancel answers 500."""
        self.token = token
        self.runner = runner
        self.runs: dict[int, Run] = {}
        self.ids = itertools.count(9000)

    def _authorized(self, request: web.Request) -> bool:
        return request.headers.get("Authorization") == f"Bearer {self.token}"

    async def _cloud(self, method: str, path: str, **kwargs: Any) -> tuple[int, Json]:  # noqa: ANN401 -- aiohttp's options
        async with self.cloud.request(method, path, **kwargs) as response:
            return response.status, await response.json()

    def _ours(self, request: web.Request) -> bool:
        info = request.match_info
        return f"{info['owner']}/{info['repo']}" == REPOSITORY and info.get("workflow", WORKFLOW) == WORKFLOW

    async def dispatch(self, request: web.Request) -> web.Response:
        if not self._authorized(request):
            return web.json_response({"message": "Bad credentials"}, status=401)
        if not self._ours(request):
            return web.json_response({"message": "Not Found"}, status=404)
        body = await request.json()
        try:
            inputs = checked_inputs(self.workflow["on"]["workflow_dispatch"]["inputs"], body.get("inputs") or {})
        except ValueError as error:
            return web.json_response({"message": str(error)}, status=422)
        context = {"inputs": inputs, "secrets": self.secrets}
        run_id = next(self.ids)
        template, job = job_of(self.workflow, context, f"{self.report}/_runner/{run_id}/conclusion")
        user_data = f"#!/usr/bin/env python3\n{self.runner}\n\nmain(json.loads({json.dumps(job)!r}))\n"
        status, view = await self._cloud(
            "POST",
            "/v1/instances",
            json={"template": template, "tags": {"github-run": str(run_id)}, "user_data": user_data},
        )
        run = Run(
            id=run_id,
            display_title=str(render(self.workflow["run-name"], context)),
            instance=view["id"] if status < 400 else None,
            created_at=datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
            inputs=inputs,
        )
        if run.instance is None:
            run.status, run.conclusion = "completed", "failure"
        self.runs[run_id] = run
        if not body.get("return_run_details"):
            return web.Response(status=204)
        url = f"https://api.github.com/repos/{REPOSITORY}/actions/runs/{run_id}"
        return web.json_response({"workflow_run_id": run_id, "run_url": url, "html_url": url})

    async def _refresh(self, run: Run) -> None:
        if run.status == "completed" or run.instance is None:
            return
        _, view = await self._cloud("GET", f"/v1/instances/{run.instance}")
        run.status = _STATUS[str(view["state"])]
        if run.status == "completed" and run.conclusion is None:
            run.conclusion = "failure"

    async def list_runs(self, request: web.Request) -> web.Response:
        if not self._authorized(request):
            return web.json_response({"message": "Bad credentials"}, status=401)
        if not self._ours(request):
            return web.json_response({"message": "Not Found"}, status=404)
        query = request.query
        since = query.get("created", ">=0000-00-00").removeprefix(">=")
        per_page, page = int(query.get("per_page", 30)), int(query.get("page", 1))
        chosen = []
        for run in sorted(self.runs.values(), key=lambda one: one.id, reverse=True):
            await self._refresh(run)
            if query.get("event", "workflow_dispatch") == "workflow_dispatch" and run.created_at >= since:
                chosen.append(run.view())
        body = {"total_count": len(chosen), "workflow_runs": chosen[(page - 1) * per_page : page * per_page]}
        etag = f'W/"{hashlib.sha256(json.dumps(body).encode()).hexdigest()}"'
        if request.headers.get("If-None-Match") == etag:
            return web.Response(status=304, headers={"ETag": etag})
        return web.json_response(body, headers={"ETag": etag})

    async def cancel(self, request: web.Request) -> web.Response:
        if not self._authorized(request):
            return web.json_response({"message": "Bad credentials"}, status=401)
        run = self.runs.get(int(request.match_info["id"]))
        if run is None:
            return web.json_response({"message": "Not Found"}, status=404)
        if self.refuse_cancel:
            return web.json_response({"message": "Server Error"}, status=500)
        await self._refresh(run)
        if run.status == "completed":
            return web.json_response({"message": "Cannot cancel a workflow run that is completed."}, status=409)
        if run.instance is not None:
            await self._cloud("DELETE", f"/v1/instances/{run.instance}")
        run.status, run.conclusion = "completed", "cancelled"
        return web.json_response({}, status=202)

    async def conclude(self, request: web.Request) -> web.Response:
        run = self.runs[int(request.match_info["id"])]
        # A job that reports is done: the run completes, and a cancel after it gets GitHub's 409.
        if run.conclusion is None:
            run.status, run.conclusion = "completed", str((await request.json())["conclusion"])
        return web.json_response({})

    async def dispatches(self, request: web.Request) -> web.Response:
        del request
        for run in self.runs.values():
            await self._refresh(run)
        return web.json_response(
            [{**run.view(), "inputs": run.inputs, "instance": run.instance} for run in self.runs.values()]
        )

    async def set_refuse_cancel(self, request: web.Request) -> web.Response:
        self.refuse_cancel = bool((await request.json())["refuse"])
        return web.json_response({})

    async def set_secrets(self, request: web.Request) -> web.Response:
        self.secrets = {str(key): str(value) for key, value in (await request.json()).items()}
        return web.json_response({})


async def app() -> web.Application:
    _, _, report_host, workflow_file, secrets_file, token_file, runner_file = sys.argv
    cloud = aiohttp.ClientSession(
        connector=aiohttp.UnixConnector(path=CLOUD, force_close=True), base_url="http://cloud"
    )
    fake = FakeGitHub(
        cloud,
        f"http://{report_host}:{sys.argv[1]}",
        json.loads(Path(workflow_file).read_text()),
        json.loads(Path(secrets_file).read_text()),
        Path(token_file).read_text().strip(),
        Path(runner_file).read_text(),
    )
    application = web.Application()
    repo = "/repos/{owner}/{repo}/actions"
    application.router.add_post(repo + "/workflows/{workflow}/dispatches", fake.dispatch)
    application.router.add_get(repo + "/workflows/{workflow}/runs", fake.list_runs)
    application.router.add_post(repo + "/runs/{id}/cancel", fake.cancel)
    application.router.add_post("/_runner/{id}/conclusion", fake.conclude)
    application.router.add_get("/_fake/runs", fake.dispatches)
    application.router.add_post("/_fake/secrets", fake.set_secrets)
    application.router.add_post("/_fake/refuse-cancel", fake.set_refuse_cancel)

    async def close(_: web.Application) -> None:
        await cloud.close()

    application.on_cleanup.append(close)
    return application


if __name__ == "__main__":
    web.run_app(app(), host="0.0.0.0", port=int(sys.argv[1]), print=None)  # noqa: S104 -- the runner guests report here
