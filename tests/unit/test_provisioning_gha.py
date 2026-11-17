"""The GitHub Actions provider against a stand-in REST API. nixidae/nixidae#1473."""

from __future__ import annotations

import base64
import datetime
import json
from typing import TYPE_CHECKING, Any

import pytest
from aiohttp import web

from pynixd.config import GhaSettings, GhaTemplateSettings, ProvisioningSettings
from pynixd.provisioning.gha import RUN_NAME_PREFIX, GhaProvider, created_since, run_tags, tags_text
from pynixd.provisioning.provider import MachineState, NotFound, ProviderError
from pynixd.provisioning.ticket import SealError, ticket_key, unseal

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

KEY = base64.b64encode(bytes(range(32))).decode()
TICKET = json.dumps({"token": "secret-token", "system_features": ["gha"], "max_jobs": 2})
TAGS = {"pynixd-owner": "me", "pynixd-instance": "big-1", "pynixd-pool": "big"}


def _run(run_id: int, tags: dict[str, str] | None, status: str, conclusion: str | None = None) -> dict[str, Any]:  # noqa: ANN401 -- a run's JSON
    title = "someone's own run" if tags is None else RUN_NAME_PREFIX + tags_text(tags)
    return {"id": run_id, "display_title": title, "status": status, "conclusion": conclusion}


class FakeGitHub:
    def __init__(self) -> None:
        self.dispatched: list[dict[str, Any]] = []
        self.listed: list[web.Request] = []
        self.cancelled: list[str] = []
        self.runs: list[dict[str, Any]] = []
        self.dispatch_status = 200
        self.cancel_status = 202
        self.authorization: set[str] = set()

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/repos/o/r/actions/workflows/pynixd-builder.yml/dispatches", self.dispatch)
        app.router.add_get("/repos/o/r/actions/workflows/pynixd-builder.yml/runs", self.list)
        app.router.add_post("/repos/o/r/actions/runs/{id}/cancel", self.cancel)
        return app

    async def dispatch(self, request: web.Request) -> web.Response:
        self.authorization.add(request.headers["Authorization"])
        self.dispatched.append(await request.json())
        if self.dispatch_status == 204:
            return web.Response(status=204)
        if self.dispatch_status != 200:
            return web.json_response({"message": "Not Found"}, status=self.dispatch_status)
        return web.json_response({"workflow_run_id": 4242, "run_url": "u", "html_url": "h"})

    async def list(self, request: web.Request) -> web.Response:
        self.listed.append(request)
        per_page, page = int(request.query["per_page"]), int(request.query["page"])
        body = {"workflow_runs": self.runs[(page - 1) * per_page : page * per_page]}
        etag = f'"{hash(json.dumps(body))}"'
        if request.headers.get("If-None-Match") == etag:
            return web.Response(status=304, headers={"ETag": etag})
        return web.json_response(body, headers={"ETag": etag})

    async def cancel(self, request: web.Request) -> web.Response:
        self.cancelled.append(request.match_info["id"])
        return web.json_response({}, status=self.cancel_status)


@pytest.fixture
async def gha(tmp_path: Path) -> AsyncIterator[tuple[FakeGitHub, GhaProvider]]:
    fake = FakeGitHub()
    runner = web.AppRunner(fake.app())
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    (tmp_path / "token").write_text("ghp_fake\n")
    (tmp_path / "key").write_text(KEY + "\n")
    settings = GhaSettings(
        repository="o/r",
        token_file=tmp_path / "token",
        ticket_key_file=tmp_path / "key",
        api_url=f"http://127.0.0.1:{port}",
        templates={"linux": GhaTemplateSettings(runs_on=["blacksmith-4vcpu-ubuntu-2404"], idle_seconds=900)},
    )
    provider = GhaProvider(settings)
    try:
        yield fake, provider
    finally:
        await provider.close()
        await runner.cleanup()


async def test_a_launch_dispatches_the_workflow_with_the_ticket_sealed_to_its_tags(
    gha: tuple[FakeGitHub, GhaProvider],
) -> None:
    fake, provider = gha
    launched = await provider.launch("linux", TAGS, TICKET)
    assert (launched.provider_id, launched.state, dict(launched.tags)) == ("4242", MachineState.PENDING, TAGS)
    [body] = fake.dispatched
    assert (body["ref"], body["return_run_details"]) == ("main", True)
    inputs = body["inputs"]
    assert json.loads(inputs["runs_on"]) == ["blacksmith-4vcpu-ubuntu-2404"]
    assert inputs["installer"].endswith("/pynixd-installer/pynixd-x86_64-linux.tar.zst")
    assert (inputs["idle_seconds"], inputs["bootstrap_minutes"]) == ("900", "10")
    assert json.loads(inputs["tags"]) == TAGS
    assert "secret-token" not in json.dumps(body)
    assert unseal(inputs["ticket"], ticket_key(KEY), inputs["tags"]) == TICKET
    with pytest.raises(SealError):
        unseal(inputs["ticket"], ticket_key(KEY), tags_text({**TAGS, "pynixd-pool": "other"}))
    assert fake.authorization == {"Bearer ghp_fake"}


async def test_a_missing_workflow_is_not_found(gha: tuple[FakeGitHub, GhaProvider]) -> None:
    fake, provider = gha
    fake.dispatch_status = 404
    with pytest.raises(NotFound):
        await provider.launch("linux", TAGS, TICKET)
    with pytest.raises(NotFound):
        await provider.launch("no-such-template", TAGS, TICKET)


async def test_a_dispatch_that_names_no_run_is_a_provider_error(gha: tuple[FakeGitHub, GhaProvider]) -> None:
    fake, provider = gha
    fake.dispatch_status = 204
    with pytest.raises(ProviderError, match="204"):
        await provider.launch("linux", TAGS, TICKET)


async def test_machines_are_this_owners_runs_with_their_states(gha: tuple[FakeGitHub, GhaProvider]) -> None:
    fake, provider = gha
    fake.runs = [
        _run(1, TAGS, "queued"),
        _run(2, {**TAGS, "pynixd-instance": "big-2"}, "in_progress"),
        _run(3, {**TAGS, "pynixd-instance": "big-3"}, "completed", "failure"),
        _run(4, {**TAGS, "pynixd-instance": "big-4"}, "completed", "cancelled"),
        _run(5, {**TAGS, "pynixd-owner": "someone-else"}, "in_progress"),
        _run(6, None, "in_progress"),
    ]
    found = await provider.machines({"pynixd-owner": "me"})
    assert [(m.provider_id, m.state, m.error) for m in found] == [
        ("1", MachineState.PENDING, None),
        ("2", MachineState.RUNNING, None),
        ("3", MachineState.ENDED, "the run ended failure"),
        ("4", MachineState.ENDED, None),
    ]
    [request] = fake.listed
    assert request.query["event"] == "workflow_dispatch"
    assert request.query["created"].startswith(">=")


async def test_an_unchanged_listing_is_answered_from_its_etag(gha: tuple[FakeGitHub, GhaProvider]) -> None:
    fake, provider = gha
    fake.runs = [_run(1, TAGS, "in_progress")]
    first = await provider.machines({})
    again = await provider.machines({})
    assert first == again
    assert "If-None-Match" not in fake.listed[0].headers
    assert "If-None-Match" in fake.listed[1].headers
    fake.runs = [_run(1, TAGS, "completed", "success")]
    [ended] = await provider.machines({})
    assert ended.state is MachineState.ENDED


async def test_a_full_page_reads_the_next(gha: tuple[FakeGitHub, GhaProvider]) -> None:
    fake, provider = gha
    fake.runs = [_run(n, {**TAGS, "pynixd-instance": f"big-{n}"}, "queued") for n in range(150)]
    assert len(await provider.machines({})) == 150
    assert [request.query["page"] for request in fake.listed] == ["1", "2"]


@pytest.mark.parametrize("status", [202, 409, 404])
async def test_cancelling_a_run_ended_or_unknown_is_no_error(gha: tuple[FakeGitHub, GhaProvider], status: int) -> None:
    fake, provider = gha
    fake.cancel_status = status
    await provider.terminate("7")
    assert fake.cancelled == ["7"]


async def test_another_cancel_answer_is_a_provider_error(gha: tuple[FakeGitHub, GhaProvider]) -> None:
    fake, provider = gha
    fake.cancel_status = 403
    with pytest.raises(ProviderError, match="403"):
        await provider.terminate("7")


def test_a_run_name_that_is_not_the_providers_has_no_tags() -> None:
    assert run_tags(RUN_NAME_PREFIX + tags_text(TAGS)) == TAGS
    assert run_tags("pynixd: builder") is None
    assert run_tags(RUN_NAME_PREFIX + "not json") is None
    assert run_tags(RUN_NAME_PREFIX + '["a list"]') is None
    assert run_tags(RUN_NAME_PREFIX + '{"n": 1}') is None


def test_the_created_filter_holds_for_a_day() -> None:
    now = datetime.datetime(2026, 11, 17, 18, 30, tzinfo=datetime.UTC)
    assert created_since(now, 86400) == ">=2026-11-16"
    assert created_since(now + datetime.timedelta(hours=5), 86400) == ">=2026-11-16"


def test_provider_gha_needs_its_settings() -> None:
    with pytest.raises(ValueError, match="gha settings"):
        ProvisioningSettings(provider="gha")


def test_a_key_of_the_wrong_size_is_refused() -> None:
    with pytest.raises(SealError, match="16 bytes"):
        ticket_key(base64.b64encode(bytes(16)).decode())
    with pytest.raises(SealError, match="base64"):
        ticket_key("not base64!")
