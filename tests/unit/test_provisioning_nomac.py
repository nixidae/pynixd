"""The NoMac provider against a stand-in NoMac API. nixidae/nixidae#1198.

Field names follow the API's answers as measured on 2026-11-19: `session`, `state`, `cleanup_confirmed`,
`activity.hard_deadline_at` (milliseconds), `error.code`, and a job's `argv`, `env` and `timeout_seconds`.
"""

from __future__ import annotations

import json
import stat
from typing import TYPE_CHECKING, Any

import pytest
from aiohttp import web

from pynixd.config import NomacSettings, NomacTemplateSettings, ProvisioningSettings
from pynixd.provisioning.nomac import BOOTSTRAP, NomacProvider, Refused, builder_files
from pynixd.provisioning.provider import MachineState, NoCapacity, NotFound, ProviderError
from pynixd.provisioning.ticket import JoinTicket
from tests.unit.test_builder_installer import install

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

TICKET = json.dumps({"token": "secret-token", "system_features": ["nomac", "big"], "max_jobs": 3})
TAGS = {"pynixd-owner": "me", "pynixd-instance": "mac-1", "pynixd-pool": "mac"}


class FakeNomac:
    def __init__(self) -> None:
        self.sessions: dict[str, dict[str, Any]] = {}
        """By id, in the API's shape."""
        self.by_key: dict[str, str] = {}
        """Session id by `Idempotency-Key`: a repeat answers the same session."""
        self.starts: list[tuple[str, dict[str, Any]]] = []
        self.jobs: list[tuple[str, str, dict[str, Any]]] = []
        self.job_keys: set[str] = set()
        self.stops: list[str] = []
        self.start_status = 202
        self.start_error = ""
        self.drop_next_start = False
        self.stop_status = 202
        self.authorization: set[str] = set()

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/mac/sessions", self.start)
        app.router.add_get("/mac/sessions", self.list)
        app.router.add_delete("/mac/sessions/{id}", self.stop)
        app.router.add_post("/mac/sessions/{id}/jobs", self.job)
        return app

    def add(self, ident: str, state: str, **fields: Any) -> dict[str, Any]:  # noqa: ANN401 -- session JSON
        self.sessions[ident] = {"id": ident, "state": state, "cleanup_confirmed": False, **fields}
        return self.sessions[ident]

    async def start(self, request: web.Request) -> web.StreamResponse:
        self.authorization.add(request.headers["Authorization"])
        key = request.headers["Idempotency-Key"]
        self.starts.append((key, await request.json()))
        if self.start_status >= 400:
            return web.json_response({"error": {"code": self.start_error}}, status=self.start_status)
        ident = self.by_key.get(key)
        if ident is None:
            ident = self.by_key[key] = f"ses_{len(self.by_key) + 1}"
            self.add(ident, "queued")
        if self.drop_next_start:
            self.drop_next_start = False
            # The session exists, and its answer never arrives.
            assert request.transport is not None
            request.transport.abort()
            return web.Response()
        return web.json_response({"session": self.sessions[ident]}, status=202)

    async def list(self, request: web.Request) -> web.Response:
        return web.json_response({"sessions": list(self.sessions.values()), "limit": 100})

    async def stop(self, request: web.Request) -> web.Response:
        ident = request.match_info["id"]
        self.stops.append(ident)
        assert request.query["confirm"] == "true"
        if self.stop_status != 202:
            return web.json_response({"error": {"code": "x"}}, status=self.stop_status)
        self.sessions[ident]["state"] = "stopping"
        return web.json_response({"session": self.sessions[ident]}, status=202)

    async def job(self, request: web.Request) -> web.Response:
        key = request.headers["Idempotency-Key"]
        if key not in self.job_keys:
            self.job_keys.add(key)
            self.jobs.append((request.match_info["id"], key, await request.json()))
        return web.json_response({"id": "job_1", "state": "running"}, status=202)


@pytest.fixture
async def nomac(tmp_path: Path) -> AsyncIterator[tuple[FakeNomac, NomacProvider, NomacSettings]]:
    fake = FakeNomac()
    runner = web.AppRunner(fake.app())
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    (tmp_path / "key").write_text("nmk_fake\n")
    settings = NomacSettings(
        api_key_file=tmp_path / "key",
        api_url=f"http://127.0.0.1:{port}",
        state_file=tmp_path / "state" / "nomac.json",
        templates={"mac": NomacTemplateSettings(spend_cap_usd=1.5, idle_timeout_seconds=900, parallel=True)},
    )
    provider = NomacProvider(settings)
    try:
        yield fake, provider, settings
    finally:
        await provider.close()
        await runner.cleanup()


async def test_a_launch_starts_a_mac_with_every_limit_and_remembers_its_tags_first(
    nomac: tuple[FakeNomac, NomacProvider, NomacSettings],
) -> None:
    fake, provider, settings = nomac
    launched = await provider.launch("mac", TAGS, TICKET)
    assert (launched.provider_id, launched.state, dict(launched.tags)) == ("ses_1", MachineState.PENDING, TAGS)
    [(key, body)] = fake.starts
    assert body == {
        "profile": "standard",
        "limits": {"max_duration_seconds": 28800, "idle_timeout_seconds": 900, "spend_cap_microunits": "1500000"},
        "access": {"drive": "none"},
        "parallel": True,
    }
    assert "secret-token" not in json.dumps(body), "the ticket goes to the Mac in the bootstrap job alone"
    assert fake.authorization == {"Bearer nmk_fake"}
    assert stat.S_IMODE(settings.state_file.stat().st_mode) == 0o600, "the file holds the ticket"
    [entry] = json.loads(settings.state_file.read_text())["entries"].values()
    assert (entry["request_key"], entry["session_id"], entry["tags"], entry["ticket"]) == (key, "ses_1", TAGS, TICKET)


async def test_a_ready_mac_gets_one_bootstrap_job_that_carries_the_ticket_in_its_environment(
    nomac: tuple[FakeNomac, NomacProvider, NomacSettings],
) -> None:
    fake, provider, settings = nomac
    await provider.launch("mac", TAGS, TICKET)
    [listed] = await provider.machines({})
    assert listed.state is MachineState.PENDING
    assert fake.jobs == [], "a queued Mac takes no job"

    fake.sessions["ses_1"]["state"] = "ready"
    [listed] = await provider.machines({})
    await provider.machines({})

    assert listed.state is MachineState.RUNNING
    [(session, key, job)] = fake.jobs
    assert (session, key) == ("ses_1", f"{fake.starts[0][0]}-bootstrap")
    assert job["argv"] == ["/bin/bash", "-c", BOOTSTRAP]
    assert "secret-token" not in BOOTSTRAP
    env = job["env"]
    assert env["PYNIXD_TICKET"] == TICKET
    assert json.loads(env["PYNIXD_INITIATOR"]) == {
        "join_ticket": "/etc/pynixd/join-ticket.json",
        "system_features": ["nomac", "big"],
        "max_jobs": 3,
    }
    assert env["PYNIXD_EXTRA_CONF"] == "extra-system-features = nomac big\n"
    assert env["PYNIXD_INSTALLER"].endswith("/pynixd-installer/pynixd-aarch64-darwin.tar.xz")
    assert env["PYNIXD_VOLUME"] == "APFS"
    [entry] = json.loads(settings.state_file.read_text())["entries"].values()
    assert entry["ticket"] is None, "a used ticket is not kept"


def test_the_builder_files_are_what_the_installer_writes_from_the_same_ticket() -> None:
    initiator, extra = builder_files(TICKET)
    expected_initiator, expected_extra = install.builder_settings(JoinTicket.model_validate_json(TICKET))
    assert (json.loads(initiator), extra) == (expected_initiator, expected_extra)


async def test_a_session_the_provider_did_not_start_is_never_listed_or_ended(
    nomac: tuple[FakeNomac, NomacProvider, NomacSettings],
) -> None:
    fake, provider, _ = nomac
    fake.add("ses_owner", "ready")
    await provider.launch("mac", TAGS, TICKET)
    assert [m.provider_id for m in await provider.machines({})] == ["ses_1"]
    assert [m.provider_id for m in await provider.machines({"pynixd-instance": "other"})] == []
    assert fake.jobs == []
    assert fake.stops == []


async def test_a_start_whose_answer_was_lost_is_asked_again_and_ended(
    nomac: tuple[FakeNomac, NomacProvider, NomacSettings],
) -> None:
    fake, provider, _ = nomac
    fake.drop_next_start = True
    with pytest.raises(ProviderError):
        await provider.launch("mac", TAGS, TICKET)
    assert list(fake.sessions) == ["ses_1"], "the Mac started though its answer was lost"

    assert await provider.machines({}) == [], "the launch failed, so its Mac is no machine"

    assert [key for key, _ in fake.starts] == [fake.starts[0][0]] * 2, "the repeat sends the same key"
    assert list(fake.sessions) == ["ses_1"], "the repeat started nothing"
    assert fake.stops == ["ses_1"]
    assert fake.jobs == []


async def test_a_refused_start_is_no_capacity_or_an_error_and_leaves_nothing_to_find(
    nomac: tuple[FakeNomac, NomacProvider, NomacSettings],
) -> None:
    fake, provider, settings = nomac
    fake.start_status, fake.start_error = 409, "session_limit_reached"
    with pytest.raises(NoCapacity):
        await provider.launch("mac", TAGS, TICKET)
    fake.start_status, fake.start_error = 402, "insufficient_credit"
    with pytest.raises(Refused, match="402"):
        await provider.launch("mac", TAGS, TICKET)
    assert json.loads(settings.state_file.read_text())["entries"] == {}
    with pytest.raises(NotFound):
        await provider.launch("no-such-template", TAGS, TICKET)


async def test_an_ended_session_is_ended_and_a_failed_one_says_why(
    nomac: tuple[FakeNomac, NomacProvider, NomacSettings],
) -> None:
    fake, provider, _ = nomac
    await provider.launch("mac", TAGS, TICKET)
    fake.sessions["ses_1"].update(state="failed", failure_code="capacity_unavailable")
    [listed] = await provider.machines({})
    assert listed.state is MachineState.ENDED
    assert listed.error == "the session failed: capacity_unavailable"


async def test_a_stop_notice_is_the_machine_s_notice(nomac: tuple[FakeNomac, NomacProvider, NomacSettings]) -> None:
    fake, provider, _ = nomac
    await provider.launch("mac", TAGS, TICKET)
    fake.sessions["ses_1"].update(
        state="ready", stop_notice={"reason": "time_limit"}, activity={"hard_deadline_at": 1795078822613}
    )
    [listed] = await provider.machines({})
    assert listed.notice == pytest.approx(1795078822.613)


async def test_terminate_stops_with_confirm_and_a_gone_session_is_no_error(
    nomac: tuple[FakeNomac, NomacProvider, NomacSettings],
) -> None:
    fake, provider, _ = nomac
    await provider.launch("mac", TAGS, TICKET)
    await provider.terminate("ses_1")
    fake.stop_status = 410
    await provider.terminate("ses_1")
    fake.stop_status = 500
    with pytest.raises(ProviderError, match="500"):
        await provider.terminate("ses_1")
    assert fake.stops == ["ses_1"] * 3


def test_provider_nomac_needs_its_settings() -> None:
    with pytest.raises(ValueError, match="nomac settings"):
        ProvisioningSettings(provider="nomac")
