"""The EC2 provider against a stand-in endpoint: botocore's serializer, signer and parser on real calls. nixidae/nixidae#1198.

The stand-in answers in EC2's own XML, so the parse is the one a live call takes.
"""

from __future__ import annotations

import ast
import base64
import datetime
import json
from typing import TYPE_CHECKING

import pytest
from aiohttp import web

from pynixd.config import Ec2Settings, Ec2TemplateSettings, ProvisioningSettings
from pynixd.provisioning.ec2 import Ec2Provider, bootstrap, client_token, notice_of
from pynixd.provisioning.provider import MachineState, NoCapacity, ProviderError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

NS = 'xmlns="http://ec2.amazonaws.com/doc/2016-11-15/"'
TICKET = json.dumps({"token": "secret-token", "system_features": ["big-parallel"], "max_jobs": 4})


def _error(code: str) -> web.Response:
    body = f"<Response><Errors><Error><Code>{code}</Code><Message>m</Message></Error></Errors><RequestID>x</RequestID></Response>"
    return web.Response(status=500 if code.startswith("Insufficient") else 400, text=body, content_type="text/xml")


def _instance(instance_id: str, state: str, spot: str | None = None) -> str:
    spot_xml = "" if spot is None else f"<spotInstanceRequestId>{spot}</spotInstanceRequestId>"
    return (
        f"<item><instanceId>{instance_id}</instanceId><instanceState><code>0</code><name>{state}</name></instanceState>"
        f"{spot_xml}<tagSet><item><key>pynixd-owner</key><value>me</value></item></tagSet></item>"
    )


class FakeEc2:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []
        self.run_errors: list[str] = []
        self.signed: list[bool] = []

    async def handle(self, request: web.Request) -> web.Response:
        form = dict(await request.post())
        params = {key: str(value) for key, value in form.items()}
        self.calls.append(params)
        self.signed.append(request.headers.get("Authorization", "").startswith("AWS4-HMAC-SHA256"))
        action = params["Action"]
        if action == "RunInstances":
            if self.run_errors:
                return _error(self.run_errors.pop(0))
            body = f"<RunInstancesResponse {NS}><reservationId>r</reservationId><instancesSet>{_instance('i-new', 'pending')}</instancesSet></RunInstancesResponse>"
        elif action == "DescribeInstances":
            items = _instance("i-1", "running", "sir-1") + _instance("i-2", "terminated")
            body = f"<DescribeInstancesResponse {NS}><reservationSet><item><reservationId>r</reservationId><instancesSet>{items}</instancesSet></item></reservationSet></DescribeInstancesResponse>"
        elif action == "DescribeSpotInstanceRequests":
            body = (
                f"<DescribeSpotInstanceRequestsResponse {NS}><spotInstanceRequestSet><item>"
                "<spotInstanceRequestId>sir-1</spotInstanceRequestId>"
                "<status><code>marked-for-termination</code><updateTime>2026-11-17T10:00:00.000Z</updateTime></status>"
                "</item></spotInstanceRequestSet></DescribeSpotInstanceRequestsResponse>"
            )
        elif action == "TerminateInstances":
            return _error("InvalidInstanceID.NotFound")
        else:
            return _error("InvalidAction")
        return web.Response(text=body, content_type="text/xml")


@pytest.fixture
async def ec2(monkeypatch: pytest.MonkeyPatch, tmp_path: object) -> AsyncIterator[tuple[FakeEc2, Ec2Provider]]:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI")
    monkeypatch.setenv("AWS_CONFIG_FILE", "/nonexistent")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
    fake = FakeEc2()
    app = web.Application()
    app.router.add_post("/", fake.handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    settings = Ec2Settings(
        region="eu-north-1",
        endpoint=f"http://127.0.0.1:{port}/",
        templates={"small": Ec2TemplateSettings(instance_types=["c7i.xlarge", "m7a.xlarge"])},
    )
    provider = Ec2Provider(settings)
    try:
        yield fake, provider
    finally:
        await provider.close()
        await runner.cleanup()


async def test_a_launch_is_a_tagged_spot_instance_that_terminates_on_shutdown(
    ec2: tuple[FakeEc2, Ec2Provider],
) -> None:
    fake, provider = ec2
    machine = await provider.launch("small", {"pynixd-owner": "me", "pynixd-instance": "small-1"}, TICKET)
    assert machine.provider_id == "i-new"
    assert machine.state is MachineState.PENDING
    call = fake.calls[0]
    assert fake.signed == [True]
    assert call["InstanceType"] == "c7i.xlarge"
    assert call["InstanceInitiatedShutdownBehavior"] == "terminate"
    assert call["InstanceMarketOptions.MarketType"] == "spot"
    kinds = {value for key, value in call.items() if key.endswith(".ResourceType")}
    assert kinds == {"instance", "volume", "network-interface", "spot-instances-request"}
    for n in range(1, 5):
        tags = {
            call[f"TagSpecification.{n}.Tag.{m}.Key"]: call[f"TagSpecification.{n}.Tag.{m}.Value"] for m in (1, 2, 3)
        }
        assert tags == {"pynixd-cloud-builder": "true", "pynixd-instance": "small-1", "pynixd-owner": "me"}
    script = base64.b64decode(call["UserData"]).decode()
    assert TICKET in script


async def test_a_type_with_no_capacity_gives_way_to_the_next(ec2: tuple[FakeEc2, Ec2Provider]) -> None:
    fake, provider = ec2
    fake.run_errors = ["InsufficientInstanceCapacity"]
    await provider.launch("small", {"pynixd-instance": "small-2"}, TICKET)
    assert [call["InstanceType"] for call in fake.calls] == ["c7i.xlarge", "m7a.xlarge"]
    assert fake.calls[0]["ClientToken"] != fake.calls[1]["ClientToken"]


async def test_no_type_with_capacity_is_no_capacity(ec2: tuple[FakeEc2, Ec2Provider]) -> None:
    fake, provider = ec2
    fake.run_errors = ["InsufficientInstanceCapacity", "MaxSpotInstanceCountExceeded"]
    with pytest.raises(NoCapacity, match="MaxSpotInstanceCountExceeded"):
        await provider.launch("small", {"pynixd-instance": "small-3"}, TICKET)


async def test_another_launch_error_is_a_provider_error_and_tries_no_other_type(
    ec2: tuple[FakeEc2, Ec2Provider],
) -> None:
    fake, provider = ec2
    fake.run_errors = ["UnauthorizedOperation"]
    with pytest.raises(ProviderError, match="UnauthorizedOperation"):
        await provider.launch("small", {"pynixd-instance": "small-4"}, TICKET)
    assert len(fake.calls) == 1


async def test_machines_filter_by_tag_and_carry_the_spot_notice(ec2: tuple[FakeEc2, Ec2Provider]) -> None:
    fake, provider = ec2
    machines = {one.provider_id: one for one in await provider.machines({"pynixd-owner": "me"})}
    assert fake.calls[0]["Filter.1.Name"] == "tag:pynixd-owner"
    assert fake.calls[0]["Filter.1.Value.1"] == "me"
    assert machines["i-1"].state is MachineState.RUNNING
    assert machines["i-1"].tags == {"pynixd-owner": "me"}
    ends = datetime.datetime(2026, 11, 17, 10, 2, tzinfo=datetime.UTC).timestamp()
    assert machines["i-1"].notice == ends
    assert machines["i-2"].state is MachineState.ENDED
    assert machines["i-2"].notice is None


async def test_terminating_an_unknown_instance_is_no_error(ec2: tuple[FakeEc2, Ec2Provider]) -> None:
    _, provider = ec2
    await provider.terminate("i-gone")


def test_a_running_request_has_no_notice() -> None:
    assert notice_of({"Status": {"Code": "fulfilled"}}) is None


def test_the_client_token_is_one_per_launch_and_type() -> None:
    tags = {"pynixd-instance": "a"}
    assert client_token(tags, "t1") == client_token(dict(tags), "t1")
    assert client_token(tags, "t1") != client_token(tags, "t2")
    assert client_token(tags, "t1") != client_token({"pynixd-instance": "b"}, "t1")
    assert len(client_token(tags, "t1")) <= 64


def test_the_bootstrap_is_python_that_never_prints_the_ticket() -> None:
    script = bootstrap(TICKET, Ec2TemplateSettings(instance_types=["c7i.xlarge"]))
    tree = ast.parse(script)
    assert script.startswith("#!/usr/bin/env python3\n")
    ticket_value = next(
        node.value.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == "TICKET"
        and isinstance(node.value, ast.Constant)
    )
    assert ticket_value == TICKET
    printed = [
        ast.unparse(node)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "print"
    ]
    assert printed
    assert not any("TICKET" in call or "ticket" in call for call in printed)
    assert "--idle-poweroff" in script


def test_provider_ec2_needs_its_settings() -> None:
    with pytest.raises(ValueError, match="ec2 settings"):
        ProvisioningSettings(provider="ec2")
