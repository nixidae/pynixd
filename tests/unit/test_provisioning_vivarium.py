"""The vivarium provider against a server that speaks vivarium's cloud routes. nixidae/nixidae#1408.

The server here is a stand-in with the same routes and answers as
`vivarium_runner/cloud.py`; the extended vivarium test drives the real one.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from aiohttp import web

from pynixd.provisioning.provider import MachineState, NoCapacity, NotFound
from pynixd.provisioning.vivarium import VivariumProvider

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path


class _Cloud:
    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.instances: dict[str, dict[str, object]] = {}

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/v1/instances", self.launch)
        app.router.add_get("/v1/instances", self.list)
        app.router.add_delete("/v1/instances/{id}", self.terminate)
        return app

    async def launch(self, request: web.Request) -> web.Response:
        body = await request.json()
        live = [i for i in self.instances.values() if i["state"] != "terminated"]
        if len(live) >= self.capacity:
            return web.json_response({"error": "insufficient capacity"}, status=409)
        if body["template"] != "worker":
            return web.json_response({"error": "no template"}, status=404)
        name = f"worker-{len(self.instances) + 1}"
        view = {
            "id": name,
            "template": "worker",
            "state": "pending",
            "tags": body["tags"],
            "user_data": body["user_data"],
            "notice": None,
            "error": None,
        }
        self.instances[name] = view
        return web.json_response(view, status=202)

    async def list(self, request: web.Request) -> web.Response:
        tags = {key.removeprefix("tag:"): value for key, value in request.query.items() if key.startswith("tag:")}
        found = [i for i in self.instances.values() if all(i["tags"].get(k) == v for k, v in tags.items())]  # type: ignore[attr-defined]  # a test's dict
        return web.json_response({"instances": found})

    async def terminate(self, request: web.Request) -> web.Response:
        instance = self.instances.get(request.match_info["id"])
        if instance is None:
            return web.json_response({"error": "no instance"}, status=404)
        instance["state"] = "terminated"
        return web.json_response(instance, status=202)


@pytest.fixture
async def cloud(tmp_path: Path) -> AsyncIterator[tuple[_Cloud, VivariumProvider]]:
    fake = _Cloud(capacity=1)
    runner = web.AppRunner(fake.app())
    await runner.setup()
    socket = tmp_path / "cloud.sock"
    await web.UnixSite(runner, str(socket)).start()
    provider = VivariumProvider(str(socket))
    try:
        yield fake, provider
    finally:
        await provider.close()
        await runner.cleanup()


async def test_a_launch_carries_its_tags_and_user_data(cloud: tuple[_Cloud, VivariumProvider]) -> None:
    fake, provider = cloud

    launched = await provider.launch("worker", {"pynixd-instance": "p-1"}, '{"token": "t"}')

    assert (launched.provider_id, launched.state, dict(launched.tags)) == (
        "worker-1",
        MachineState.PENDING,
        {"pynixd-instance": "p-1"},
    )
    assert fake.instances["worker-1"]["user_data"] == '{"token": "t"}'
    assert [m.provider_id for m in await provider.machines({"pynixd-instance": "p-1"})] == ["worker-1"]
    assert await provider.machines({"pynixd-instance": "p-2"}) == []


async def test_a_full_cloud_answers_no_capacity(cloud: tuple[_Cloud, VivariumProvider]) -> None:
    _, provider = cloud
    await provider.launch("worker", {}, "")

    with pytest.raises(NoCapacity):
        await provider.launch("worker", {}, "")


async def test_terminate_ends_the_machine_and_ignores_an_unknown_one(cloud: tuple[_Cloud, VivariumProvider]) -> None:
    _, provider = cloud
    await provider.launch("worker", {}, "")

    await provider.terminate("worker-1")
    await provider.terminate("worker-9")

    assert [m.state for m in await provider.machines({})] == [MachineState.ENDED]


async def test_an_unknown_template_is_not_found(cloud: tuple[_Cloud, VivariumProvider]) -> None:
    _, provider = cloud
    with pytest.raises(NotFound):
        await provider.launch("nothing", {}, "")
