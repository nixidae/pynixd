"""EC2's Query API in front of vivarium's cloud: the real `Ec2Provider` launches guests. nixidae/nixidae#1420.

Three actions, the ones the provider calls on instances with no spot request:
RunInstances, DescribeInstances and TerminateInstances. `ImageId` names the
vivarium template, and `UserData` is the launch's user data, decoded, as
EC2 hands it to cloud-init. Signatures are not checked.

`python fake_ec2.py PORT`, on a guest with `vivarium.cloud = true`.
"""

from __future__ import annotations

import base64
import sys
from typing import Any, Final
from xml.sax.saxutils import escape

import aiohttp
from aiohttp import web

CLOUD: Final = "/run/vivarium/cloud.sock"
NS: Final = 'xmlns="http://ec2.amazonaws.com/doc/2016-11-15/"'

STATES: Final = {"pending": (0, "pending"), "running": (16, "running"), "terminated": (48, "terminated")}
"""vivarium's state to EC2's code and name. vivarium's `failed` is EC2's `terminated` with a state reason."""

type Instance = dict[str, Any]
"""vivarium's instance view, as its API answers it."""


def instance_xml(view: Instance) -> str:
    code, name = STATES.get(str(view["state"]), STATES["terminated"])
    tags = "".join(
        f"<item><key>{escape(key)}</key><value>{escape(value)}</value></item>" for key, value in view["tags"].items()
    )
    reason = (
        "" if view.get("error") is None else f"<stateReason><message>{escape(view['error'])}</message></stateReason>"
    )
    return (
        f"<item><instanceId>{view['id']}</instanceId><imageId>{escape(view['template'])}</imageId>"
        f"<instanceState><code>{code}</code><name>{name}</name></instanceState>{reason}<tagSet>{tags}</tagSet></item>"
    )


def error(code: str, status: int = 400) -> web.Response:
    body = f"<Response><Errors><Error><Code>{code}</Code><Message>{code}</Message></Error></Errors><RequestID>fake</RequestID></Response>"
    return web.Response(status=status, text=body, content_type="text/xml")


def ok(action: str, inner: str) -> web.Response:
    return web.Response(
        text=f"<{action}Response {NS}><requestId>fake</requestId>{inner}</{action}Response>", content_type="text/xml"
    )


def indexed(params: dict[str, str], prefix: str) -> list[str]:
    """`prefix.1`, `prefix.2`, ... in order: the Query API's list encoding."""
    found = []
    n = 1
    while f"{prefix}.{n}" in params:
        found.append(params[f"{prefix}.{n}"])
        n += 1
    return found


def instance_tags(params: dict[str, str]) -> dict[str, str]:
    tags: dict[str, str] = {}
    n = 1
    while f"TagSpecification.{n}.ResourceType" in params:
        if params[f"TagSpecification.{n}.ResourceType"] == "instance":
            m = 1
            while f"TagSpecification.{n}.Tag.{m}.Key" in params:
                tags[params[f"TagSpecification.{n}.Tag.{m}.Key"]] = params[f"TagSpecification.{n}.Tag.{m}.Value"]
                m += 1
        n += 1
    return tags


class FakeEc2:
    def __init__(self, cloud: aiohttp.ClientSession) -> None:
        self.cloud = cloud

    async def call(self, method: str, path: str, **kwargs: Any) -> tuple[int, Any]:  # noqa: ANN401 -- the API's JSON
        async with self.cloud.request(method, path, **kwargs) as response:
            return response.status, await response.json()

    async def handle(self, request: web.Request) -> web.Response:
        params = {key: str(value) for key, value in (await request.post()).items()}
        action = params.get("Action")
        if action == "RunInstances":
            return await self.run(params)
        if action == "DescribeInstances":
            return await self.describe(params)
        if action == "TerminateInstances":
            return await self.terminate(params)
        return error("InvalidAction")

    async def run(self, params: dict[str, str]) -> web.Response:
        body = {
            "template": params["ImageId"],
            "tags": instance_tags(params),
            "user_data": base64.b64decode(params["UserData"]).decode(),
        }
        status, view = await self.call("POST", "/v1/instances", json=body)
        if status == 409:
            return error("InsufficientInstanceCapacity", 500)
        if status == 404:
            return error("InvalidAMIID.NotFound")
        if status >= 400:
            return error("InternalError", 500)
        return ok(
            "RunInstances", f"<reservationId>r-fake</reservationId><instancesSet>{instance_xml(view)}</instancesSet>"
        )

    async def describe(self, params: dict[str, str]) -> web.Response:
        query = {}
        n = 1
        while f"Filter.{n}.Name" in params:
            name = params[f"Filter.{n}.Name"]
            if not name.startswith("tag:"):
                return error("InvalidParameterValue")
            query[name] = params[f"Filter.{n}.Value.1"]
            n += 1
        _, listed = await self.call("GET", "/v1/instances", params=query)
        items = "".join(instance_xml(view) for view in listed["instances"])
        reservations = (
            ""
            if not items
            else f"<item><reservationId>r-fake</reservationId><instancesSet>{items}</instancesSet></item>"
        )
        return ok("DescribeInstances", f"<reservationSet>{reservations}</reservationSet>")

    async def terminate(self, params: dict[str, str]) -> web.Response:
        items = []
        for instance_id in indexed(params, "InstanceId"):
            status, view = await self.call("DELETE", f"/v1/instances/{instance_id}")
            if status == 404:
                return error("InvalidInstanceID.NotFound")
            code, name = STATES["terminated"]
            items.append(
                f"<item><instanceId>{view['id']}</instanceId>"
                f"<currentState><code>{code}</code><name>{name}</name></currentState>"
                f"<previousState><code>16</code><name>running</name></previousState></item>"
            )
        return ok("TerminateInstances", f"<instancesSet>{''.join(items)}</instancesSet>")


async def app() -> web.Application:
    cloud = aiohttp.ClientSession(
        connector=aiohttp.UnixConnector(path=CLOUD, force_close=True), base_url="http://cloud"
    )
    application = web.Application()
    application.router.add_post("/", FakeEc2(cloud).handle)

    async def close(_: web.Application) -> None:
        await cloud.close()

    application.on_cleanup.append(close)
    return application


if __name__ == "__main__":
    web.run_app(app(), host="127.0.0.1", port=int(sys.argv[1]), print=None)
