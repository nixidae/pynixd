"""vivarium's cloud API as a provider: a pynixd in a vivarium guest launches guests. nixidae/nixidae#1408.

The guest's agent serves the API on a Unix socket (`vivarium.cloud = true`
on the node), one request per connection. The routes are
`vivarium_runner/cloud.py`'s.
"""

from __future__ import annotations

import contextlib
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import aiohttp

from .provider import Machine, MachineState, NoCapacity, NotFound, ProviderError

if TYPE_CHECKING:
    from collections.abc import Mapping

SOCKET = "/run/vivarium/cloud.sock"

_STATES = {
    "pending": MachineState.PENDING,
    "running": MachineState.RUNNING,
    "terminated": MachineState.ENDED,
    "failed": MachineState.ENDED,
}


def machine(view: Mapping[str, Any]) -> Machine:
    """A machine from the API's instance view."""
    notice = view.get("notice")
    tags: Mapping[str, Any] = view.get("tags") or {}
    return Machine(
        provider_id=str(view["id"]),
        state=_STATES[str(view["state"])],
        tags={str(key): str(value) for key, value in tags.items()},
        notice=None if notice is None else float(notice),
        error=None if view.get("error") is None else str(view["error"]),
    )


class VivariumProvider:
    name = "vivarium"

    def __init__(self, socket: str = SOCKET) -> None:
        # The server closes each connection after one reply.
        self._session = aiohttp.ClientSession(
            connector=aiohttp.UnixConnector(path=socket, force_close=True),
            base_url="http://cloud",
        )

    async def _call(self, method: str, path: str, **kwargs: Any) -> Any:  # noqa: ANN401 -- the API's JSON, read by each caller
        try:
            async with self._session.request(method, path, **kwargs) as response:
                body = await response.json()
                status = response.status
        except (aiohttp.ClientError, OSError, ValueError) as error:
            raise ProviderError(f"vivarium cloud: {method} {path}: {error}") from error
        if status == HTTPStatus.CONFLICT:
            raise NoCapacity(str(body.get("error")))
        if status == HTTPStatus.NOT_FOUND:
            raise NotFound(str(body.get("error")))
        if status >= HTTPStatus.BAD_REQUEST:
            raise ProviderError(f"vivarium cloud: {method} {path}: {status} {body.get('error')}")
        return body

    async def launch(self, template: str, tags: Mapping[str, str], user_data: str) -> Machine:
        body = await self._call(
            "POST", "/v1/instances", json={"template": template, "tags": dict(tags), "user_data": user_data}
        )
        return machine(body)

    async def terminate(self, provider_id: str) -> None:
        # An instance the run never had cannot cost anything.
        with contextlib.suppress(NotFound):
            await self._call("DELETE", f"/v1/instances/{provider_id}")

    async def machines(self, tags: Mapping[str, str]) -> list[Machine]:
        body = await self._call("GET", "/v1/instances", params={f"tag:{key}": value for key, value in tags.items()})
        return [machine(view) for view in body["instances"]]

    async def close(self) -> None:
        await self._session.close()
