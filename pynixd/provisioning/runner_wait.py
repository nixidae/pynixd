"""The controller's end of a runner build (`pynixd_store.builder.runner`, nixidae/nixidae#1560, spike).

`build()` starts this as the helper's process, on the build's terminal: `python -I -m
pynixd.provisioning.runner_wait TOP SPEC CONTAINER READY`. It checks that the runner's spec holds the job's
builder, arguments and environment, touches READY so the runner starts, copies the runner's log to the terminal
through the API, and writes the builder's exit status to TOP once the runner ended. A set-up error goes to TOP
as well, as the other sites' do.

Killing this process does not stop the runner: central deletes the pod once the controller reports.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import anyio
import kr8s.asyncio
from kr8s.asyncio.objects import Pod

from pynixd_store.builder.runner import (
    ETC_NAME,
    EXIT_NAME,
    POD_NAME_VARIABLE,
    POD_NAMESPACE_VARIABLE,
    STATUS_NAME,
    RunnerState,
    etc_files,
    exit_status,
    from_container,
    invocation,
    mismatch,
    runner_state,
)

if TYPE_CHECKING:
    from collections.abc import Callable

POD_HOSTS: Final = "/etc/hosts"
"""The controller's `/etc/hosts`, which the kubelet writes for the pod."""


class SetupError(Exception):
    """The runner cannot run this build."""


async def _until(pod: Pod, container: str, done: Callable[[RunnerState], bool]) -> RunnerState:
    """The runner's state once *done* holds for it, from a read and a watch from that read's version."""
    await pod.refresh()
    state = runner_state(pod.raw, container)
    while not done(state):
        if state.refused is not None:
            raise SetupError(f"the runner container does not start: {state.refused}")
        async for _event, changed in pod.watch():
            state = runner_state(changed.raw, container)
            if done(state) or state.refused is not None:
                break
        else:
            # The API server closed the watch: read again.
            await pod.refresh()
            state = runner_state(pod.raw, container)
    return state


async def _follow(pod: Pod, container: str) -> None:
    """Copy the runner's log, as the runtime wrote it, to stdout: the build's terminal."""
    params = {"follow": "true", "container": container}
    async with pod.api.call_api(
        "GET",
        version="v1",
        url=f"pods/{pod.name}/log",
        namespace=pod.namespace,
        params=params,
        stream=True,
        timeout=None,
    ) as response:
        async for piece in response.aiter_bytes():
            await anyio.to_thread.run_sync(os.write, 1, piece)


async def wait(top: Path, spec_file: Path, container: str, ready: Path) -> None:
    spec: dict[str, Any] = json.loads(await anyio.Path(spec_file).read_text())  # Any: the job's ChildSpec JSON
    expected = invocation(spec["builder"], spec["args"], spec["env"])
    api = await kr8s.asyncio.api()
    pod = await Pod.get(os.environ[POD_NAME_VARIABLE], namespace=os.environ[POD_NAMESPACE_VARIABLE], api=api)
    runner = next((c for c in pod.raw["spec"]["containers"] if c["name"] == container), None)
    if runner is None:
        raise SetupError(f"the pod has no container named {container!r}")
    if (differs := mismatch(expected, from_container(runner))) is not None:
        raise SetupError(f"the runner container does not run this build: {differs}")
    files = etc_files(
        uid=spec["sandbox_uid"],
        gid=spec["sandbox_gid"],
        network=not spec["private_network"],
        pod_hosts=await anyio.Path(POD_HOSTS).read_text(),
    )
    etc = anyio.Path(top / ETC_NAME)
    await etc.mkdir()
    for name, text in files.items():
        await (etc / name).write_text(text)
        await (etc / name).chmod(0o644)
    # The kubelet mounts each of them in the runner when it creates it, after the controller's postStart hook.
    await anyio.Path(ready).touch()
    await _until(pod, container, lambda state: state.started)
    await _follow(pod, container)
    ended = await _until(pod, container, lambda state: state.terminated is not None)
    if ended.terminated is None:
        raise SetupError("the runner container's state has no end")
    await anyio.Path(top / EXIT_NAME).write_text(str(exit_status(ended.terminated)))


def main() -> None:
    top, spec_file, container, ready = sys.argv[1:]
    try:
        anyio.run(wait, Path(top), Path(spec_file), container, Path(ready))
    except SetupError as error:
        (Path(top) / STATUS_NAME).write_text(str(error))
        sys.exit(1)


if __name__ == "__main__":
    main()
