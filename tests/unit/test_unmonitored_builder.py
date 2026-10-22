"""A builder that refuses some channels is monitored over the ones it allows.

Exec without SFTP still runs `pynixd-agent`, and a builder that refuses both
falls to no monitor with one warning, not a traceback each interval. It then
ranks last (`tests/unit/test_store_choice.py`). nixidae/nixidae#1016.
"""

from __future__ import annotations

import asyncio
import shutil
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import anyio
import asyncssh

from pynixd.agent import AGENT_HELLO, agent_args
from pynixd.config import PressureSettings
from pynixd.monitor import GenericResourcePoller
from pynixd.store.ssh import AGENT_DIR, SSHStore

if TYPE_CHECKING:
    from pynixd.psi import SystemHealth


class _ExecOnly(asyncssh.SSHServer):
    def begin_auth(self, username: str) -> bool:
        del username
        return False


def _exec_in(home: Path) -> Any:
    """Run each command in a local shell in *home*, as sshd runs it in the login's home."""

    async def handle(process: asyncssh.SSHServerProcess[bytes]) -> None:
        if process.command is None:
            process.exit(1)  # the SFTP subsystem, refused
            return
        local = await asyncio.create_subprocess_shell(
            str(process.command), cwd=home, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE
        )
        assert local.stdin is not None and local.stdout is not None
        stdin, stdout = local.stdin, local.stdout

        async def feed() -> None:
            while data := await process.stdin.read(65536):
                stdin.write(data)
                await stdin.drain()
            stdin.close()

        async with anyio.create_task_group() as tg:
            tg.start_soon(feed)
            while data := await stdout.read(65536):
                process.stdout.write(data)
            tg.cancel_scope.cancel()
        process.exit(await local.wait())

    return handle


async def test_the_agent_goes_up_over_exec_when_sftp_is_refused(tmp_path: Path) -> None:
    binary = shutil.which("pynixd-agent")
    assert binary is not None, "pynixd-agent is not on PATH"
    agent = Path(binary).resolve()
    server = await asyncssh.create_server(
        _ExecOnly,
        "127.0.0.1",
        0,
        server_host_keys=[asyncssh.generate_private_key("ssh-ed25519")],
        process_factory=_exec_in(tmp_path),
        encoding=None,
    )
    store = SimpleNamespace(
        _feature_matrix={},
        systems={"this-system"},
        pressure_settings=PressureSettings(agents={"this-system": agent}),
        store_id="exec-only",
    )
    try:
        async with asyncssh.connect(
            "127.0.0.1", server.get_port(), username="nix", known_hosts=None, agent_path=None, client_keys=None
        ) as conn:
            run_agent = SSHStore._run_agent(cast("Any", store), conn, agent_args(PressureSettings(interval=10.0)))  # noqa: SLF001 -- the upload is the unit
            async with run_agent as stream:
                with anyio.fail_after(10):
                    hello = (await stream.readline()).decode()
    finally:
        server.close()
        await server.wait_closed()

    assert hello.startswith(AGENT_HELLO)
    assert (tmp_path / AGENT_DIR / agent.parent.parent.name).is_file()


async def test_a_host_that_refuses_sftp_is_not_polled_again() -> None:
    seen: list[SystemHealth] = []
    fell_back: list[bool] = []
    asked: list[str] = []

    async def refused(path: str) -> str | None:
        asked.append(path)
        raise asyncssh.ChannelOpenError(1, "subsystem request failed")  # 1: administratively prohibited

    poller = GenericResourcePoller(
        PressureSettings(interval=0.01), seen.append, refused, lambda: fell_back.append(True)
    )
    poller.running = True
    with anyio.fail_after(5):
        await poller.run()

    assert fell_back == [True]
    assert seen == []
    assert len(asked) == 1
