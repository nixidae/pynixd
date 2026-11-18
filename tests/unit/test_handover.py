"""The hand-over's decisions that need no second process. docs/design/upgrade-handover.md."""

from __future__ import annotations

import socket
from dataclasses import dataclass

import anyio
import anyio.lowlevel
import pytest

from pynixd.handover import Answer, Inherited, ListenerName, Offer, _inherited
from pynixd.instance import Server
from pynixd.tcp_listen import adopt_tcp, bind_tcp
from pynixd_store.db_migrations import TARGET_VERSION


async def test_a_schema_change_refuses_the_overlap() -> None:
    server = Server()
    assert await server.handover_refusal(Offer.of(server.settings)) is None
    refusal = await server.handover_refusal(Offer(schema_version=TARGET_VERSION + 1))
    assert refusal is not None
    assert "schema" in refusal


async def test_a_moved_daemon_socket_refuses_the_overlap() -> None:
    server = Server()
    offer = Offer.of(server.settings)
    offer.unix_path = "/run/elsewhere.sock"
    refusal = await server.handover_refusal(offer)
    assert refusal is not None
    assert "/run/elsewhere.sock" in refusal


async def test_a_changed_local_store_refuses_the_overlap() -> None:
    server = Server()
    offer = Offer.of(server.settings)
    offer.local_store = '{"socket_path": "/nix/var/nix/daemon-socket/upstream"}'
    assert await server.handover_refusal(offer) == "the local store's settings change"


async def test_an_offer_from_an_older_handover_checks_the_schema_only() -> None:
    assert await Server().handover_refusal(Offer(schema_version=TARGET_VERSION)) is None


async def test_a_draining_process_refuses_a_second_hand_over() -> None:
    server = Server()
    server.draining = True
    assert await server.handover_refusal(Offer.of(server.settings)) == "it is draining already"


@dataclass
class StubProvisioner:
    live: int
    launches_held: bool = False

    async def live_count(self) -> int:
        return self.live


@pytest.mark.parametrize("live", [0, 1])
async def test_live_provisioned_builders_refuse_and_none_hold_launches(live: int) -> None:
    server = Server()
    provisioner = StubProvisioner(live)
    server.provisioner = provisioner  # type: ignore[assignment]  # the two members a refusal reads
    refusal = await server.handover_refusal(Offer.of(server.settings))
    if live:
        assert refusal is not None
        assert "provisioned builders are live" in refusal
        assert not provisioner.launches_held
    else:
        assert refusal is None
        assert provisioner.launches_held
        server.handover_abandoned()
        assert not provisioner.launches_held


def test_the_fds_keep_their_listener_in_order() -> None:
    answer = Answer(pid=1, listeners={"unix": 1, "ssh": 2, "handover": 1}, unix_owns_path=True)
    inherited = _inherited(answer, [3, 4, 5, 6])
    assert inherited.fds == {"unix": [3], "ssh": [4, 5], "handover": [6]}
    assert inherited.unix_owns_path
    assert Inherited.model_validate_json(inherited.model_dump_json()) == inherited


def test_an_unknown_field_from_another_version_is_ignored() -> None:
    answer = Answer.model_validate_json('{"protocol": 2, "pid": 7, "listeners": {}, "later": true}')
    assert answer.pid == 7


def test_a_socket_where_the_settings_say_is_adopted() -> None:
    sockets = bind_tcp("127.0.0.1", 0)
    port = sockets[0].getsockname()[1]
    try:
        assert adopt_tcp(sockets, "127.0.0.1", port) is sockets
    finally:
        for sock in sockets:
            sock.close()


def test_a_moved_port_binds_anew_and_closes_the_old_socket() -> None:
    old = bind_tcp("127.0.0.1", 0)
    new = adopt_tcp(old, "127.0.0.1", 0)
    try:
        assert old[0].fileno() == -1
        assert new[0].getsockname()[1] != 0
        assert new[0].family == socket.AF_INET
    finally:
        for sock in new:
            sock.close()


async def _drain_around_a_loop(drain_inside: bool) -> tuple[int, int]:
    """Drain a server whose one singleton loop runs passes; answers (passes started, passes finished)."""
    server = Server()
    started = finished = 0
    inside = anyio.Event()
    release = anyio.Event()

    async def loop() -> None:
        nonlocal started, finished
        while True:
            async with server._singleton_pass():
                started += 1
                inside.set()
                await release.wait()
                finished += 1
            await anyio.lowlevel.checkpoint()

    async with server._background.running():
        if not drain_inside:
            release.set()
        server._spawn_singleton(loop)
        await inside.wait()
        await server.drain()
        await anyio.lowlevel.checkpoint()
        if drain_inside:
            # The pass running is work: the drain must not end under it.
            assert not server.drained.is_set()
        release.set()
        with anyio.fail_after(5):
            await server.drained.wait()
    return started, finished


async def test_a_drain_inside_a_pass_lets_it_finish_and_starts_no_other() -> None:
    assert await _drain_around_a_loop(drain_inside=True) == (1, 1)


async def test_a_drain_between_passes_starts_no_other() -> None:
    started, finished = await _drain_around_a_loop(drain_inside=False)
    assert started == finished


def test_a_family_the_kernel_lacks_is_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    real = socket.socket

    def no_ipv6(family: socket.AddressFamily = socket.AF_INET, *args: object, **kwargs: object) -> socket.socket:
        if family == socket.AF_INET6:
            raise OSError(97, "Address family not supported by protocol")
        return real(family, *args, **kwargs)  # type: ignore[arg-type]  # pass-through of socket's own arguments

    monkeypatch.setattr(socket, "socket", no_ipv6)
    sockets = bind_tcp("", 0)
    try:
        assert {sock.family for sock in sockets} == {socket.AF_INET}
    finally:
        for sock in sockets:
            sock.close()


def test_listener_names_are_the_wire_names() -> None:
    assert {name.value for name in ListenerName} == {"unix", "ssh", "reverse", "http", "https", "handover"}
