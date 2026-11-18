"""The hand-over's decisions that need no second process. docs/design/upgrade-handover.md."""

from __future__ import annotations

import socket

from pynixd.handover import Answer, Inherited, ListenerName, _inherited
from pynixd.instance import Server
from pynixd.tcp_listen import adopt_tcp, bind_tcp
from pynixd_store.db_migrations import TARGET_VERSION


def test_a_schema_change_refuses_the_overlap() -> None:
    server = Server()
    assert server.handover_refusal(TARGET_VERSION) is None
    refusal = server.handover_refusal(TARGET_VERSION + 1)
    assert refusal is not None
    assert "schema" in refusal


def test_a_draining_process_refuses_a_second_hand_over() -> None:
    server = Server()
    server.draining = True
    assert server.handover_refusal(TARGET_VERSION) == "it is draining already"


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


def test_listener_names_are_the_wire_names() -> None:
    assert {name.value for name in ListenerName} == {"unix", "ssh", "reverse", "http", "https", "handover"}
