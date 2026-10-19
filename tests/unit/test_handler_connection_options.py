"""A handler that changes the store gives the connection the client's options.

`LocalStore::addToStore` of Nix checks the signature of each path against
`trusted-public-keys`, and a daemon learns that setting through `SetOptions`
alone. The three handlers that add a path took a transfer connection with no
options, so the daemon read its own keys and refused a path the cache had
signed with the key the client had just named. `require-sigs` and
`secret-key-files` travel the same road.

`ca:signatures` of the Nix functional suite measured it. This test is the
machine-checkable half: prose said "a connection carries the options of one
client" from issue nixidae/nixidae#231 onward, and three call sites did not. Issues nixidae/nixidae#236 and
nixidae/nixidae#231.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_HANDLERS = Path(__file__).resolve().parent.parent.parent / "pynixd" / "handlers"

READ_ONLY_HANDLERS = {
    "nar_from_path.py": (
        "It reads a NAR out of the store and changes nothing, so no setting of "
        "the client decides what the daemon does with it. The signature checks "
        "are on the road in."
    ),
}
"""Each handler that may take a connection with no options, and why.

Add an entry only for a handler that reads. A handler that writes to the store
belongs under a client's options, because that is where Nix puts the decision.
"""


def _transfer_conn_calls(path: Path) -> list[ast.Call]:
    """Every `transfer_conn(...)` call in the module at *path*."""
    tree = ast.parse(path.read_text())
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "transfer_conn"
    ]


def _handlers_using_a_transfer_connection() -> list[Path]:
    return sorted(path for path in _HANDLERS.glob("*.py") if _transfer_conn_calls(path))


def _store_execute_calls(path: Path) -> list[ast.Call]:
    """Every `local_store.execute(...)` call in the module at *path*."""
    tree = ast.parse(path.read_text())
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "execute"
        and isinstance(node.func.value, ast.Attribute)
        and node.func.value.attr == "local_store"
    ]


def _passes_a_client(call: ast.Call) -> bool:
    return any(keyword.arg == "client" for keyword in call.keywords) or len(call.args) >= 2


def test_a_handler_that_writes_passes_the_options_of_the_client() -> None:
    """The call takes an argument, and the argument is not nothing."""
    bare: list[str] = []
    for path in _handlers_using_a_transfer_connection():
        if path.name in READ_ONLY_HANDLERS:
            continue
        for call in _transfer_conn_calls(path):
            if not call.args and not call.keywords:
                bare.append(path.name)

    assert bare == [], (
        f"these handlers take a transfer connection with no options: {sorted(set(bare))}. "
        f"The daemon then reads its own settings, and `trusted-public-keys`, `require-sigs` "
        f"and `secret-key-files` of the client decide nothing. Issue nixidae/nixidae#236."
    )


def test_a_handler_that_writes_applies_the_options_it_asked_for() -> None:
    """The pool filters idle connections by the option set, and applies none.

    `get_or_create_conn` discards an idle connection that carries another
    set, and a connection it makes is new and carries none. `Connection.call`
    applies the set for an operation that goes through it, and a handler that
    streams writes its own bytes and never calls it. So the handler has to
    apply the set itself.
    """
    missing: list[str] = []
    for path in _handlers_using_a_transfer_connection():
        if path.name in READ_ONLY_HANDLERS:
            continue
        if "apply_options" not in path.read_text():
            missing.append(path.name)

    assert missing == [], (
        f"these handlers ask for a connection with options and never apply them: {sorted(missing)}. "
        f"Add `await conn.apply_options(options)` after the acquire. Issue nixidae/nixidae#236."
    )


def test_the_reason_of_each_read_only_handler_is_written_down() -> None:
    """An entry with no reason is a hole that reads as a decision."""
    for name, reason in READ_ONLY_HANDLERS.items():
        assert (_HANDLERS / name).is_file(), name
        assert len(reason) > 40, name


@pytest.mark.parametrize("name", ["add_to_store.py", "add_to_store_nar.py", "add_multiple_to_store.py"])
def test_the_three_handlers_that_add_a_path_are_covered(name: str) -> None:
    """The three that `ca:signatures` measured, named so a rename cannot lose them."""
    path = _HANDLERS / name
    assert path.is_file(), name
    assert _transfer_conn_calls(path), name
    assert name not in READ_ONLY_HANDLERS, name


def test_a_handler_that_dispatches_to_the_store_passes_the_client() -> None:
    """A nested call with no client discards the pooled connection.

    `get_or_create_conn` drops an idle connection whose `applied_options` is a
    set when the caller names none (`pool.py:196`), and the handler then pays a
    fresh upstream handshake. `AddToStore` signed its path info through such a
    call, so every derived path handshook again: the `AddToStore` half of a
    NixOS system instantiation measured 80 s against 7 s when the client rode
    along.
    """
    bare = [
        path.name
        for path in sorted(_HANDLERS.glob("*.py"))
        for call in _store_execute_calls(path)
        if not _passes_a_client(call)
    ]

    assert bare == [], (
        f"these handlers dispatch to the store without the client's options: {sorted(set(bare))}. "
        f"Add `client=ctx.proxy.client` so the pool reuses the connection."
    )
