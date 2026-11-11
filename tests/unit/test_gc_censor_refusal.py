"""A delegated daemon's DELETE_SPECIFIC refusal, censored for an untrusted client.

pynixd's connection is root's, so a Determinate v3.24.0 daemon names the
runtime and temporary roots (`gc.cc:762`, `:777`) that it censors for an
untrusted client of its own (`daemon.cc:807`). nixidae/nixidae#1278.
"""

from __future__ import annotations

import pytest
from test_gc_retires_idle_connections import (  # type: ignore[import-not-found]  # pytest puts tests/unit on sys.path; not a package
    FakeContext,
    FakeProxy,
    _body,
    _request,
)

from pynixd.handlers.collect_garbage import CollectGarbageHandler, censored_refusal
from pynixd.serde import CollectGarbageResponse, LogError
from pynixd.serde.auth import Role
from pynixd_store.hintfmt import magenta

PATH = "/nix/store/00000000000000000000000000000001-a"


def _refusal(reason: str) -> str:
    return f"Cannot delete path '{magenta(PATH)}' because it's {reason}."


@pytest.mark.parametrize(
    "name",
    ["/proc/1234/maps", "/proc/1234/environ", "{nix-process:1234}", "{nix-process:unknown}", "{memory:7}"],
)
def test_a_runtime_or_temporary_root_is_censored(name: str) -> None:
    reason = "in use by" if name.startswith("{nix-process") else "referenced by the GC root"
    assert censored_refusal(_refusal(f"{reason} '{magenta(name)}'")) == _refusal(f"{reason} '{magenta('{censored}')}'")


@pytest.mark.parametrize(
    "message",
    [
        _refusal(f"referenced by the GC root '{magenta('/nix/var/nix/gcroots/auto/x')}'"),
        _refusal(f"referenced by path '{magenta(PATH)}'"),
        f"Cannot delete path '{magenta(PATH)}' since it is still alive. To find out why, use: nix-store --query --roots",
    ],
    ids=["gcroots-link", "referrer", "upstream"],
)
def test_every_other_refusal_stays(message: str) -> None:
    """A `gcroots` link stays, as `find_roots.censor` keeps it; upstream 2.35 names no root (`gc.cc:780`)."""
    assert censored_refusal(message) == message


async def _answer(role: Role, message: str) -> CollectGarbageResponse:
    proxy = FakeProxy(await _body(_request()))
    proxy.local_store.answer.logs.add(LogError(msg=message))
    response = await CollectGarbageHandler().handle(FakeContext(proxy=proxy, role=role))  # type: ignore[arg-type]  # a fake context
    assert isinstance(response, CollectGarbageResponse)
    return response


def _errors(response: CollectGarbageResponse) -> list[str]:
    return [m.msg for m in response.logs.messages if isinstance(m, LogError)]


@pytest.mark.anyio
async def test_the_handler_censors_for_an_untrusted_client() -> None:
    message = _refusal(f"in use by '{magenta('{nix-process:1234}')}'")
    errors = _errors(await _answer(Role.USER, message))
    assert errors == [censored_refusal(message)]
    assert "1234" not in errors[0]


@pytest.mark.anyio
async def test_the_handler_keeps_the_names_for_a_trusted_client() -> None:
    message = _refusal(f"in use by '{magenta('{nix-process:1234}')}'")
    assert _errors(await _answer(Role.ADMIN, message)) == [message]
