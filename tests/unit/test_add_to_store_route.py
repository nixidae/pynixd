"""What AddToStore's header decides: the method it names, and whether pynixd or the daemon adds the dump."""

from __future__ import annotations

import pytest

from pynixd.write_router import Dump, Facts, Write, first_reason
from pynixd_store.add import ImportSettings
from pynixd_store.content_address import Method, parse_method_algo

REF = "/nix/store/" + "a" * 32 + "-ref"
FACTS = Facts(writable=True, settings=ImportSettings())


@pytest.mark.parametrize(
    ("text", "parsed"),
    [
        ("text:sha256", (Method.TEXT, "sha256")),
        ("fixed:r:sha256", (Method.NAR, "sha256")),
        ("fixed:r:sha512", (Method.NAR, "sha512")),
        ("fixed:sha1", (Method.FLAT, "sha1")),
        ("fixed:git:sha1", (Method.GIT, "sha1")),
    ],
)
def test_parse_method_algo(text: str, parsed: tuple[Method, str]) -> None:
    assert parse_method_algo(text) == parsed


@pytest.mark.parametrize("text", ["sha256", "fixed:r", "fixed:r:blake3", "other:sha256", "text:"])
def test_parse_method_algo_refuses(text: str) -> None:
    with pytest.raises(ValueError, match=r"content address|hash algorithm"):
        parse_method_algo(text)


def _why(name: str, method: str, refs: list[str]) -> str | None:
    dump = Dump(name, method, frozenset(refs))
    return first_reason(Write(op="AddToStore", dump=dump), FACTS)


def test_pynixd_adds_what_it_can_hash() -> None:
    assert _why("drv.drv", "text:sha256", [REF]) is None
    assert _why("src", "fixed:r:sha256", [REF]) is None
    assert _why("f", "fixed:sha1", []) is None


def test_pynixd_adds_a_git_dump_with_git_hashing() -> None:
    dump = Dump("f", "fixed:git:sha1", frozenset())
    facts = Facts(writable=True, settings=ImportSettings(git_hashing=True))
    assert first_reason(Write(op="AddToStore", dump=dump), facts) is None


@pytest.mark.parametrize(
    ("name", "method", "refs", "reason"),
    [
        ("f", "fixed:git:sha1", [], "Git"),
        ("f", "fixed:blake3", [], "unknown hash algorithm"),
        ("f", "text:sha1", [], "text hashing requires sha256"),
        ("f", "fixed:sha256", [REF], "not allowed to refer to other store paths"),
        ("bad name", "fixed:r:sha256", [], "illegal character"),
    ],
)
def test_the_daemon_adds_what_pynixd_does_not_reproduce(name: str, method: str, refs: list[str], reason: str) -> None:
    """Nix's refusal of each comes after `startWork`, and only the daemon gives its words."""
    why = _why(name, method, refs)
    assert why is not None
    assert reason in why
