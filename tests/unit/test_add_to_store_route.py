"""What AddToStore's header decides: the method it names, and whether pynixd or the daemon adds the dump."""

from __future__ import annotations

import pytest

from pynixd.constants import proto
from pynixd.content_address import Method, parse_method_algo
from pynixd.store.local_db import LocalDBStore

NEW = proto(1, 38)
REF = "/nix/store/" + "a" * 32 + "-ref"


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


def test_pynixd_adds_what_it_can_hash() -> None:
    assert LocalDBStore.why_the_daemon_adds("drv.drv", "text:sha256", [REF], version=NEW) is None
    assert LocalDBStore.why_the_daemon_adds("src", "fixed:r:sha256", [REF], version=NEW) is None
    assert LocalDBStore.why_the_daemon_adds("f", "fixed:sha1", [], version=NEW) is None


@pytest.mark.parametrize(
    ("name", "method", "refs", "version", "reason"),
    [
        ("f", "fixed:r:sha256", [], proto(1, 24), "unframed"),
        ("f", "fixed:git:sha1", [], NEW, "Git"),
        ("f", "fixed:blake3", [], NEW, "unknown hash algorithm"),
        ("f", "text:sha1", [], NEW, "text hashing requires sha256"),
        ("f", "fixed:sha256", [REF], NEW, "not allowed to refer to other store paths"),
        ("bad name", "fixed:r:sha256", [], NEW, "illegal character"),
    ],
)
def test_the_daemon_adds_what_pynixd_does_not_reproduce(
    name: str, method: str, refs: list[str], version: int, reason: str
) -> None:
    """Nix's refusal of each comes after `startWork`, and only the daemon gives its words."""
    why = LocalDBStore.why_the_daemon_adds(name, method, refs, version=version)
    assert why is not None
    assert reason in why
