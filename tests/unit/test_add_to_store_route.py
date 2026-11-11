"""What AddToStore's header decides: the method it names, and whether pynixd or the daemon adds the dump."""

from __future__ import annotations

import pytest

from pynixd.write_router import Dump, Facts, Write, first_reason, parse_dump_header
from pynixd_store.add import ImportSettings
from pynixd_store.content_address import Method, parse_method_algo, text_needs_sha256
from pynixd_store.hintfmt import magenta, missing_feature

REF = "/nix/store/" + "a" * 32 + "-ref"
FACTS = Facts(writable=True, settings=ImportSettings())
NONE: frozenset[str] = frozenset()


@pytest.mark.parametrize(
    ("text", "parsed"),
    [
        ("text:sha256", (Method.TEXT, "sha256")),
        ("fixed:r:sha256", (Method.NAR, "sha256")),
        ("fixed:r:sha512", (Method.NAR, "sha512")),
        ("fixed:sha1", (Method.FLAT, "sha1")),
        # `parseWithAlgo` reads up to the algorithm and ignores the rest.
        ("fixed:sha256:more", (Method.FLAT, "sha256")),
    ],
)
def test_parse_method_algo(text: str, parsed: tuple[Method, str]) -> None:
    assert parse_method_algo(text, NONE) == parsed


def test_parse_method_algo_takes_a_gated_algorithm_with_its_feature() -> None:
    assert parse_method_algo("fixed:git:sha1", frozenset({"git-hashing"})) == (Method.GIT, "sha1")
    assert parse_method_algo("fixed:r:blake3", frozenset({"blake3-hashes"})) == (Method.NAR, "blake3")


_EXPECT = "expect 'blake3', 'md5', 'sha1', 'sha256', or 'sha512'"


@pytest.mark.parametrize(
    ("text", "refusal"),
    [
        (
            "sha256",
            f"content address prefix '{magenta('sha256')}' is unrecognized. Recogonized prefixes are 'text' or 'fixed'",
        ),
        ("fixed:r", f"content address hash must be in form '<algo>:<hash>', but found: {magenta('fixed:r:')}"),
        ("fixed:r:md4", f"unknown hash algorithm '{magenta('md4')}', {_EXPECT}"),
        ("text:", f"unknown hash algorithm '{magenta('')}', {_EXPECT}"),
        ("fixed:r:blake3", missing_feature("blake3-hashes")),
        ("fixed:git:sha1", missing_feature("git-hashing")),
    ],
)
def test_parse_method_algo_refuses_in_nix_words(text: str, refusal: str) -> None:
    """`parseContentAddressMethodPrefix` (`content-address.cc:143`), with the feature gates of `hash.cc:475`."""
    with pytest.raises(ValueError) as refused:
        parse_method_algo(text, NONE)
    assert str(refused.value) == refusal


def test_a_text_header_with_another_algorithm_is_refused() -> None:
    with pytest.raises(ValueError) as refused:
        parse_dump_header(Dump("f", "text:sha1", frozenset()), NONE)
    assert str(refused.value) == text_needs_sha256("f", "sha1")


def _why(name: str, method: str, refs: list[str], facts: Facts = FACTS) -> str | None:
    dump = Dump(name, method, frozenset(refs))
    return first_reason(Write(op="AddToStore", dump=dump), facts)


def test_pynixd_adds_what_it_can_hash() -> None:
    assert _why("drv.drv", "text:sha256", [REF]) is None
    assert _why("src", "fixed:r:sha256", [REF]) is None
    assert _why("f", "fixed:sha1", []) is None
    # pynixd refuses these itself, in Nix's words, when it makes the path.
    assert _why("bad name", "fixed:r:sha256", []) is None
    assert _why("f", "fixed:sha256", [REF]) is None


def test_pynixd_adds_a_git_dump_with_git_hashing() -> None:
    facts = Facts(
        writable=True, settings=ImportSettings(git_hashing=True, experimental_features=frozenset({"git-hashing"}))
    )
    assert _why("f", "fixed:git:sha1", [], facts) is None


def test_the_daemon_hashes_with_blake3() -> None:
    """Python's `hashlib` has no BLAKE3, so with `blake3-hashes` such a dump goes to the daemon."""
    facts = Facts(writable=True, settings=ImportSettings(experimental_features=frozenset({"blake3-hashes"})))
    assert _why("f", "fixed:r:blake3", [], facts) == "pynixd does not hash with BLAKE3"
