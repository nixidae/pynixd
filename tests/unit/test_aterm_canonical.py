"""The ATerm parser accepts what Nix master's `derivation::parse` accepts, and refuses the rest with its message.

Nix's cases are `MalformedATermTest` and `RoundtripATermTest.everythingGoes` in
`src/libstore-tests/derivation/external-formats.cc` (NixOS/nix#16580). Its rules are
`src/libstore/derivation/aterm.cc`: canonical form (NixOS/nix#16284), canonical store paths
(NixOS/nix#16287), fixed-output paths against their content address (NixOS/nix#16318), and a 0xFF byte
read as a byte, not as end of input (NixOS/nix#16580). nixidae/nixidae#1671.
"""

from __future__ import annotations

import hashlib
import re

import pytest

from pynixd_store.content_address import content_addressed_path, fixed_output_address
from pynixd_store.drv_parser import Derivation, drv_bytes, drv_text, parse_drv, parse_drv_bytes
from pynixd_store.store_path import DrvOutput, StorePath

ANSI = re.compile(r"\x1b\[[0-9;]*m")
A = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo"
OUT = f'[("out","{A}","","")]'
TAIL = '"x86_64-linux","/bin/sh",[],[])'
SHA = "0000000000000000000000000000000000000000000000000000000000000000"


def drv(outputs: str = OUT, inputs: str = "[]", sources: str = "[]", rest: str = TAIL) -> str:
    return f"Derive({outputs},{inputs},{sources},{rest}"


def refusal(text: str, name: str | None = None) -> str:
    with pytest.raises(ValueError) as refused:  # noqa: PT011 -- the message is the assertion
        parse_drv(text, name=name)
    return ANSI.sub("", str(refused.value))


NIX_CASES = [
    pytest.param("", "expected 'D' character", id="truncated_1"),
    pytest.param("D", "does not start with", id="truncated_2"),
    pytest.param("Derive(", "expected '[' character", id="truncated_3"),
    pytest.param("Derive([", "expected '(' character", id="truncated_4"),
    pytest.param('Derive([("out","', "unterminated", id="truncated_5"),
    pytest.param('Derive([("out","\xff', "unterminated", id="truncated_6"),
    pytest.param(
        'DrvWithVersion("invalid-version",',
        "unknown derivation ATerm format version 'invalid-version'",
        id="bad_version",
    ),
    pytest.param(
        'Derive([("out\\n")', "unexpected escape sequence in unquoted string", id="escapes_rejected_in_output_name"
    ),
    pytest.param(
        f'Derive([("blahblah\x7f\x00","{A}","","")]', "expected ',' character", id="bad_character_in_output_name"
    ),
    pytest.param(f'Derive([("out","{A}', "unterminated string in derivation", id="unterminated_string"),
    pytest.param(
        'Derive([("out","/nix/store/\\naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo")',
        "unexpected escape sequence in unquoted string",
        id="escapes_rejected_in_store_path_string",
    ),
    pytest.param(f'Derive([("out","{A}","sha256\\0"))', "unknown escape '\\0'", id="invalid_escapes_in_hash_algo"),
    pytest.param(
        'Derive([("bar","/gnu/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo","","")',
        "path '/gnu/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo' is not in the Nix store",
        id="bad_store_path_different_store",
    ),
    pytest.param(
        'Derive([("bar","/nix/store/foo/../aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo","","")',
        "store path basename 'foo/../aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo' contains illegal base-32 character 'o'",
        id="bad_store_path_non_canonical",
    ),
]


@pytest.mark.parametrize(("text", "message"), NIX_CASES)
def test_nix_malformed_aterm(text: str, message: str) -> None:
    """`MalformedATermTest.throwsFormatError`, `external-formats.cc:20-95`."""
    assert message in refusal(text)


CASES = [
    pytest.param(" " + drv(), "expected 'D' character", id="whitespace_before"),
    pytest.param(drv(outputs=f'[ ("out","{A}","","")]'), "expected '(' character", id="whitespace_in_list"),
    pytest.param(drv(rest='"x86_64-linux", "/bin/sh",[],[])'), "expected '\"' character", id="whitespace_after_comma"),
    pytest.param(drv() + "\n", "expected end of file, found '\n'", id="trailing_newline"),
    pytest.param(drv() + "\xff", "expected end of file", id="trailing_byte"),
    pytest.param(drv(outputs=f'[("out","{A}","","")x'), "invalid list", id="invalid_list"),
    pytest.param(drv(rest='"x86_64-linux","/bin/sh",["a\nb"],[])'), "invalid character in string", id="raw_newline"),
    pytest.param(drv(rest='"x86_64-linux","/bin/sh",["a\tb"],[])'), "invalid character in string", id="raw_tab"),
    pytest.param(drv(rest='"x86_64-linux","/bin/sh",["a\rb"],[])'), "invalid character in string", id="raw_cr"),
    pytest.param(drv(rest='"x86_64-linux","/bin/sh",["a\\$b"],[])'), "unknown escape '\\$'", id="unknown_escape"),
    pytest.param(drv(rest='"x86_64-linux","/bin/sh",["a\\'), "unterminated string", id="escape_at_end"),
    pytest.param(drv(rest='"x86\\_64-linux","/bin/sh",[],[])'), "unexpected escape sequence", id="escape_in_platform"),
    pytest.param(
        drv(sources='["/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\\a-foo"]'),
        "unexpected escape sequence",
        id="escape_in_source",
    ),
    pytest.param(
        drv(inputs=f'[("{A}.drv",["o\\ut"])]'), "unexpected escape sequence", id="escape_in_input_output_name"
    ),
    pytest.param(drv(sources='["/nix/store/short"]'), "is too short to be a valid store path basename", id="short"),
    pytest.param(
        drv(sources='["/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_foo"]'),
        "hash part is not followed by a '-'",
        id="no_dash",
    ),
    pytest.param(
        drv(sources='["/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-f!o"]'),
        "path 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-f!o' is not a valid store path: name 'f!o' contains illegal character '!'",
        id="bad_name",
    ),
    pytest.param(
        drv(sources='["/nix/storeaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo"]'), "is not in the Nix store", id="no_slash"
    ),
    pytest.param(drv(inputs=f'[("{A}",["out"])]'), "is not a valid derivation path", id="input_not_drv"),
    pytest.param(
        drv(inputs=f'[("{A}.drv",[])]'), f"inputDrvs entry for '{A}.drv' specifies no outputs", id="no_outputs"
    ),
    pytest.param(
        "DrvWithVersion(" + f'"xp-dyn-drv",{OUT},[("{A}.drv",([],[]))],[],{TAIL}',
        f"inputDrvs entry for '{A}.drv' specifies no outputs",
        id="no_outputs_dynamic",
    ),
    pytest.param(
        "DrvWithVersion(" + f'"xp-dyn-drv",{OUT},[("{A}.drv","out")],[],{TAIL}',
        "invalid inputDrvs entry in derivation",
        id="bad_dynamic_entry",
    ),
    pytest.param(
        drv(outputs=f'[("out","{A}","","{SHA}")]'), "hash specified without hash algorithm", id="hash_no_algo"
    ),
    pytest.param(
        drv(outputs=f'[("out","{A}","r:sha256","impure")]'),
        "impure derivation output should not specify output path",
        id="impure_with_path",
    ),
    pytest.param(
        drv(outputs=f'[("out","{A}","r:sha256","")]'),
        "content-addressing derivation output should not specify output path",
        id="floating_with_path",
    ),
    pytest.param(
        drv(outputs='[("out","","sha256","' + SHA + '")]'),
        "path '' is not in the Nix store",
        id="fixed_without_path",
    ),
    pytest.param(drv(outputs='[("out","","md4","")]'), "unknown hash algorithm 'md4'", id="unknown_hash_algo"),
    pytest.param(
        drv(outputs=f'[("out","{A}","sha256","00")]'),
        "hash '00' has wrong length for hash algorithm 'sha256'",
        id="hash_wrong_length",
    ),
    pytest.param(
        drv(rest='"x86_64-linux","/bin/sh",[],[("__json","{")])'),
        "cannot process __json attribute",
        id="bad_structured_attrs",
    ),
    pytest.param(
        drv(rest='"x86_64-linux","/bin/sh",[],[("__json","NaN")])'),
        "cannot process __json attribute",
        id="structured_attrs_nan",
    ),
    pytest.param(
        drv(rest='"x86_64-linux","/bin/sh",[],[("__json","\\"' + drv_text(b"\xff") + '\\"")])'),
        "cannot process __json attribute: invalid UTF-8",
        id="structured_attrs_not_utf8",
    ),
]


def test_structured_attrs_parse() -> None:
    parsed = parse_drv(drv(rest='"x86_64-linux","/bin/sh",[],[("__json","{\\"a\\":[1,\\"\\\\u00e9\\"]}")])'))
    assert parsed.env["__json"] == '{"a":[1,"\\u00e9"]}'


@pytest.mark.parametrize(("text", "message"), CASES)
def test_refused(text: str, message: str) -> None:
    """Each rule of `aterm.cc` at its point, with its message."""
    assert message in refusal(text)


def test_fixed_output_path_must_match_its_address() -> None:
    """`parseOutput` (`aterm.cc:353`): the stated path of a fixed output is a function of its content address."""
    text = drv(outputs=f'[("out","{A}","sha256","{SHA}")]')
    assert "which does not match its content address" in refusal(text, name="foo")

    path = content_addressed_path(fixed_output_address("sha256", SHA), "foo", [], "/nix/store")
    parsed = parse_drv(drv(outputs=f'[("out","{path}","sha256","{SHA}")]'), name="foo")
    assert parsed.outputs[0].path == path


def test_output_names_and_child_keys_are_not_validated() -> None:
    """Nix accepts an output name with any byte but `"` and `\\`, and a child-map key with escapes.

    The FIXME at `external-formats.cc:52` says output names are not checked. A dynamic child-map key goes
    through `parseString` (`aterm.cc:426`) while every other output name goes through `parseUnquotedString`.
    """
    parsed = parse_drv(drv(outputs=f'[("o\x00\x7f\n","{A}","","")]'))
    assert parsed.outputs[0].output_name == "o\x00\x7f\n"

    dynamic = parse_drv("DrvWithVersion(" + f'"xp-dyn-drv",{OUT},[("{A}.drv",([],[("o\\"ut",["x"])]))],[],{TAIL}')
    assert list(dynamic.dynamic_input_drvs[StorePath(f"{A}.drv")].children) == ['o"ut']


ALL_BYTES = bytes(range(256))
OUTPUT_NAME = "+_-._?=abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890"
NIX_GOLDEN_SHA256 = "f9a3ce12614698beed9ce9f5b840a1fcc343ab28162a063ec8dd69ef0f26a5ef"
"""sha256 of `src/libstore-tests/data/derivation/aterm-encoding.drv` at NixOS/nix#16580's merge, 1998 bytes."""


def _every_byte() -> Derivation:
    """`RoundtripATermTest.everythingGoes` (`external-formats.cc:100`): every byte in every free-form field."""
    every = drv_text(ALL_BYTES)
    # Nix writes the platform verbatim, so the bytes that need an escape are swapped for NUL to parse again.
    platform = drv_text(bytes(0 if b in b'\\\n\t\r"' else b for b in ALL_BYTES))
    path = f"/nix/store/j5v9jrrqr8yn981qi5x5dvcx075j29gm-foo-{OUTPUT_NAME}"
    return Derivation(
        outputs=[DrvOutput(output_name=OUTPUT_NAME, path=path, hash_algo="", hash_value="")],
        platform=platform,
        builder=every,
        args=[every, every],
        env={every: every, OUTPUT_NAME: path},
    )


@pytest.mark.anyio
async def test_every_byte_round_trips_as_nix_writes_it() -> None:
    """0xFF is a byte, not end of input: NixOS/nix#16580 fixed the signed `char` that read it as `EOF`."""
    text = _every_byte().unparse()
    raw = drv_bytes(text)
    assert hashlib.sha256(raw).hexdigest() == NIX_GOLDEN_SHA256

    parsed = await parse_drv_bytes(raw, name="foo")
    assert drv_bytes(parsed.builder) == ALL_BYTES
    assert drv_bytes(parsed.unparse()) == raw
