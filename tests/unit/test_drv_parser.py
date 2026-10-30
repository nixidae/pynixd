"""Unit tests for pynixd.drv_parser — ATerm .drv file parser.

Tests are split into two categories:
1. **Live probes**: Evaluate ``tests/nix/drv-probes.nix`` once per session via
   ``nix eval``, read the real .drv files, and validate parsing + to_json().
2. **Manufactured edge cases**: Static inline strings for parser features that
   are hard to produce via nix (escaped strings, empty env, deferred outputs, etc).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any

import anyio
import pytest

from pynixd import drv_parser
from pynixd.drv_parser import Derivation, parse_drv, to_basic_derivation
from pynixd.serde import OutputKind
from pynixd.store_layout import StoreLayout
from pynixd.store_path import DrvOutput, StorePath
from tests.conftest import NIX_BIN

if TYPE_CHECKING:
    from nix_daemon_protocol.aliases import OutputMap

_PROBES_NIX = Path(__file__).parent.parent.parent / "tests" / "nix" / "drv-probes.nix"

type Probes = dict[str, tuple[str, str, dict[str, Any]]]


# ── Session-scoped fixture: evaluate drv-probes.nix once ───────────────────


class _ProbesUnavailable(Exception):
    """`nix eval` named a `.drv` and the store does not hold it.

    A build sandbox without the `recursive-nix` feature has no daemon socket,
    so an evaluation computes a derivation path and writes nothing. Issue #38.
    """

    def __init__(self, drv_path: str) -> None:
        super().__init__(
            f"no store holds {drv_path}, so no probe can be read. "
            "A build sandbox needs the recursive-nix feature for that, and "
            "tests/derivations/pytest/default.nix does not ask for it.",
        )


@pytest.fixture(scope="session")
def drv_probes_path() -> Path:
    return _PROBES_NIX


@pytest.fixture(scope="session")
def probes(drv_probes_path: Path) -> Probes:
    """Evaluate all entries in drv-probes.nix.

    Returns {name: (drv_path, drv_content, canonical_derivation_show)}.
    Evaluated once per test session.
    """

    # **A Nix binary is not the thing these need.** `tests.pytest` puts
    # `pkgs.nix` in the builder and exports `NIX_BIN`, so this check passed
    # there and the 21 tests it was written to skip errored anyway, with
    # `FileNotFoundError: /nix/store/...-ca-fixed.drv`. Issue #38.
    #
    # What they need is a store that `nix eval` can write a `.drv` into.
    # `tests/derivations/pytest/default.nix` dropped
    # `requiredSystemFeatures = [ "recursive-nix" ]`, because nothing here
    # advertises that feature and Nix waits rather than failing -- one build
    # sat 5h41m on a second of CPU. So the builder has no daemon socket,
    # `drvPath` still evaluates to a path, and nothing puts the file there.
    #
    # The check is therefore the operation itself, asked once. Issue #38.
    if shutil.which(str(NIX_BIN)) is None:
        pytest.skip(f"{NIX_BIN} is not on PATH, so no probe can be evaluated")

    async def _eval_all() -> Probes:
        # List attribute names using --apply
        proc = await asyncio.create_subprocess_exec(
            NIX_BIN,
            "eval",
            "--impure",
            "--file",
            str(drv_probes_path),
            "--apply",
            "builtins.attrNames",
            "--json",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"Failed to list probes: {stderr.decode()}")
        names = json.loads(stdout.decode())

        result: Probes = {}
        for name in names:
            proc = await asyncio.create_subprocess_exec(
                NIX_BIN,
                "eval",
                "--impure",
                "--file",
                str(drv_probes_path),
                f"{name}.drvPath",
                "--raw",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError(f"eval {name}.drvPath failed: {stderr.decode()}")
            drv_path = stdout.decode().strip()
            try:
                drv_content = await anyio.Path(drv_path).read_text()
            except FileNotFoundError as ex:
                raise _ProbesUnavailable(drv_path) from ex

            proc = await asyncio.create_subprocess_exec(
                NIX_BIN,
                "derivation",
                "show",
                drv_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError(f"derivation show {name} failed: {stderr.decode()}")
            canonical = json.loads(stdout.decode())

            result[name] = (drv_path, drv_content, canonical)
        return result

    try:
        return asyncio.run(_eval_all())
    except _ProbesUnavailable as ex:
        pytest.skip(str(ex))


# ── Helpers for extracting data from canonical JSON ────────────────────────


def _canonical_entry(canonical: dict[str, Any]) -> dict[str, Any]:
    """Get the single derivation entry from ``nix derivation show`` output.

    Handles both ``{"derivations": {...}, "version": 4}`` and the flat format.
    """
    d = canonical.get("derivations", canonical)
    entry: dict[str, Any] = next(iter(d.values()))
    return entry


def _canonical_drv_key(canonical: dict[str, Any]) -> str:
    d = canonical.get("derivations", canonical)
    key: str = next(iter(d))
    return key


# ── Live probes tests ──────────────────────────────────────────────────────


class TestLiveProbes:
    """Tests using real derivations from tests/nix/drv-probes.nix.

    Each test parses the .drv file and validates against
    ``nix derivation show`` canonical JSON.
    """

    def test_simple_env(self, probes: Probes) -> None:
        _, drv_content, canonical = probes["simple"]
        parsed = parse_drv(drv_content)
        result = parsed.to_json(_canonical_drv_key(canonical))
        entry = result[_canonical_drv_key(canonical)]
        assert entry["env"] == _canonical_entry(canonical)["env"]

    def test_simple_metadata(self, probes: Probes) -> None:
        _, drv_content, canonical = probes["simple"]
        parsed = parse_drv(drv_content)
        entry = _canonical_entry(canonical)
        assert parsed.platform == entry["system"]
        assert parsed.builder == entry["builder"]

    def test_simple_output_kind(self, probes: Probes) -> None:
        _, drv_content, _ = probes["simple"]
        parsed = parse_drv(drv_content)
        assert parsed.output_kinds() == [OutputKind.INPUT_ADDRESSED]

    def test_ca_floating_env(self, probes: Probes) -> None:
        _, drv_content, canonical = probes["ca-floating"]
        parsed = parse_drv(drv_content)
        result = parsed.to_json(_canonical_drv_key(canonical))
        assert result[_canonical_drv_key(canonical)]["env"] == _canonical_entry(canonical)["env"]

    def test_ca_floating_output_kind(self, probes: Probes) -> None:
        _, drv_content, _ = probes["ca-floating"]
        parsed = parse_drv(drv_content)
        assert parsed.output_kinds() == [OutputKind.CA_FLOATING]

    def test_ca_floating_no_output_path(self, probes: Probes) -> None:
        _, drv_content, _ = probes["ca-floating"]
        parsed = parse_drv(drv_content)
        assert parsed.outputs[0].path == ""

    def test_ca_fixed_env(self, probes: Probes) -> None:
        _, drv_content, canonical = probes["ca-fixed"]
        parsed = parse_drv(drv_content)
        result = parsed.to_json(_canonical_drv_key(canonical))
        assert result[_canonical_drv_key(canonical)]["env"] == _canonical_entry(canonical)["env"]

    def test_ca_fixed_output_kind(self, probes: Probes) -> None:
        _, drv_content, _ = probes["ca-fixed"]
        parsed = parse_drv(drv_content)
        # Fixed CA: path + hash_algo + hash_value all set
        assert parsed.output_kinds() == [OutputKind.CA_FIXED]

    def test_text_hashed_env(self, probes: Probes) -> None:
        _, drv_content, canonical = probes["text-hashed"]
        parsed = parse_drv(drv_content)
        result = parsed.to_json(_canonical_drv_key(canonical))
        assert result[_canonical_drv_key(canonical)]["env"] == _canonical_entry(canonical)["env"]

    def test_dynamic_env(self, probes: Probes) -> None:
        _, drv_content, canonical = probes["dynamic"]
        parsed = parse_drv(drv_content)
        result = parsed.to_json(_canonical_drv_key(canonical))
        assert result[_canonical_drv_key(canonical)]["env"] == _canonical_entry(canonical)["env"]

    def test_dynamic_has_dynamic_flag(self, probes: Probes) -> None:
        _, drv_content, _ = probes["dynamic"]
        parsed = parse_drv(drv_content)
        assert parsed.env["__dynamicDerivation"] == "1"
        assert parsed.required_system_features == {"recursive-nix"}

    def test_with_features(self, probes: Probes) -> None:
        _, drv_content, _ = probes["with-features"]
        parsed = parse_drv(drv_content)
        assert parsed.required_system_features == {"kvm", "big-parallel"}

    def test_multiple_input_drvs(self, probes: Probes) -> None:
        """Most probes have multiple input derivations (stdenv + bash)."""
        _, drv_content, _ = probes["simple"]
        parsed = parse_drv(drv_content)
        assert len(parsed.input_drvs) == 2
        # runCommand includes source-stdenv.sh as input srcs
        assert len(parsed.input_srcs) == 2

    def test_ca_has_input_srcs(self, probes: Probes) -> None:
        """CA derivations include source-stdenv.sh as input sources."""
        _, drv_content, _ = probes["ca-floating"]
        parsed = parse_drv(drv_content)
        assert len(parsed.input_srcs) == 2  # source-stdenv.sh + default-builder.sh

    def test_minimal(self, probes: Probes) -> None:
        """stdenvNoCC.mkDerivation — minimal build environment."""
        _, drv_content, canonical = probes["minimal"]
        parsed = parse_drv(drv_content)
        result = parsed.to_json(_canonical_drv_key(canonical))
        assert result[_canonical_drv_key(canonical)]["env"] == _canonical_entry(canonical)["env"]
        assert parsed.platform == "x86_64-linux"

    def test_to_json_matches_canonical(self, probes: Probes) -> None:
        """For every probe, verify to_json() produces the same env as nix derivation show."""
        for name, (_, drv_content, canonical) in probes.items():
            parsed = parse_drv(drv_content)
            result = parsed.to_json(_canonical_drv_key(canonical))
            entry = _canonical_entry(canonical)
            # We compare env dicts — should be identical
            assert result[_canonical_drv_key(canonical)]["env"] == entry["env"], f"env mismatch for {name}"


# ── Manufactured edge cases ────────────────────────────────────────────────


class TestManufacturedExamples:
    """Tests using static inline strings for parser features hard to produce via nix."""

    def test_deferred(self) -> None:
        parsed = parse_drv('Derive([("out","","","")],[],[],"x86_64-linux","/bin/sh",["-c","true"],[("name","def")])')
        assert parsed.output_kinds() == [OutputKind.DEFERRED]

    def test_empty_env(self) -> None:
        parsed = parse_drv('Derive([("out","/nix/store/abc-foo","","")],[],[],"x86_64-linux","/bin/sh",[],[])')
        assert parsed.env == {}

    def test_escaped_strings(self) -> None:
        parsed = parse_drv(
            'Derive([("out","/nix/store/abc-foo","","")]'
            ',[],[],"x86_64-linux"'
            ',"/bin/sh",["-c","echo \\"hello\\""]'
            ',[("name","esc"),("desc","line1\\nline2\\ttab")])'
        )
        assert parsed.args == ["-c", 'echo "hello"']
        assert parsed.env["desc"] == "line1\nline2\ttab"

    @pytest.mark.parametrize(
        "body",
        [
            "",
            "plain",
            '\\"quoted\\"',
            "\\\\",
            "\\\\n",
            "\\\\\\n",
            '\\\\\\"',
            "\\n\\r\\t",
            "\\a\\z\\$\\'",
            "\\\\\\\\\\x",
            "nul\0and\\\\\\n\\q",
        ],
    )
    def test_escapes_read_as_nix_reads_them(self, body: str) -> None:
        """`parseString` at `derivations.cc:188-205`, one escape at a time.

        The parser unescapes by `str.replace` (nixidae/nixidae#1139); this is
        the loop it must agree with, NUL fallback included.
        """
        expected: list[str] = []
        chars = iter(body)
        for ch in chars:
            if ch == "\\":
                esc = next(chars)
                expected.append({"n": "\n", "r": "\r", "t": "\t"}.get(esc, esc))
            else:
                expected.append(ch)
        parsed = parse_drv(
            f'Derive([("out","/nix/store/abc-foo","","")],[],[],"x86_64-linux","/bin/sh",[],[("v","{body}")])'
        )
        assert parsed.env["v"] == "".join(expected)

    @pytest.mark.parametrize("piece", [1, 2, 3, 5, 8])
    def test_pieces_never_split_an_escape(self, piece: int, monkeypatch: pytest.MonkeyPatch) -> None:
        """A large value is scanned and unescaped in `_PIECE`-sized calls (nixidae/nixidae#1139).

        Every cut lands inside some escape for one of these sizes.
        """
        body = '\\\\a\\"b\\\\\\nc\\t\\\\\\\\\\"d\\q' * 3
        whole = parse_drv(
            f'Derive([("out","/nix/store/abc-foo","","")],[],[],"x86_64-linux","/bin/sh",[],[("v","{body}")])'
        )
        monkeypatch.setattr(drv_parser, "_PIECE", piece)
        cut = parse_drv(
            f'Derive([("out","/nix/store/abc-foo","","")],[],[],"x86_64-linux","/bin/sh",[],[("v","{body}")])'
        )
        assert cut.env["v"] == whole.env["v"]

    async def test_a_large_drv_parses_off_the_loop(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Above `_INLINE_PARSE_BYTES` the parse runs on a worker thread."""
        threads: list[int] = []
        parse = drv_parser.parse_drv

        def recording(content: str) -> Derivation:
            threads.append(threading.get_ident())
            return parse(content)

        monkeypatch.setattr(drv_parser, "parse_drv", recording)
        value = "x" * (drv_parser._INLINE_PARSE_BYTES + 1)
        raw = f'Derive([("out","/nix/store/abc-foo","","")],[],[],"x86_64-linux","/bin/sh",[],[("v","{value}")])'
        parsed = await drv_parser.parse_drv_bytes(raw.encode())
        assert parsed.env["v"] == value
        assert threads
        assert threading.get_ident() not in threads

    def test_multiple_outputs(self) -> None:
        parsed = parse_drv(
            'Derive([("out","/nix/store/a-out","",""),("lib","/nix/store/a-lib","","")]'
            ',[],[],"x86_64-linux"'
            ',"/bin/sh",["-c","make"]'
            ',[("name","multi"),("outputs","out lib")])'
        )
        assert len(parsed.outputs) == 2

    def test_required_system_features(self) -> None:
        parsed = parse_drv(
            'Derive([("out","/nix/store/a-foo","","")]'
            ',[],[],"x86_64-linux"'
            ',"/bin/sh",["-c","true"]'
            ',[("name","feat"),("requiredSystemFeatures","kvm big-parallel")])'
        )
        assert parsed.required_system_features == {"kvm", "big-parallel"}


class TestDrvWithVersion:
    """Tests for the DrvWithVersion ATerm format.

    This format is only produced by Nix >= 2.18 for true dynamic derivations.
    The example below is manufactured based on the format spec.
    """

    def test_dynamic_format(self) -> None:
        parsed = parse_drv(
            'DrvWithVersion("xp-dyn-drv",'
            '[("out","","text:sha256","")]'
            ',[("/nix/store/dep.drv",([],[])),'
            '("/nix/store/simple.drv",["out"])]'
            ",[]"
            ',"x86_64-linux"'
            ',"/bin/sh",["-c","true"]'
            ',[("name","dyn")])'
        )
        assert parsed.is_dynamic
        assert StorePath("/nix/store/dep.drv") in parsed.dynamic_input_drvs
        assert StorePath("/nix/store/simple.drv") in parsed.input_drvs
        assert parsed.input_drvs[StorePath("/nix/store/simple.drv")] == ["out"]
        from pynixd.drv_parser import ChildMapNode

        assert parsed.dynamic_input_drvs[StorePath("/nix/store/dep.drv")] == ChildMapNode()


class TestDerivationProperties:
    """Tests for Derivation property methods with explicit data."""

    def test_required_system_features_empty(self) -> None:
        assert Derivation(env={}).required_system_features == set()

    def test_output_paths(self) -> None:
        parsed = Derivation(
            outputs=[
                DrvOutput(output_name="out", path="/nix/store/abc-foo", hash_algo="", hash_value=""),
            ],
        )
        assert parsed.output_paths() == {"out": StorePath("/nix/store/abc-foo")}

    def test_output_kinds_mixed(self) -> None:
        parsed = Derivation(
            outputs=[
                DrvOutput(output_name="ia", path="/nix/store/a", hash_algo="", hash_value=""),
                DrvOutput(output_name="ca", path="", hash_algo="sha256", hash_value="xyz"),
                DrvOutput(output_name="flt", path="", hash_algo="sha256", hash_value=""),
            ],
        )
        assert parsed.output_kinds() == [
            OutputKind.INPUT_ADDRESSED,
            OutputKind.CA_FIXED,
            OutputKind.CA_FLOATING,
        ]

    def test_name(self, probes: Probes) -> None:
        """Name should be derived from the store path (hash-name.drv)."""
        for name, (drv_path, drv_content, _) in probes.items():
            parsed = parse_drv(drv_content)
            # parsed.name is derived from to_json; verify drv_path contains the expected name
            assert parsed.env.get("name") in drv_path, f"{name}: expected {parsed.env.get('name')} in {drv_path}"


class TestOutputClassification:
    """What kind each output is, and what follows from that.

    **`OutputKind` is the one place that classifies an output.** Three callers
    read the two raw fields of the `.drv` instead, and each one got a
    different answer for an impure output, which carries `r:sha256` in the
    algorithm and the word `impure` in the digest. One of the three read that
    as a content hash and called the derivation fixed-output.
    """

    def test_selected_output_paths_takes_the_named_ones(self) -> None:
        parsed = Derivation(
            outputs=[
                DrvOutput(output_name="out", path="/nix/store/a-out", hash_algo="", hash_value=""),
                DrvOutput(output_name="dev", path="/nix/store/a-dev", hash_algo="", hash_value=""),
            ],
        )

        assert parsed.selected_output_paths({"dev"}) == {"dev": StorePath("/nix/store/a-dev")}
        assert parsed.selected_output_paths({"*"}) == parsed.output_paths()

    def test_an_impure_derivation_registers_no_realisation(self) -> None:
        """Nix guards the registration, at `derivation-goal.cc:226`.

        Every build of an impure derivation makes a new output, so one id
        cannot hold two. The daemon answers "Trying to register a realisation
        of '...', but we already have another one locally", and the connection
        goes out of the pool as dirty.

        An impure output names no path, which is the rule that
        `needs_realisations` reads, so the answer needs the exception.
        """
        parsed = Derivation(
            outputs=[DrvOutput(output_name="out", path="", hash_algo="r:sha256", hash_value="impure")],
        )

        assert all(not output.path for output in parsed.outputs)
        assert parsed.needs_realisations is False

    def test_a_floating_output_does_register_a_realisation(self) -> None:
        parsed = Derivation(
            outputs=[DrvOutput(output_name="out", path="", hash_algo="r:sha256", hash_value="")],
        )

        assert parsed.output_kinds() == [OutputKind.CA_FLOATING]
        assert parsed.needs_realisations is True

    def test_an_impure_output_is_impure_and_not_fixed(self) -> None:
        """`("out","","r:sha256","impure")` is what Nix writes for one.

        Three places asked this question by reading the two raw fields, and
        one of them read `impure` in the digest as a content hash and called
        the derivation fixed-output. `OutputKind` is the one place that
        classifies an output.
        """
        parsed = Derivation(
            outputs=[DrvOutput(output_name="out", path="", hash_algo="r:sha256", hash_value="impure")],
        )

        assert parsed.output_kinds() == [OutputKind.IMPURE]
        assert parsed.is_impure
        assert not parsed.is_fixed_output

    def test_a_fixed_output_derivation_is_fixed(self) -> None:
        parsed = Derivation(
            outputs=[DrvOutput(output_name="out", path="/nix/store/a-out", hash_algo="sha256", hash_value="abc")],
        )

        assert parsed.is_fixed_output
        assert not parsed.is_impure

    def test_the_hash_of_an_impure_derivation_comes_from_its_aterm(self) -> None:
        """`hashDerivationModulo` at `derivations.cc:902` asks `type().isFixed()`.

        An impure derivation answers no, so its hash is the SHA-256 of the
        masked ATerm and not `fixed:out:...`. pynixd took the fixed branch,
        so every realisation of an impure derivation carried an id that no
        Nix agrees with.
        """
        parsed = parse_drv(
            'Derive([("out","","r:sha256","impure")],[],[],"x86_64-linux","/bin/sh",'
            '["-c","true"],[("name","impure"),("out","/1abc")])'
        )

        expected = hashlib.sha256(parsed.unparse(maskOutputs=True).encode()).hexdigest()

        assert parsed.hash_derivation_modulo(mask_outputs=True) == {"out": expected}

    def test_an_input_addressed_derivation_with_an_input_does_not_resolve(self) -> None:
        """`Derivation::shouldResolve` at `derivations.cc:1129`."""
        plain = DrvOutput(output_name="out", path="/nix/store/a-out", hash_algo="", hash_value="")
        deferred = DrvOutput(output_name="out", path="", hash_algo="", hash_value="")
        inputs = {StorePath("/nix/store/b-dep.drv"): ["out"]}

        assert not Derivation(outputs=[plain], input_drvs=inputs).should_resolve(ca_derivations=True)
        assert not Derivation(outputs=[plain]).should_resolve(ca_derivations=True)
        assert Derivation(outputs=[deferred], input_drvs=inputs).should_resolve(ca_derivations=False)

    def test_a_fixed_output_derivation_resolves_only_with_ca_derivations(self) -> None:
        """`derivations.cc:1134`: resolving one is optional, and the feature decides. nixidae/nixidae#1010."""
        fixed = DrvOutput(output_name="out", path="/nix/store/a-src", hash_algo="sha256", hash_value="00" * 32)
        drv = Derivation(outputs=[fixed], input_drvs={StorePath("/nix/store/b-curl.drv"): ["out"]})

        assert drv.should_resolve(ca_derivations=True)
        assert not drv.should_resolve(ca_derivations=False)


class TestParseDrvEdgeCases:
    def test_invalid_syntax(self) -> None:
        with pytest.raises(ValueError):  # noqa: PT011
            parse_drv("Derive(invalid")

    def test_unterminated_string(self) -> None:
        with pytest.raises(ValueError, match="Unterminated string"):
            parse_drv('Derive([("out","abc')

    def test_unknown_version(self) -> None:
        with pytest.raises(ValueError, match="Unknown derivation ATerm version"):
            parse_drv('DrvWithVersion("unknown-version",[]')

    def test_empty_text(self) -> None:
        with pytest.raises(ValueError):  # noqa: PT011
            parse_drv("")


class TestToBasicDerivation:
    """to_basic_derivation with mocked output_cache avoids disk I/O."""

    async def test_simple_conversion(self, probes: Probes) -> None:

        _, drv_content, _ = probes["simple"]
        parsed = parse_drv(drv_content)
        result = await to_basic_derivation(parsed, StoreLayout.chroot(Path("/tmp/fake-store")))
        assert len(result.outputs) == 1
        assert result.platform == "x86_64-linux"

    async def test_with_output_cache(self, probes: Probes) -> None:

        _, drv_content, _ = probes["simple"]
        parsed = parse_drv(drv_content)
        drv = next(iter(parsed.input_drvs.keys()))
        out_name = parsed.input_drvs[drv][0]
        cache: OutputMap = {drv: {out_name: StorePath(f"/nix/store/realized-{out_name}")}}
        result = await to_basic_derivation(parsed, StoreLayout.chroot(Path("/tmp/fake-store")), output_cache=cache)
        assert StorePath(path=f"/nix/store/realized-{out_name}") in result.input_srcs

    async def test_cache_missing_adds_drv(self, probes: Probes) -> None:

        _, drv_content, _ = probes["simple"]
        parsed = parse_drv(drv_content)
        result = await to_basic_derivation(parsed, StoreLayout.chroot(Path("/tmp/fake-store")))
        for drv_path in parsed.input_drvs:
            assert StorePath(path=str(drv_path)) in result.input_srcs


class TestDrvOutputFields:
    def test_fields(self) -> None:
        o = DrvOutput(hash_algo="sha256", hash_value="xyz", output_name="out", path="/nix/store/a")
        assert o.hash_algo == "sha256"
        assert o.hash_value == "xyz"
        assert o.output_name == "out"
        assert o.path == "/nix/store/a"
        assert o.name == "out"


class TestToJson:
    def test_serializable(self, probes: Probes) -> None:
        _, drv_content, _ = probes["simple"]
        parsed = parse_drv(drv_content)
        json.dumps(parsed.to_json("/nix/store/test.drv"))

    def test_ca_no_path(self, probes: Probes) -> None:
        _, drv_content, _ = probes["ca-floating"]
        parsed = parse_drv(drv_content)
        result = parsed.to_json("/nix/store/ca.drv")
        out_entry = result["/nix/store/ca.drv"]["outputs"]["out"]
        assert "path" not in out_entry


class TestSerialize:
    """Tests for Derivation.serialize() roundtrips."""

    def test_traditional_roundtrip(self) -> None:
        text = (
            'Derive([("out","/nix/store/result","",""),'
            '("dev","/nix/store/dev","","")],'
            '[("/nix/store/dep.drv",["out"])],'
            '["/nix/store/src"],'
            '"x86_64-linux","/bin/bash",["-c","true"],'
            '[("name","test"),("key","value")])'
        )
        parsed = parse_drv(text)
        serialized = parsed.serialize()
        reparsed = parse_drv(serialized)
        assert reparsed.serialize() == serialized

    def test_dynamic_roundtrip(self) -> None:
        text = (
            'DrvWithVersion("xp-dyn-drv",'
            '[("out","/nix/store/result","","")],'
            '[("/nix/store/dep.drv",([],[])),'
            '("/nix/store/simple.drv",["out"])],'
            '[],"x86_64-linux","/bin/sh",["-c","true"],'
            '[("name","dyn")])'
        )
        parsed = parse_drv(text)
        serialized = parsed.serialize()
        reparsed = parse_drv(serialized)
        assert reparsed.serialize() == serialized

    def test_empty_derivation(self) -> None:
        text = 'Derive([],[],[],"x86_64-linux","/bin/sh",[],[])'
        parsed = parse_drv(text)
        assert parsed.serialize() == text

    def test_escaping(self) -> None:
        parsed = Derivation(
            outputs=[DrvOutput(output_name="out", path="", hash_algo="", hash_value="")],
            input_drvs={},
            input_srcs=set(),
            platform="x86_64-linux",
            builder='/bin/sh -c "echo \\"hello\\""',
            args=["-c", 'echo "hello"'],
            env={"key": "value\nwith\ttabs"},
        )
        serialized = parsed.serialize()
        reparsed = parse_drv(serialized)
        assert reparsed.builder == parsed.builder
        assert reparsed.args == parsed.args
        assert reparsed.env == parsed.env

    def test_live_probe_roundtrip(self, probes: Probes) -> None:
        """Every live probe must roundtrip idempotently."""
        for name, drv_content, _ in probes.values():
            parsed = parse_drv(drv_content)
            serialized = parsed.serialize()
            reparsed = parse_drv(serialized)
            assert reparsed.serialize() == serialized, f"roundtrip failed for {name}"
