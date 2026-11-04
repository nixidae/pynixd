"""pynixd's sandbox builds what the daemon builds (`docs/design/builder.md`, proof 5).

The same two derivations build once with `BuildDerivation` on the `pynixd`
route and once on the `daemon` route, each into a fresh test store. The
output's NAR hash, the `ValidPaths` and `Refs` rows and the build log lines
must be equal. `registrationTime` and the row ids differ by nature.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
from typing import TYPE_CHECKING, Any

from prometheus_client import REGISTRY

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

DEP = (
    'derivation { name = "parity-dep"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "echo dep > $out" ]; }'
)
# A reference to the dependency, a reference to itself, a carriage return and stderr.
TOP = (
    f"let dep = {DEP}; in "
    'derivation { name = "parity-top"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "echo one; printf \'50%%\\r100%%\\n\'; echo two >&2; printf tail; echo ${dep} $out > $out" ]; }'
)


def _script(name: str, body: str) -> str:
    return (
        f'derivation {{ name = "{name}"; system = builtins.currentSystem; builder = "/bin/sh"; '
        f"args = [ (builtins.toFile \"{name}.sh\" ''\n{body}\n'') ]; }}"
    )


# More lines than `log-lines` keeps, a `@nix` line after each, and a last line that no newline ends.
FAILS = _script(
    "parity-fails",
    """i=0
while [ $i -lt 30 ]; do echo line $i; echo '@nix {"action":"setPhase","phase":"p"}'; i=$((i+1)); done
printf partial
exit 3""",
)

# Each `@nix` action an untrusted builder may send, and lines `handleJSONLogMessage` refuses.
JSON = _script(
    "parity-json",
    """echo before
echo '@nix {"action":"setPhase","phase":"buildPhase"}'
echo '@nix {"action":"msg","level":0,"msg":"builder says hi"}'
echo '@nix {"action":"msg","level":7,"msg":"too quiet"}'
echo '@nix {"action":"start","id":5,"type":105,"level":0,"text":"not a transfer"}'
echo '@nix {"action":"start","id":6,"type":101,"level":0,"text":"downloading","fields":["u"]}'
echo '@nix {"action":"result","id":6,"type":105,"fields":[1,2,0,0]}'
echo '@nix {"action":"stop","id":6}'
echo '@nix {"action":"msg","level":"loud","msg":"x"}'
echo '@nix {"action":"dance"}'
echo '@nix {bad'
echo after
echo done > $out""",
)
BAD_JSON = "bad JSON log message from the derivation builder: "

FAKE_HASH = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="


def _fod(name: str, mode: str, command: str, output_hash: str = FAKE_HASH) -> str:
    return (
        f"let dep = {DEP}; in "
        f'derivation {{ name = "{name}"; system = builtins.currentSystem; builder = "/bin/sh"; '
        f'args = [ "-c" "{command}" ]; outputHashMode = "{mode}"; outputHashAlgo = "sha256"; '
        f'outputHash = "{output_hash}"; }}'
    )


FOD_FLAT = _fod("parity-fod-flat", "flat", "echo flat > $out")
# A self-reference moves the output to its real path, so `rewriteOutput` changes its content.
FOD_SELF = _fod("parity-fod-self", "recursive", "echo $out > $out")
FOD_DEP = "parity-fod-dep", "recursive", "echo ${dep} > $out"


def _rows(root: Path) -> tuple[list[tuple[Any, ...]], list[tuple[str, str]]]:
    db = sqlite3.connect(f"file:{root}/nix/var/nix/db/db.sqlite?mode=ro", uri=True)
    try:
        valid = db.execute(
            "SELECT path, hash, narSize, deriver, ultimate, sigs, ca FROM ValidPaths ORDER BY path"
        ).fetchall()
        refs = db.execute(
            "SELECT a.path, b.path FROM Refs JOIN ValidPaths a ON a.id = referrer "
            "JOIN ValidPaths b ON b.id = reference ORDER BY a.path, b.path"
        ).fetchall()
    finally:
        db.close()
    return valid, refs


def _without_times(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _without_times(v) for k, v in value.items() if k != "registrationTime"}
    if isinstance(value, list):
        return [_without_times(v) for v in value]
    return value


@contextlib.asynccontextmanager
async def _serve(
    tmp_path: Path, route: OpRoute, *, experimental_features: tuple[str, ...] = ()
) -> AsyncIterator[tuple[str, Path]]:
    """A fresh store with `BuildDerivation` on *route*: its URL and its root."""
    root = tmp_path / route.name / "store"
    socket = tmp_path / f"{route.name}.sock"
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
        nix_config=for_test_store(substituters=(), experimental_features=experimental_features),
        op_routes={"BuildDerivation": route},
    )
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None):
        yield f"unix://{socket}?root={root}", root


async def _build(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    async with _serve(tmp_path, route) as (url, root):
        _rc, out, err, _both = await run_subproc(
            [
                NIX_BIN,
                "build",
                "--store",
                url,
                "--impure",
                "--no-link",
                "--print-out-paths",
                "--print-build-logs",
                "--expr",
                TOP,
            ]
        )
        top = out.strip()
        _rc, info, _err, _both = await run_subproc([NIX_BIN, "path-info", "--store", url, "--json", "--recursive", top])
    valid, refs = _rows(root)
    return {
        "top": top,
        "info": _without_times(json.loads(info)),
        "valid": valid,
        "refs": refs,
        "log": [line for line in err.splitlines() if line.startswith("parity-")],
    }


async def test_pynixd_builds_what_the_daemon_builds(tmp_path: Path) -> None:
    here = await _build(tmp_path, OpRoute.pynixd)
    there = await _build(tmp_path, OpRoute.daemon)

    assert here["top"] == there["top"]
    assert here["info"] == there["info"]
    assert here["valid"] == there["valid"]
    assert here["refs"] == there["refs"]
    assert here["log"] == there["log"]
    assert any(line.endswith("100%") for line in here["log"]), here["log"]


async def _fail(tmp_path: Path, route: OpRoute) -> list[str]:
    async with _serve(tmp_path, route) as (url, _root):
        _rc, _out, err, _both = await run_subproc(
            [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--expr", FAILS], expected_retcode=1
        )
    return _errors(err)


async def test_a_failed_build_says_what_the_daemon_says(tmp_path: Path) -> None:
    """`fixupBuilderFailureErrorMessage`: the reason, the outputs, the last `log-lines` lines, `nix log`."""
    here = await _fail(tmp_path, OpRoute.pynixd)
    there = await _fail(tmp_path, OpRoute.daemon)

    assert here == there
    assert any("Last 25 log lines:" in line for line in here), here
    assert any(line.rstrip().endswith("> partial") for line in here), here
    assert not any("@nix" in line for line in here), here


async def _json(tmp_path: Path, route: OpRoute) -> list[str]:
    async with _serve(tmp_path, route) as (url, _root):
        _rc, _out, err, _both = await run_subproc(
            [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--print-build-logs", "--expr", JSON]
        )
    # NIX-DEVIATION (#27) in `jsonlog.handle`: the parse error's own words differ.
    return [line.split(BAD_JSON, 1)[0] + BAD_JSON if BAD_JSON in line else line for line in err.splitlines()]


async def test_a_builders_json_lines_reach_the_client_as_the_daemon_sends_them(tmp_path: Path) -> None:
    here = await _json(tmp_path, OpRoute.pynixd)
    there = await _json(tmp_path, OpRoute.daemon)

    assert here == there
    assert "builder says hi" in here, here
    assert not any("too quiet" in line or "not a transfer" in line for line in here), here


def _probe(top: str, dep: str) -> str:
    """What the build sees of `/top`, of its store path and of the dependency only its closure brings."""
    checks = "; ".join(f"if [ -e {p} ]; then echo yes; else echo no; fi" for p in ("/top", top, dep))
    return (
        'derivation { name = "parity-probe"; system = builtins.currentSystem; builder = "/bin/sh"; '
        f'args = [ "-c" "({checks}) > $out" ]; }}'
    )


async def _sandbox_closure(tmp_path: Path, route: OpRoute) -> tuple[int, str]:
    async with _serve(tmp_path, route) as (url, root):
        build = [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--print-out-paths"]
        _rc, top, _err, _both = await run_subproc([*build, "--expr", TOP])
        top = top.strip()
        _rc, dep, _err, _both = await run_subproc([NIX_BIN, "path-info", "--store", url, "--expr", DEP, "--impure"])
        probe_expr = _probe(top, dep.strip())
        # Optional: a source names a host path, and this store's files are under its root.
        rc, probe, err, _both = await run_subproc(
            [*build, "--option", "extra-sandbox-paths", f"/top={top}?", "--expr", probe_expr], expected_retcode=None
        )
        return rc, (root / probe.strip().lstrip("/")).read_text() if rc == 0 else err


async def test_a_sandbox_path_in_the_store_brings_its_closure(tmp_path: Path) -> None:
    """`DerivationBuildingGoal` binds the closure of each store path `sandbox-paths` names.

    NIX-DEFECT (#23) in `local._binds`: the daemon binds the closure from the
    store path, which a store at `?root=` does not have (Lillecarl/nix#392).
    """
    here = await _sandbox_closure(tmp_path, OpRoute.pynixd)
    there = await _sandbox_closure(tmp_path, OpRoute.daemon)

    # `/top` names a host path that this store does not have; its closure is in the store.
    assert here == (0, "no\nyes\nyes\n")
    assert there[0] == 1
    assert "-parity-dep" in there[1] and "No such file or directory" in there[1], there[1]


def _errors(err: str) -> list[str]:
    lines = err.splitlines()
    return lines[next(i for i, line in enumerate(lines) if line.startswith("error:")) :]


async def _fixed(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    errors: dict[str, list[str]] = {}
    async with _serve(tmp_path, route) as (url, root):

        async def build(expr: str) -> list[str]:
            _rc, _out, err, _both = await run_subproc(
                [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--expr", expr], expected_retcode=1
            )
            return _errors(err)

        errors["flat"] = await build(FOD_FLAT)
        errors["self"] = await build(FOD_SELF)
        errors["dep"] = await build(_fod(*FOD_DEP))
        got = next(line.split()[-1] for line in errors["dep"] if line.strip().startswith("got:"))
        # The right hash: the path the mismatch registered is valid, and the reference still fails the build.
        errors["dep-right"] = await build(_fod(*FOD_DEP, output_hash=got))
    valid, refs = _rows(root)
    return {"errors": errors, "valid": valid, "refs": refs}


async def test_a_fixed_output_with_the_wrong_hash_stays_valid_under_its_real_one(tmp_path: Path) -> None:
    """`registerOutputs` registers a content-addressed output before `checkOutputs` refuses it."""
    here = await _fixed(tmp_path, OpRoute.pynixd)
    there = await _fixed(tmp_path, OpRoute.daemon)

    assert here["errors"] == there["errors"]
    assert here["valid"] == there["valid"]
    assert here["refs"] == there["refs"]
    assert all(any("hash mismatch" in line for line in here["errors"][k]) for k in ("flat", "self", "dep")), here
    assert any("must not reference store paths" in line for line in here["errors"]["dep-right"]), here
    names = [row[0].split("-", 1)[1] for row in here["valid"]]
    assert {"parity-fod-flat", "parity-fod-self", "parity-fod-dep", "parity-dep"} <= set(names), names


TWINS = (
    'derivation { name = "parity-twins"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'outputs = [ "out" "dev" ]; args = [ "-c" "echo twin > $out; echo twin > $dev" ]; }'
)


async def _optimised(tmp_path: Path, route: OpRoute, *, optimise: bool) -> dict[str, Any]:
    base = tmp_path / f"o{int(optimise)}"
    base.mkdir(exist_ok=True)
    async with _serve(base, route) as (url, root):
        flags = ["--option", "auto-optimise-store", "true"] if optimise else []
        _rc, out, _err, _both = await run_subproc(
            [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--print-out-paths", *flags, "--expr", TWINS]
        )
    store = root / "nix/store"
    links = {entry.stat().st_ino for entry in (store / ".links").iterdir()} if (store / ".links").exists() else set()
    outputs = {}
    for path in out.split():
        st = (store / path.rsplit("/", 1)[-1]).lstat()
        outputs[path.rsplit("/", 1)[-1].split("-", 1)[1]] = (st.st_nlink, st.st_ino in links)
    return outputs


async def test_auto_optimise_store_links_the_outputs_of_a_build(tmp_path: Path) -> None:
    """`registerOutputs` calls `optimisePath` (`derivation-builder.cc:1600`) on each output it registers."""
    for optimise in (True, False):
        here = await _optimised(tmp_path, OpRoute.pynixd, optimise=optimise)
        there = await _optimised(tmp_path, OpRoute.daemon, optimise=optimise)
        assert here == there, optimise
        expected = (3, True) if optimise else (1, False)
        assert here == {"parity-twins": expected, "parity-twins-dev": expected}, here


PLACEHOLDERS = (
    'derivation { name = "parity-placeholders"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'outputs = [ "out" "dev" ]; foo = builtins.placeholder "dev"; passAsFile = [ "bar" ]; '
    'bar = builtins.placeholder "out"; '
    'args = [ "-c" "read -r bar < $barPath; echo $foo $bar > ${builtins.placeholder "out"}; echo dev > $dev" ]; }'
)


async def _placeholders(tmp_path: Path, route: OpRoute) -> tuple[int, dict[str, str]]:
    async with _serve(tmp_path, route) as (url, root):
        rc, out, _err, _both = await run_subproc(
            [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--print-out-paths", "--expr", PLACEHOLDERS],
            expected_retcode=None,
        )
        return rc, {p: (root / p.lstrip("/")).read_text() for p in out.split()}


async def test_an_output_placeholder_reaches_the_builder_as_its_path(tmp_path: Path) -> None:
    """`inputRewrites` (`derivation-builder.cc:435`) in the arguments, the environment and `passAsFile`."""
    here = await _placeholders(tmp_path, OpRoute.pynixd)
    there = await _placeholders(tmp_path, OpRoute.daemon)

    assert here == there
    assert here[0] == 0, here
    dev = next(p for p in here[1] if p.endswith("-dev"))
    out = next(p for p in here[1] if not p.endswith("-dev"))
    assert here[1][out] == f"{dev} {out}\n", here


# Busybox `sh` has no `declare`, so the build copies both files out: the daemon's are the reference.
STRUCTURED = r"""derivation {
  name = "parity-structured"; system = builtins.currentSystem; builder = "/bin/sh";
  __structuredAttrs = true;
  args = [ "-c" ''
    copy() { while IFS= read -r line || [ -n "$line" ]; do printf '%s\n' "$line"; done < "$1"; }
    { copy "$NIX_ATTRS_SH_FILE"; copy "$NIX_ATTRS_JSON_FILE"; } > ${builtins.placeholder "out"}
  '' ];
  int = 123456789; big = 1099511627783; neg = -5; yes = true; no = false; nothing = null;
  list = [ "a" "it's" "\"" 1 null ]; mixed = [ "a" [ "b" ] ];
  hardening = { format = true; fortify = false; }; outer.inner = [ 1 2 3 ];
  text = "tab\tnewline\nunicode é"; dest = "${builtins.placeholder "out"}/foo";
  "foo bar" = "BAD"; "1foobar" = "BAD";
}"""


async def _structured(tmp_path: Path, route: OpRoute) -> tuple[int, list[str]]:
    async with _serve(tmp_path, route) as (url, root):
        rc, out, _err, _both = await run_subproc(
            [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--print-out-paths", "--expr", STRUCTURED],
            expected_retcode=None,
        )
        return rc, [(root / p.lstrip("/")).read_text() for p in sorted(out.split())]


async def test_structured_attrs_reach_the_builder_as_the_daemon_writes_them(tmp_path: Path) -> None:
    """`prepareStructuredAttrs` and `writeShell` (`parsed-derivations.cc`), each placeholder rewritten."""
    here = await _structured(tmp_path, OpRoute.pynixd)
    there = await _structured(tmp_path, OpRoute.daemon)

    assert here == there
    assert here[0] == 0, here
    assert any("declare -A outputs=" in text for text in here[1]), here


_COPY = """copy() { while IFS= read -r line || [ -n "$line" ]; do printf '%s\\n' "$line"; done < "$1"; }"""
# `dep.drvPath` puts the derivation's closure in the input closure, and `exportReferences` adds its outputs.
EXPORTS = {
    "plain": f"""let dep = {DEP}; in derivation {{
  name = "parity-export"; system = builtins.currentSystem; builder = "/bin/sh";
  exportReferencesGraph = [ "graph" dep "drv.graph" dep.drvPath ];
  args = [ "-c" ''{_COPY}
    {{ copy graph; copy drv.graph; }} > $out'' ];
}}""",
    "structured": f"""let dep = {DEP}; in derivation {{
  name = "parity-export-structured"; system = builtins.currentSystem; builder = "/bin/sh";
  __structuredAttrs = true; exportReferencesGraph.closure = [ dep [ dep.drvPath ] ];
  args = [ "-c" ''{_COPY}
    {{ copy "$NIX_ATTRS_SH_FILE"; copy "$NIX_ATTRS_JSON_FILE"; }} > ${{builtins.placeholder "out"}}'' ];
}}""",
}
EXPORT_REJECTED = (
    f"let dep = {DEP}; in "
    'derivation { name = "parity-export-rejected"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'exportReferencesGraph = [ "graph" (builtins.unsafeDiscardStringContext "${dep}") ]; '
    'args = [ "-c" "echo > $out" ]; }'
)


async def _exported(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    results: dict[str, Any] = {}
    async with _serve(tmp_path, route) as (url, root):
        for name, expr in EXPORTS.items():
            rc, out, _err, _both = await run_subproc(
                [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--print-out-paths", "--expr", expr],
                expected_retcode=None,
            )
            results[name] = rc, [(root / p.lstrip("/")).read_text() for p in out.split()]
        _rc, _out, err, _both = await run_subproc(
            [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--expr", EXPORT_REJECTED], expected_retcode=1
        )
        results["rejected"] = _errors(err)
    return results


async def test_export_references_graph_reaches_the_builder_as_the_daemon_writes_it(tmp_path: Path) -> None:
    """`exportReferences` (`store-api.cc:830`) as `makeValidityRegistration` and as `pathInfoToJSON`."""
    here = await _exported(tmp_path, OpRoute.pynixd)
    there = await _exported(tmp_path, OpRoute.daemon)

    assert here == there
    assert here["plain"][0] == here["structured"][0] == 0, here
    assert "-parity-dep.drv\n" in here["plain"][1][0], here
    assert '"closureSize":' in here["structured"][1][0], here
    assert any("cannot export references of path" in line for line in here["rejected"]), here


MID = (
    f"let dep = {DEP}; in "
    'derivation { name = "parity-mid"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "echo ${dep} > $out" ]; }'
)


def _checked(name: str, attrs: str, command: str = "echo ${mid} $out > $out") -> str:
    """A build that refers to `mid` (and through it `dep`) and to itself, under the output checks *attrs*."""
    return (
        f"let dep = {DEP}; mid = {MID}; in "
        f'derivation {{ name = "parity-{name}"; system = builtins.currentSystem; builder = "/bin/sh"; '
        f'args = [ "-c" "{command}" ]; {attrs} }}'
    )


CHECKED = {
    # Only the recursive checks ignore a self-reference (`ignoreSelfRefs`).
    "allowed-not-self": "allowedReferences = [ mid ];",
    "allowed-self": 'allowedReferences = [ "out" mid ]; disallowedReferences = [ dep ];',
    "allowed-empty": "allowedReferences = [ ];",
    "disallowed": "disallowedReferences = [ mid ];",
    "requisites-ok": "allowedRequisites = [ mid dep ];",
    "requisites": "allowedRequisites = [ mid ];",
    "disallowed-requisites": "disallowedRequisites = [ dep ];",
    "unknown-output": 'allowedReferences = [ mid "dev" "bin" ];',
}
# Structured attrs check per output and ignore no self-reference; `$out` is not in the environment.
STRUCTURED_CHECKED = {
    "s-allowed-self": 'outputChecks.out.allowedReferences = [ "out" mid ];',
    "s-requisites-self": "outputChecks.out.allowedRequisites = [ mid dep ];",
    "s-requisites-ok": 'outputChecks.out.allowedRequisites = [ "out" mid dep ];',
    "s-max-size": "outputChecks.out.maxSize = 10;",
    "s-max-closure": "outputChecks.out = { maxSize = 100000; maxClosureSize = 100; };",
    "s-other-output": "outputChecks.dev.allowedReferences = [ ];",
    "s-discard": "unsafeDiscardReferences.out = true; outputChecks.out.allowedReferences = [ ];",
}
_STRUCTURED_COMMAND = 'echo ${mid} ${builtins.placeholder "out"} > ${builtins.placeholder "out"}'


async def _output_checks(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    results: dict[str, Any] = {}
    async with _serve(tmp_path, route) as (url, root):
        exprs = {name: _checked(name, attrs) for name, attrs in CHECKED.items()} | {
            name: _checked(name, f"__structuredAttrs = true; {attrs}", _STRUCTURED_COMMAND)
            for name, attrs in STRUCTURED_CHECKED.items()
        }
        for name, expr in exprs.items():
            rc, _out, err, _both = await run_subproc(
                [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--expr", expr],
                expected_retcode=None,
            )
            results[name] = (rc, _errors(err) if rc else [])
    valid, refs = _rows(root)
    return {"results": results, "valid": valid, "refs": refs}


async def test_output_checks_refuse_what_the_daemon_refuses(tmp_path: Path) -> None:
    """`checkOutputs` (`derivation-check.cc:39`) after `registerOutputs` has described each output.

    `unsafeDiscardReferences` (`derivation-builder.cc:1174`) leaves the output with no references to check.
    """
    here = await _output_checks(tmp_path, OpRoute.pynixd)
    there = await _output_checks(tmp_path, OpRoute.daemon)

    assert here == there
    passed = {name for name, (rc, _errors) in here["results"].items() if rc == 0}
    assert passed == {
        "allowed-self",
        "requisites-ok",
        "s-allowed-self",
        "s-requisites-ok",
        "s-other-output",
        "s-discard",
    }, here["results"]
    assert any("is too large at" in line for line in here["results"]["s-max-size"][1]), here["results"]
    assert any("closure of path" in line for line in here["results"]["s-max-closure"][1]), here["results"]
    assert any("not a valid output" in line for line in here["results"]["unknown-output"][1]), here["results"]
    assert any("not allowed to refer" in line for line in here["results"]["disallowed-requisites"][1]), here


async def _signed(tmp_path: Path, route: OpRoute, key: Path) -> list[tuple[Any, ...]]:
    async with _serve(tmp_path, route) as (url, root):
        build = [NIX_BIN, "build", "--store", url, "--impure", "--no-link"]
        await run_subproc([*build, "--option", "secret-key-files", str(key), "--expr", TOP])
        await run_subproc(
            [*build, "--option", "extra-secret-key-files", str(key), "--expr", FOD_FLAT], expected_retcode=1
        )
    return _rows(root)[0]


async def test_a_built_output_carries_the_signature_of_each_secret_key(tmp_path: Path) -> None:
    """`registerOutputs` calls `signPathInfo` (`derivation-builder.cc:1605`) for every output it registers."""
    key = tmp_path / "secret"
    _rc, secret, _err, _both = await run_subproc([NIX_BIN, "key", "generate-secret", "--key-name", "parity-1"])
    key.write_text(secret)
    here = await _signed(tmp_path, OpRoute.pynixd, key)
    there = await _signed(tmp_path, OpRoute.daemon, key)

    assert here == there
    sigs = {row[0].split("-", 1)[1]: row[5] for row in here if not row[0].endswith(".drv")}
    assert set(sigs) == {"parity-dep", "parity-top", "parity-fod-flat"}, sigs
    assert all(s and s.startswith("parity-1:") for s in sigs.values()), sigs


def _ca(name: str, command: str, mode: str = "recursive", outputs: str = '[ "out" ]') -> str:
    return (
        f'derivation {{ name = "{name}"; system = builtins.currentSystem; builder = "/bin/sh"; '
        f'__contentAddressed = true; outputHashMode = "{mode}"; outputHashAlgo = "sha256"; '
        f'outputs = {outputs}; args = [ "-c" "{command}" ]; }}'
    )


# `out` refers to `dev`, so `dev` moves first and `out` gets its final hash; each refers to itself.
# `echo $out` puts the scratch path in the log. The inputs are floating as well: a flat file, and
# text with a reference.
CA_TOP = (
    "let dep = " + _ca("parity-ca-dep", "echo dep > $out") + "; "
    "flat = " + _ca("parity-ca-flat", "echo flat > $out", "flat") + "; "
    "text = "
    + _ca("parity-ca-text", "echo ${dep} > $out", "text")
    + "; in "
    + _ca(
        "parity-ca-top",
        "echo $out; echo $dev > $dev; echo ${dep} ${flat} ${text} $dev $out > $out",
        outputs='[ "out" "dev" ]',
    )
)


def _fallthrough() -> float:
    return sum(
        sample.value
        for metric in REGISTRY.collect()
        if metric.name == "pynixd_op_fallthrough"
        for sample in metric.samples
        if sample.name == "pynixd_op_fallthrough_total" and sample.labels.get("op") == "BuildDerivation"
    )


async def _floating(tmp_path: Path, route: OpRoute, key: Path) -> dict[str, Any]:
    async with _serve(tmp_path, route, experimental_features=("ca-derivations",)) as (url, root):
        _rc, out, err, _both = await run_subproc(
            [
                NIX_BIN,
                "build",
                "--store",
                url,
                "--extra-experimental-features",
                "ca-derivations",
                "--option",
                "secret-key-files",
                str(key),
                "--impure",
                "--no-link",
                "--print-out-paths",
                "--print-build-logs",
                "--expr",
                CA_TOP,
            ]
        )
        paths = out.split()
        _rc, info, _err, _both = await run_subproc(
            [NIX_BIN, "path-info", "--store", url, "--json", "--recursive", *paths]
        )
    db = sqlite3.connect(f"file:{root}/nix/var/nix/db/db.sqlite?mode=ro", uri=True)
    try:
        trace = db.execute(
            "SELECT drvPath, outputName, outputPath, signatures FROM BuildTraceV3 ORDER BY drvPath, outputName"
        ).fetchall()
    finally:
        db.close()
    valid, refs = _rows(root)
    return {
        "paths": paths,
        "info": _without_times(json.loads(info)),
        "valid": valid,
        "refs": refs,
        "trace": trace,
        "log": [line for line in err.splitlines() if line.startswith("parity-")],
    }


async def test_a_floating_output_lands_where_the_daemon_puts_it(tmp_path: Path) -> None:
    """`newInfoFromCA` (`derivation-builder.cc:1341`) and the realisations `registerOutputs` signs (`:1656`)."""
    key = tmp_path / "secret"
    _rc, secret, _err, _both = await run_subproc([NIX_BIN, "key", "generate-secret", "--key-name", "parity-1"])
    key.write_text(secret)
    before = _fallthrough()
    here = await _floating(tmp_path, OpRoute.pynixd, key)
    assert _fallthrough() == before, "pynixd's sandbox builds every floating derivation"
    there = await _floating(tmp_path, OpRoute.daemon, key)

    assert here["paths"] == there["paths"]
    assert here["info"] == there["info"]
    assert here["valid"] == there["valid"]
    assert here["refs"] == there["refs"]
    assert here["trace"] == there["trace"]
    assert here["log"] == there["log"]
    assert len(here["paths"]) == 2, here["paths"]
    built = {row[2].split("-", 1)[1] for row in here["trace"]}
    assert {"parity-ca-dep", "parity-ca-flat", "parity-ca-text", "parity-ca-top", "parity-ca-top-dev"} <= built
    assert any(row[3].startswith("parity-1:") for row in here["trace"]), here["trace"]


IMPURE_ENV = (
    'derivation { name = "parity-impure-env"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'impureEnvVars = [ "PARITY_TOKEN" "PARITY_EXTRA" ]; outputHashMode = "flat"; outputHashAlgo = "sha256"; '
    f'outputHash = "{FAKE_HASH}"; '
    'args = [ "-c" "echo token=$PARITY_TOKEN extra=$PARITY_EXTRA; echo x > $out" ]; }'
)


async def _impure_env(tmp_path: Path, route: OpRoute) -> list[str]:
    async with _serve(tmp_path, route, experimental_features=("configurable-impure-env",)) as (url, _root):
        _rc, _out, err, _both = await run_subproc(
            [
                NIX_BIN,
                "build",
                "--store",
                url,
                "--extra-experimental-features",
                "configurable-impure-env",
                "--option",
                "extra-impure-env",
                "PARITY_TOKEN=lost PARITY_EXTRA=lost",
                "--option",
                "impure-env",
                "PARITY_TOKEN=secret",
                "--impure",
                "--no-link",
                "--print-build-logs",
                "--expr",
                IMPURE_ENV,
            ],
            expected_retcode=1,
        )
    return [line for line in err.splitlines() if line.startswith("parity-")]


async def test_impure_env_reaches_a_fixed_output_build_as_the_daemon_passes_it(tmp_path: Path) -> None:
    """`initEnv` reads `impure-env` before the environment (`derivation-builder.cc:762`)."""
    before = _fallthrough()
    here = await _impure_env(tmp_path, OpRoute.pynixd)
    assert _fallthrough() == before, "pynixd's sandbox builds parity-impure-env"
    there = await _impure_env(tmp_path, OpRoute.daemon)

    assert here == there
    assert any(line.endswith("token=secret extra=") for line in here), here


async def test_a_fixed_output_with_fsync_store_paths_goes_to_the_daemon(tmp_path: Path) -> None:
    """`moveOutputToTempDir` fsyncs a fixed output (`derivation-builder.cc:1417`), and pynixd's restore cannot."""
    before = _fallthrough()
    async with _serve(tmp_path, OpRoute.pynixd) as (url, _root):
        _rc, _out, err, _both = await run_subproc(
            [
                NIX_BIN,
                "build",
                "--store",
                url,
                "--option",
                "fsync-store-paths",
                "true",
                "--impure",
                "--no-link",
                "--expr",
                FOD_FLAT,
            ],
            expected_retcode=1,
        )
    assert _fallthrough() == before + 1
    assert any("hash mismatch in fixed-output derivation" in line for line in _errors(err)), err
