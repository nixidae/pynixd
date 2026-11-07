"""pynixd's sandbox builds what the daemon builds (`docs/design/builder.md`, proof 5).

The same two derivations build once with `BuildDerivation` on the `pynixd`
route and once on the `daemon` route, each into a fresh test store. The
output's NAR hash, the `ValidPaths` and `Refs` rows and the build log lines
must be equal. `registrationTime` and the row ids differ by nature.
"""

from __future__ import annotations

import base64
import contextlib
import fcntl
import hashlib
import io
import json
import os
import sqlite3
import tarfile
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import anyio
import pytest
from aiohttp import web
from prometheus_client import REGISTRY

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from pynixd_store.nar_fs import dump_path
from tests._conftest.config import NIX_GIT_BIN
from tests._conftest.constants import _NO_PROBE_FEATURE_MATRIX
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Mapping

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
    tmp_path: Path,
    route: OpRoute,
    *,
    experimental_features: tuple[str, ...] = (),
    diff_hook: Path | None = None,
    post_build_hook: Path | None = None,
    config_file: bool = False,
    ops: tuple[str, ...] = ("BuildDerivation",),
    settings: Mapping[str, Any] | None = None,
    **spec_args: Any,
) -> AsyncIterator[tuple[str, Path]]:
    """A fresh store with each of *ops* on *route*: its URL and its root. *spec_args* go to `make_test_spec`.

    *settings* are the store's own, by their `NixConfig` field names.

    With *config_file*, the daemon reads the store's settings from a file named by `NIX_USER_CONF_FILES`, and
    its environment holds no `NIX_CONFIG`.
    """
    root = tmp_path / route.name / "store"
    socket = tmp_path / f"{route.name}.sock"
    nix_config = for_test_store(substituters=(), experimental_features=experimental_features)
    if diff_hook is not None:
        nix_config.run_diff_hook = True
        nix_config.diff_hook = str(diff_hook)
    if post_build_hook is not None:
        nix_config.post_build_hook = str(post_build_hook)
    for name, value in (settings or {}).items():
        setattr(nix_config, name, value)
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
        nix_config=nix_config,
        op_routes=dict.fromkeys(ops, route),
        **spec_args,
    )
    if config_file and spec.extra_env is not None:
        conf = tmp_path / f"{route.name}.conf"
        conf.write_text(nix_config.to_nix_conf() + "\n")
        del spec.extra_env["NIX_CONFIG"]
        spec.extra_env["NIX_USER_CONF_FILES"] = str(conf)
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
  pi = 3.14; whole = 2.0; tiny = 1.0e-5; small = 0.0001; near = 0.99999999; half = -2.5;
  single = 16777217.5; floats = [ 1.5 2.0 ]; scaled = { a = 3.0; b = 0.1; };
}"""


async def _structured(tmp_path: Path, route: OpRoute) -> tuple[int, list[str]]:
    async with _serve(tmp_path, route) as (url, root):
        rc, out, _err, _both = await run_subproc(
            [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--print-out-paths", "--expr", STRUCTURED],
            expected_retcode=None,
        )
        return rc, [(root / p.lstrip("/")).read_text() for p in sorted(out.split())]


async def test_structured_attrs_reach_the_builder_as_the_daemon_writes_them(tmp_path: Path) -> None:
    """`prepareStructuredAttrs` and `writeShell` (`parsed-derivations.cc`), each placeholder rewritten.

    `writeShell` declares a float only when its single-precision value is whole (`parsed-derivations.cc:131`).
    """
    before = _fallthrough()
    here = await _structured(tmp_path, OpRoute.pynixd)
    assert _fallthrough() == before, "pynixd's sandbox builds parity-structured"
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


CA_TWIN = (
    'derivation { name = "parity-ca-twin"; system = builtins.currentSystem; builder = "/bin/sh"; '
    '__contentAddressed = true; outputHashMode = "recursive"; outputHashAlgo = "sha256"; '
    'args = [ "-c" "echo twin > $out" ]; }'
)


async def _held(tmp_path: Path, route: OpRoute) -> tuple[bool, str]:
    """Build `CA_TWIN` while this process holds its final path's lock, as a concurrent build would.

    Whether the build was still running when the lock was let go, and the path it gave.
    """
    content = tmp_path / route.name / "parity-ca-twin"
    content.parent.mkdir(parents=True)
    content.write_text("twin\n")
    _rc, final, _err, _both = await run_subproc(
        [
            NIX_BIN,
            "store",
            "add",
            "--store",
            tmp_path / route.name / "dry",
            "--dry-run",
            "--name",
            "parity-ca-twin",
            content,
        ]
    )
    async with _serve(tmp_path, route, experimental_features=("ca-derivations",)) as (url, root):
        lock = root / "nix/store" / f"{final.strip().rsplit('/', 1)[-1]}.lock"
        lock.parent.mkdir(parents=True, exist_ok=True)
        done = anyio.Event()
        out: list[str] = []

        async def build() -> None:
            _rc, stdout, _err, _both = await run_subproc(
                [
                    NIX_BIN,
                    "build",
                    "--store",
                    url,
                    "--extra-experimental-features",
                    "ca-derivations",
                    "--impure",
                    "--no-link",
                    "--print-out-paths",
                    "--expr",
                    CA_TWIN,
                ]
            )
            out.append(stdout.strip())
            done.set()

        with lock.open("a") as held:
            fcntl.flock(held, fcntl.LOCK_EX)
            async with anyio.create_task_group() as group:
                group.start_soon(build)
                await anyio.sleep(5)
                waited = not done.is_set()
                fcntl.flock(held, fcntl.LOCK_UN)
    return waited, out[0]


async def test_a_floating_output_waits_for_the_lock_on_its_path(tmp_path: Path) -> None:
    """`lockPaths(..., wait = !optFixedPath)` (`derivation-builder.cc:1510`): a floating output waits; it does not fail."""
    here = await _held(tmp_path, OpRoute.pynixd)
    there = await _held(tmp_path, OpRoute.daemon)

    assert here == there
    assert here[0], "the build finished while another process held the lock on its path"


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


def _all_fallthroughs() -> float:
    return sum(
        sample.value
        for metric in REGISTRY.collect()
        if metric.name == "pynixd_op_fallthrough"
        for sample in metric.samples
        if sample.name == "pynixd_op_fallthrough_total"
    )


async def _synced_writes(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    """Under `fsync-store-paths`: a dump (`nix store add`), a NAR (`nix copy`), and a fixed output built twice."""
    tree = tmp_path / "tree"
    if not tree.exists():
        (tree / "a" / "b").mkdir(parents=True)
        (tree / "top").write_text("top\n")
        (tree / "a" / "middle").write_text("middle\n")
        (tree / "a" / "b" / "deep").write_text("deep\n")
        (tree / "link").symlink_to("top")
    source = f"local?root={tmp_path / route.name / 'source'}"
    sync = ("--option", "fsync-store-paths", "true")
    ops = ("BuildDerivation", "AddToStore", "AddToStoreNar", "AddMultipleToStore")
    async with _serve(tmp_path, route, ops=ops) as (url, root):
        _rc, out, _err, _both = await run_subproc(
            [NIX_BIN, "store", "add", "--store", url, *sync, "--name", "parity-synced-tree", str(tree)]
        )
        added = out.strip()
        (tmp_path / "copied").write_text("copied\n")
        _rc, out, _err, _both = await run_subproc(
            [NIX_BIN, "store", "add-file", "--store", source, str(tmp_path / "copied")]
        )
        copied = out.strip()
        await run_subproc([NIX_BIN, "copy", "--no-check-sigs", *sync, "--from", source, "--to", url, copied])
        _rc, _out, err, _both = await run_subproc(
            [NIX_BIN, "build", "--store", url, *sync, "--impure", "--no-link", "--expr", FOD_FLAT],
            expected_retcode=1,
        )
        got = next(line.split()[-1] for line in err.splitlines() if line.strip().startswith("got:"))
        _rc, out, _err, _both = await run_subproc(
            [
                NIX_BIN,
                "build",
                "--store",
                url,
                *sync,
                "--impure",
                "--no-link",
                "--print-out-paths",
                "--expr",
                _fod("parity-fod-flat", "flat", "echo flat > $out", got),
            ]
        )
        built = out.strip()
    valid, refs = _rows(root)
    return {
        "paths": {"added": added, "copied": copied, "built": built},
        "errors": _errors(err),
        "valid": valid,
        "refs": refs,
        "real": Path(os.path.realpath(root / "nix" / "store")),
    }


async def test_fsync_store_paths_syncs_each_path_before_pynixd_registers_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`recursiveSync` and `syncParent` before `registerValidPath` (`local-store.cc:1136`, `:1298`).

    Nix does not sync a build output (Lillecarl/nix#402); pynixd does. Each write stays in pynixd, its result is
    the daemon's, and every file and directory of each new path is synced, its symlink not.
    """
    synced: list[str] = []
    fsync = os.fsync

    def record(fd: int) -> None:
        synced.append(os.readlink(f"/proc/self/fd/{fd}"))
        fsync(fd)

    monkeypatch.setattr(os, "fsync", record)
    before = _all_fallthroughs()
    here = await _synced_writes(tmp_path, OpRoute.pynixd)
    assert _all_fallthroughs() == before, "the daemon took a write of the pynixd route"
    mine = list(synced)
    there = await _synced_writes(tmp_path, OpRoute.daemon)

    for key in ("paths", "errors", "valid", "refs"):
        assert here[key] == there[key], (key, here[key], there[key])
    real = here["real"]
    tree = str(real / here["paths"]["added"].rsplit("/", 1)[-1])
    expected = [f"{tree}/top", f"{tree}/a/middle", f"{tree}/a/b/deep", f"{tree}/a/b", f"{tree}/a", tree, str(real)]
    assert [path for path in mine if path.startswith(tree) or path == str(real)][: len(expected)] == expected, mine
    assert f"{tree}/link" not in mine, mine
    for key in ("copied", "built"):
        path = str(real / here["paths"][key].rsplit("/", 1)[-1])
        assert path in mine, (f"pynixd did not sync the {key} path", mine)
        assert mine[mine.index(path) + 1] == str(real), (key, mine)


async def _registrations(tmp_path: Path, sync: bool) -> list[str]:
    """An add and a build on the pynixd route; the paths it registered, in order."""
    settings = {"sync_before_registering": True} if sync else {}
    ops = ("BuildDerivation", "AddToStore")
    async with _serve(tmp_path / str(sync), OpRoute.pynixd, ops=ops, settings=settings) as (url, _root):
        (tmp_path / "registered").write_text("registered\n")
        _rc, out, _err, _both = await run_subproc(
            [NIX_BIN, "store", "add-file", "--store", url, str(tmp_path / "registered")]
        )
        _rc, built, _err, _both = await run_subproc(
            [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--print-out-paths", "--expr", DEP]
        )
    return [out.strip(), built.strip()]


async def test_sync_before_registering_syncs_before_each_registration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`registerValidPaths` calls `sync()` first with `sync-before-registering` (`local-store.cc:964`)."""
    syncs: list[None] = []
    sync = os.sync

    def record() -> None:
        syncs.append(None)
        sync()

    monkeypatch.setattr(os, "sync", record)
    before = _all_fallthroughs()
    off = await _registrations(tmp_path, sync=False)
    assert syncs == [], "pynixd synced with sync-before-registering off"
    on = await _registrations(tmp_path, sync=True)
    assert _all_fallthroughs() == before, "the daemon took a write of the pynixd route"
    assert on == off
    # The added file, the derivation, and the build's output: one registration each.
    assert len(syncs) >= 3, syncs


async def _flat_with_references(tmp_path: Path, route: OpRoute) -> tuple[int, list[str]]:
    async with _serve(tmp_path, route) as (url, _root):
        rc, _out, err, _both = await run_subproc(
            [
                NIX_BIN,
                "build",
                "--store",
                url,
                "--impure",
                "--no-link",
                "--expr",
                _fod("parity-fod-flat-dep", "flat", "echo ${dep} > $out"),
            ],
            expected_retcode=None,
        )
    return rc, _errors(err)


async def test_a_flat_fixed_output_with_a_reference_fails_as_the_daemon_fails_it(tmp_path: Path) -> None:
    """`makeFixedOutputPath` (`store-dir-config.cc:119`) refuses a flat address with references."""
    before = _fallthrough()
    here = await _flat_with_references(tmp_path, OpRoute.pynixd)
    assert _fallthrough() == before
    there = await _flat_with_references(tmp_path, OpRoute.daemon)
    assert here == there


# `out` refers to `dev` and to itself. The scratch path is random, so the log holds no output path.
IMPURE = (
    'derivation { name = "parity-impure"; system = builtins.currentSystem; builder = "/bin/sh"; '
    '__impure = true; outputHashMode = "recursive"; outputHashAlgo = "sha256"; outputs = [ "out" "dev" ]; '
    'impureEnvVars = [ "PARITY_TOKEN" ]; '
    'args = [ "-c" "echo token=$PARITY_TOKEN checked=$NIX_OUTPUT_CHECKED; echo dev > $dev; echo $dev $out > $out" ]; }'
)
IMPURE_FEATURES = ("ca-derivations", "impure-derivations", "configurable-impure-env")


async def _impure(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    async with _serve(tmp_path, route, experimental_features=IMPURE_FEATURES) as (url, root):
        _rc, out, err, _both = await run_subproc(
            [
                NIX_BIN,
                "build",
                "--store",
                url,
                "--extra-experimental-features",
                " ".join(IMPURE_FEATURES),
                "--option",
                "impure-env",
                "PARITY_TOKEN=secret",
                "--impure",
                "--no-link",
                "--print-out-paths",
                "--print-build-logs",
                "--expr",
                IMPURE,
            ]
        )
        paths = out.split()
        _rc, info, _err, _both = await run_subproc([NIX_BIN, "path-info", "--store", url, "--json", *paths])
        contents = {path: (root / path.lstrip("/")).read_text() for path in paths}
    db = sqlite3.connect(f"file:{root}/nix/var/nix/db/db.sqlite?mode=ro", uri=True)
    try:
        trace = db.execute("SELECT drvPath, outputName, outputPath FROM BuildTraceV3").fetchall()
    finally:
        db.close()
    valid, refs = _rows(root)
    return {
        "paths": paths,
        "info": _without_times(json.loads(info)),
        "contents": contents,
        "valid": valid,
        "refs": refs,
        "trace": trace,
        "log": [line for line in err.splitlines() if line.startswith("parity-")],
    }


async def test_an_impure_derivation_builds_where_the_daemon_builds_it(tmp_path: Path) -> None:
    """`registerOutputs` addresses an impure output as a floating one (`derivation-builder.cc:1469`) and registers
    no realisation (`:1667`); `initEnv` passes `impureEnvVars` and no `NIX_OUTPUT_CHECKED` (`:749`, `:761`)."""
    before = _fallthrough()
    here = await _impure(tmp_path, OpRoute.pynixd)
    assert _fallthrough() == before, "pynixd's sandbox builds parity-impure"
    there = await _impure(tmp_path, OpRoute.daemon)

    assert here == there
    assert len(here["paths"]) == 2, here["paths"]
    assert here["trace"] == []
    assert any(line.endswith("token=secret checked=") for line in here["log"]), here["log"]


def _export_drv(name: str, dep: str) -> str:
    return (
        f"let dep = {dep}; in "
        f'derivation {{ name = "{name}"; system = builtins.currentSystem; builder = "/bin/sh"; '
        'exportReferencesGraph = [ "graph" (builtins.unsafeDiscardOutputDependency dep.drvPath) ]; '
        'args = [ "-c" "echo > $out" ]; }'
    )


# A derivation in the graph whose output was never built, and one whose output is floating. The context
# of `drvPath` alone would build every output first.
EXPORT_ERRORS = {
    "invalid": _export_drv("parity-export-invalid", _script("parity-export-unbuilt", "echo > $out")),
    "floating": _export_drv("parity-export-floating", _ca("parity-export-ca", "echo > $out")),
}


async def _export_errors(tmp_path: Path, route: OpRoute) -> dict[str, tuple[int, list[str]]]:
    results: dict[str, tuple[int, list[str]]] = {}
    async with _serve(tmp_path, route, experimental_features=("ca-derivations",)) as (url, _root):
        for name, expr in EXPORT_ERRORS.items():
            rc, _out, err, _both = await run_subproc(
                [
                    NIX_BIN,
                    "build",
                    "--store",
                    url,
                    "--extra-experimental-features",
                    "ca-derivations",
                    "--impure",
                    "--no-link",
                    "--expr",
                    expr,
                ],
                expected_retcode=None,
            )
            results[name] = rc, _errors(err)
    return results


async def test_a_derivation_in_the_export_graph_without_an_output_path_fails_as_the_daemon_fails_it(
    tmp_path: Path,
) -> None:
    """`exportReferences` (`store-api.cc:850`) throws a plain `Error`, which `Store::buildDerivation` answers
    as `MISC_FAILURE` with its text (`entry-points.cc:89`)."""
    before = _fallthrough()
    here = await _export_errors(tmp_path, OpRoute.pynixd)
    assert _fallthrough() == before
    there = await _export_errors(tmp_path, OpRoute.daemon)

    assert here == there
    assert any("is not valid" in line for line in here["invalid"][1]), here
    assert any("exportReferences on CA derivations" in line for line in here["floating"][1]), here


# A floating Git output, one that refers to itself, and a fixed one with the wrong hash.
GIT_OUTPUTS = {
    "floating": _ca("parity-git", "echo git > $out", "git"),
    "self": _ca("parity-git-self", "echo $out > $out", "git"),
    "fixed": _fod("parity-git-fod", "git", "echo fod > $out"),
}
GIT_FEATURES = ("ca-derivations", "git-hashing")


async def _git_outputs(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    results: dict[str, Any] = {}
    async with _serve(tmp_path, route, experimental_features=GIT_FEATURES) as (url, root):
        for name, expr in GIT_OUTPUTS.items():
            rc, out, err, _both = await run_subproc(
                [
                    NIX_BIN,
                    "build",
                    "--store",
                    url,
                    "--extra-experimental-features",
                    " ".join(GIT_FEATURES),
                    "--impure",
                    "--no-link",
                    "--print-out-paths",
                    "--expr",
                    expr,
                ],
                expected_retcode=None,
            )
            results[name] = rc, out.split(), _errors(err) if rc else []
    results["valid"], results["refs"] = _rows(root)
    return results


async def test_a_git_hashed_output_lands_where_the_daemon_puts_it(tmp_path: Path) -> None:
    """`newInfoFromCA` hashes a Git output with `git::dumpHash`, no modulo (`derivation-builder.cc:1366`), and
    `makeFixedOutputPath` refuses its references (`store-dir-config.cc:119`)."""
    before = _fallthrough()
    here = await _git_outputs(tmp_path, OpRoute.pynixd)
    assert _fallthrough() == before, "pynixd's sandbox builds each Git-hashed derivation"
    there = await _git_outputs(tmp_path, OpRoute.daemon)

    assert here == there
    assert here["floating"][0] == 0, here
    assert any("not allowed to refer to other store paths" in line for line in here["self"][2]), here
    assert any("hash mismatch" in line for line in here["fixed"][2]), here


FETCHED = b"fetched by builtin:fetchurl\n"


def _sri(data: bytes) -> str:
    return "sha256-" + base64.b64encode(hashlib.sha256(data).digest()).decode()


def _nar_string(data: bytes) -> bytes:
    return len(data).to_bytes(8, "little") + data + b"\0" * (-len(data) % 8)


# `executable` makes the output recursive: its hash is that of the file's NAR.
EXECUTABLE_NAR = (
    b"".join(_nar_string(s) for s in (b"nix-archive-1", b"(", b"type", b"regular", b"executable", b"", b"contents"))
    + _nar_string(FETCHED)
    + _nar_string(b")")
)


@contextlib.asynccontextmanager
async def _http(tmp_path: Path) -> AsyncIterator[tuple[str, dict[str, Any]]]:
    """The files the fetches ask for, at `localhost`: the name must resolve inside the sandbox.

    *seen* records the path and User-Agent of each request.
    """
    tree = tmp_path / "tree"
    (tree / "sub").mkdir(parents=True)
    (tree / "sub" / "file").write_text("unpacked\n")
    (tree / "run").write_text("#!/bin/sh\n")
    (tree / "run").chmod(0o755)
    nar = b"".join(dump_path(tree))
    seen: dict[str, Any] = {"requests": [], "nar": nar}
    mirrored = f"/mirror/sha256/{hashlib.sha256(FETCHED).hexdigest()}"

    async def handle(request: web.Request) -> web.StreamResponse:
        seen["requests"].append((request.path, request.headers.get("User-Agent", "")))
        if request.path in ("/file", mirrored):
            return web.Response(body=FETCHED)
        if request.path == "/tree.nar":
            return web.Response(body=nar)
        if request.path == "/forbidden":
            return web.Response(status=403, text="go away\n")
        return web.Response(status=404, text="no such file\n")

    app = web.Application()
    app.router.add_route("GET", "/{tail:.*}", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    try:
        yield f"http://localhost:{runner.addresses[0][1]}", seen
    finally:
        await runner.cleanup()


def _fetchurl(name: str, url: str, extra: str) -> str:
    return f'import <nix/fetchurl.nix> {{ name = "{name}"; url = "{url}"; {extra} }}'


def _tree(path: Path) -> list[str] | str:
    if path.is_dir():
        return sorted(f"{p.relative_to(path)} {oct(p.lstat().st_mode)}" for p in path.rglob("*"))
    return f"{path.read_text()} {oct(path.lstat().st_mode)}"


async def _fetches(tmp_path: Path, route: OpRoute, base: str, seen: dict[str, Any]) -> dict[str, Any]:
    results: dict[str, Any] = {}
    async with _serve(tmp_path, route) as (url, root):
        fetches = {
            "flat": ((), _fetchurl("parity-fetch", f"{base}/file", f'hash = "{_sri(FETCHED)}";')),
            "executable": (
                (),
                _fetchurl("parity-fetch-exe", f"{base}/file", f'hash = "{_sri(EXECUTABLE_NAR)}"; executable = true;'),
            ),
            "unpack": (
                (),
                _fetchurl("parity-fetch-nar", f"{base}/tree.nar", f'hash = "{_sri(seen["nar"])}"; unpack = true;'),
            ),
            "mirror": (
                ("--option", "hashed-mirrors", f"{base}/mirror"),
                _fetchurl("parity-fetch-mirror", f"{base}/gone", f'hash = "{_sri(FETCHED)}";'),
            ),
            "missing": ((), _fetchurl("parity-fetch-missing", f"{base}/gone", f'hash = "{FAKE_HASH}";')),
            "forbidden": ((), _fetchurl("parity-fetch-forbidden", f"{base}/forbidden", f'hash = "{FAKE_HASH}";')),
            "wrong-hash": ((), _fetchurl("parity-fetch-wrong", f"{base}/file", f'hash = "{FAKE_HASH}";')),
        }
        for name, (options, expr) in fetches.items():
            seen["requests"].clear()
            rc, out, err, _both = await run_subproc(
                [
                    NIX_BIN,
                    "build",
                    "--store",
                    url,
                    *options,
                    "--impure",
                    "--no-link",
                    "--print-out-paths",
                    "--expr",
                    expr,
                ],
                expected_retcode=None,
            )
            paths = out.split()
            errors = _errors(err) if rc else []
            results[name] = rc, paths, [_tree(root / p.lstrip("/")) for p in paths], errors, list(seen["requests"])
    results["valid"], results["refs"] = _rows(root)
    return results


async def test_builtin_fetchurl_fetches_what_the_daemon_fetches(tmp_path: Path) -> None:
    """`builtinFetchurl` (`builtins/fetchurl.cc:10`): a flat file, an executable one, an unpacked NAR, the
    hashed mirrors first, and the errors of a missing file, a refused one and a wrong hash."""
    # One server for both runs: the URL is part of the derivation, so its port must not change.
    async with _http(tmp_path) as (base, seen):
        before = _fallthrough()
        here = await _fetches(tmp_path, OpRoute.pynixd, base, seen)
        assert _fallthrough() == before, "pynixd's sandbox runs every builtin:fetchurl"
        there = await _fetches(tmp_path, OpRoute.daemon, base, seen)

    assert here == there
    assert here["flat"][0] == here["executable"][0] == here["unpack"][0] == here["mirror"][0] == 0, here
    assert here["mirror"][4][0][0].startswith("/mirror/sha256/"), here["mirror"]
    assert any("HTTP error" in line and "404" in line for line in here["missing"][3]), here["missing"]
    assert here["flat"][4][0][1].startswith("curl/"), here["flat"]


#: Package trees for `builtin:buildenv`; "->" makes a symbolic link.
PROFILE_PACKAGES = {
    "env-a": {
        "bin/a": "a",
        "bin/same": "a",
        "share/doc/a": "a",
        "lib": "->bin",
        ".hidden": "",
        "manifest.json": "",
        "gone": "->/nowhere",
    },
    "env-b": {"bin/b": "b", "bin/same": "b", "share/doc/b": "b"},
    "env-c": {"bin/c": "c"},
    "conflict-a": {"bin/x": "a"},
    "conflict-b": {"bin/x": "b"},
    "file-a": {"etc": "a"},
    "dir-b": {"etc/x": "b"},
    "merge-a": {"share/x": "a"},
    "merge-b": {"share/y": "b"},
    "merge-c": {"share": "c"},
}

#: Each profile: (active, priority, package); `env-c` is in `env-a`'s propagated-user-env-packages.
PROFILES = {
    "profile": [("true", 3, "env-a"), ("true", 5, "env-b"), ("false", 1, "env-c"), ("true", 5, "file")],
    "conflict": [("true", 5, "conflict-a"), ("true", 5, "conflict-b")],
    "non-directory": [("true", 1, "file-a"), ("true", 2, "dir-b")],
    "directory": [("true", 1, "merge-a"), ("true", 2, "merge-b"), ("true", 3, "merge-c")],
}


def _packages(root: Path, names: Iterable[str]) -> None:
    for name in names:
        for rel, text in PROFILE_PACKAGES[name].items():
            path = root / name / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            if text.startswith("->"):
                path.symlink_to(text[2:])
            else:
                path.write_text(text)


def _profile(packages: Path, name: str) -> str:
    entries = " ".join(
        f'[ "{active}" {priority} 1 '
        + (
            '(builtins.toFile "file" "f")'
            if pkg == "file"
            else f'(builtins.path {{ path = {packages}/{pkg}; name = "{pkg}"; }})'
        )
        + " ]"
        for active, priority, pkg in PROFILES[name]
    )
    return (
        f'derivation {{ name = "parity-{name}"; system = "builtin"; builder = "builtin:buildenv"; '
        f'manifest = builtins.toFile "manifest.nix" "[ ]"; derivations = [ {entries} ]; }}'
    )


def _links(path: Path) -> dict[str, str]:
    return {
        str(p.relative_to(path)): os.readlink(p) if p.is_symlink() else oct(p.lstat().st_mode)
        for p in sorted(path.rglob("*"))
    }


async def _profiles(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    results: dict[str, Any] = {}
    packages = tmp_path / route.name / "packages"
    async with _serve(tmp_path, route) as (url, root):
        _packages(packages, ["env-c"])
        _rc, propagated, _err, _both = await run_subproc(
            [
                NIX_BIN,
                "eval",
                "--store",
                url,
                "--impure",
                "--raw",
                "--expr",
                f'builtins.path {{ path = {packages}/env-c; name = "env-c"; }}',
            ]
        )
        _packages(packages, PROFILE_PACKAGES.keys() - {"env-c"})
        (packages / "env-a" / "nix-support").mkdir()
        (packages / "env-a" / "nix-support" / "propagated-user-env-packages").write_text(f"{propagated}\n")
        for name in PROFILES:
            rc, out, err, _both = await run_subproc(
                [
                    NIX_BIN,
                    "build",
                    "--store",
                    url,
                    "--impure",
                    "--no-link",
                    "--print-out-paths",
                    "--expr",
                    _profile(packages, name),
                ],
                expected_retcode=None,
            )
            paths = out.split()
            results[name] = rc, paths, [_links(root / p.lstrip("/")) for p in paths], err.splitlines()
    results["valid"], results["refs"] = _rows(root)
    return results


async def test_builtin_buildenv_builds_the_profile_the_daemon_builds(tmp_path: Path) -> None:
    """`builtinBuildenv` (`builtins/buildenv.cc:173`): merged directories, priorities, a propagated package,
    the skipped names, its two warnings, and the errors of a conflict and of both collisions."""
    before = _fallthrough()
    here = await _profiles(tmp_path, OpRoute.pynixd)
    assert _fallthrough() == before, "pynixd's sandbox runs every builtin:buildenv"
    there = await _profiles(tmp_path, OpRoute.daemon)

    assert here == there
    assert here["profile"][0] == 0, here["profile"]
    assert {"bin/c", "share/doc/b", "manifest.nix"} <= set(here["profile"][2][0]), here["profile"]
    assert any("skipping dangling symlink" in line for line in here["profile"][3]), here["profile"]
    assert all(here[name][0] == 1 for name in ("conflict", "non-directory", "directory")), here


def _tar_member(
    tar: tarfile.TarFile, name: str, kind: bytes, *, mode: int = 0o644, data: bytes = b"", link: str = ""
) -> None:
    info = tarfile.TarInfo(name)
    info.type, info.mode, info.size, info.linkname, info.mtime = kind, mode, len(data), link, 1234567890
    tar.addfile(info, io.BytesIO(data) if data else None)


def _tarballs(root: Path) -> None:
    root.mkdir(parents=True)
    with tarfile.open(root / "channel.tar.xz", "w:xz") as tar:
        _tar_member(tar, "nixexprs-1/", tarfile.DIRTYPE, mode=0o755)
        _tar_member(tar, "nixexprs-1/default.nix", tarfile.REGTYPE, data=b"{ }: 1\n")
        _tar_member(tar, "nixexprs-1/closed/", tarfile.DIRTYPE, mode=0o200)
        _tar_member(tar, "nixexprs-1/closed/f", tarfile.REGTYPE, data=b"inside\n")
        _tar_member(tar, "nixexprs-1/run", tarfile.REGTYPE, mode=0o755, data=b"#!/bin/sh\n")
        _tar_member(tar, "nixexprs-1/again", tarfile.LNKTYPE, link="nixexprs-1/default.nix")
        _tar_member(tar, "nixexprs-1/link", tarfile.SYMTYPE, link="default.nix")
    with zipfile.ZipFile(root / "channel.zip", "w") as archive:
        archive.writestr("zipped/default.nix", "{ }: 2\n")
    with tarfile.open(root / "empty.tar", "w"):
        pass
    with tarfile.open(root / "two.tar.gz", "w:gz") as tar:
        _tar_member(tar, "one", tarfile.REGTYPE, data=b"1\n")
        _tar_member(tar, "two", tarfile.REGTYPE, data=b"2\n")
    (root / "garbage.tar").write_bytes(b"this is not an archive\n" * 64)


#: Each case: (channelName, tarball).
CHANNELS = {
    "tar-xz": ("nixpkgs", "channel.tar.xz"),
    "zip": ("other", "channel.zip"),
    "empty": ("empty", "empty.tar"),
    "two": ("two", "two.tar.gz"),
    "garbage": ("garbage", "garbage.tar"),
    "separator": ("a/b", "channel.tar.xz"),
}


def _channel(tarballs: Path, case: str) -> str:
    name, tarball = CHANNELS[case]
    return (
        f'derivation {{ name = "parity-channel-{case}"; system = "builtin"; builder = "builtin:unpack-channel"; '
        f'channelName = "{name}"; src = builtins.path {{ path = {tarballs}/{tarball}; name = "{tarball}"; }}; }}'
    )


async def _channels(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    results: dict[str, Any] = {}
    # One set for both routes: gzip and zip write the time of writing, and the tarball is an input.
    tarballs = tmp_path / "tarballs"
    if not tarballs.exists():
        _tarballs(tarballs)
    async with _serve(tmp_path, route) as (url, root):
        for case in CHANNELS:
            rc, out, err, _both = await run_subproc(
                [
                    NIX_BIN,
                    "build",
                    "--store",
                    url,
                    "--impure",
                    "--no-link",
                    "--print-out-paths",
                    "--expr",
                    _channel(tarballs, case),
                ],
                expected_retcode=None,
            )
            paths = out.split()
            results[case] = rc, paths, [_links(root / p.lstrip("/")) for p in paths], err.splitlines()
    results["valid"], results["refs"] = _rows(root)
    return results


async def test_builtin_unpack_channel_unpacks_what_the_daemon_unpacks(tmp_path: Path) -> None:
    """`builtinUnpackChannel` (`builtins/unpack-channel.cc:6`) over `unpackTarfile` (`tarfile.cc:202`): a tar.xz
    with a closed directory, a hard link and a symbolic link, a zip, and the errors of an empty tarball, one of
    two files, one that is no archive, and a channel name with a separator."""
    before = _fallthrough()
    here = await _channels(tmp_path, OpRoute.pynixd)
    assert _fallthrough() == before, "pynixd's sandbox runs every builtin:unpack-channel"
    there = await _channels(tmp_path, OpRoute.daemon)

    assert here == there
    assert here["tar-xz"][0] == here["zip"][0] == 0, here
    assert {"nixpkgs/closed/f", "nixpkgs/again", "nixpkgs/link"} <= set(here["tar-xz"][2][0]), here["tar-xz"]
    assert all(here[case][0] == 1 for case in ("empty", "two", "garbage", "separator")), here


REBUILDS = {
    "same": (True, _script("parity-check-same", "echo same > $out")),
    # The sandbox has no `date` and no `mkdir`; a read of the kernel's UUID needs no binary.
    "differs": (True, _script("parity-check-differs", "read u < /proc/sys/kernel/random/uuid; echo $u > $out")),
    "never-built": (False, _script("parity-check-never", "echo never > $out")),
    # An opaque path: `makeGoal` substitutes it without repair (`worker.cc:138`).
    "opaque": (False, 'builtins.toFile "parity-check-opaque" "opaque"'),
}


def _sent(op: str) -> float:
    return sum(
        sample.value
        for metric in REGISTRY.collect()
        if metric.name == "pynixd_upstream_ops"
        for sample in metric.samples
        if sample.name == "pynixd_upstream_ops_total" and sample.labels.get("op") == op
    )


async def _rebuilds(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    results: dict[str, Any] = {}
    async with _serve(tmp_path, route) as (url, root):
        common = [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--print-out-paths"]
        for name, (first, expr) in REBUILDS.items():
            if first:
                await run_subproc([*common, "--expr", expr])
            rc, out, err, _both = await run_subproc(
                [*common, "--rebuild", "--print-build-logs", "--expr", expr], expected_retcode=None
            )
            results[name] = rc, out.split(), err.replace(str(root), "<root>").splitlines()
    valid, results["refs"] = _rows(root)
    # Each build of `differs` has its own NAR hash.
    results["valid"] = [row[:1] + row[2:] if row[0].endswith("-parity-check-differs") else row for row in valid]
    return results


async def test_a_rebuild_checks_what_the_daemon_checks(tmp_path: Path) -> None:
    """`nix build --rebuild` sends `BuildMode.CHECK`: the same output, a different one, and none built before.

    `registerOutputs` (`derivation-builder.cc:1546`) and `DerivationGoal` (`derivation-goal.cc:143`).
    """
    before = _sent("BuildPathsWithResults"), _sent("BuildDerivation"), _fallthrough()
    here = await _rebuilds(tmp_path, OpRoute.pynixd)
    assert (_sent("BuildPathsWithResults"), _sent("BuildDerivation"), _fallthrough()) == before, (
        "pynixd's sandbox checks every rebuild"
    )
    there = await _rebuilds(tmp_path, OpRoute.daemon)

    assert here == there
    assert here["same"][0] == 0, here["same"]
    assert here["differs"][0] == 1, here["differs"]
    assert any("may not be deterministic" in line for line in here["differs"][2]), here["differs"]
    assert any("checking is not possible" in line for line in here["never-built"][2]), here["never-built"]
    assert here["opaque"][0] == 0, here["opaque"]


KEPT = "note: keeping build directory "
KEEPS = {
    "fails": _script("parity-keep-fails", "echo left > $NIX_BUILD_TOP/left; exit 1"),
    "differs": _script(
        "parity-keep-differs", "echo left > $NIX_BUILD_TOP/left; read u < /proc/sys/kernel/random/uuid; echo $u > $out"
    ),
    "builds": _script("parity-keep-builds", "echo left > $NIX_BUILD_TOP/left; echo built > $out"),
}


async def _keeps(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    """Each case under `--keep-failed`: its exit, its lines with the kept directory named `<kept>`, what it kept."""
    results: dict[str, Any] = {}
    async with _serve(tmp_path, route) as (url, root):
        common = [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--keep-failed"]
        for name, expr in KEEPS.items():
            extra = []
            if name == "differs":
                await run_subproc([*common, "--expr", expr])
                extra = ["--rebuild"]
            rc, _out, err, _both = await run_subproc([*common, *extra, "--expr", expr], expected_retcode=None)
            kept = [line.removeprefix(KEPT).strip('"') for line in err.splitlines() if line.startswith(KEPT)]
            lines = err.replace(str(root), "<root>")
            found: dict[str, Any] = {}
            for directory in map(Path, kept):
                lines = lines.replace(str(directory).replace(str(root), "<root>"), "<kept>")
                found = {
                    "left": (directory / "left").read_text(),
                    "modes": [oct(p.stat().st_mode & 0o7777) for p in (directory.parent, directory)],
                }
            checks = sorted(p.name.split("-", 1)[1] for p in (root / "nix" / "store").glob("*.check"))
            results[name] = rc, lines.splitlines(), len(kept), found, checks
    return results


async def test_keep_failed_keeps_what_the_daemon_keeps(tmp_path: Path) -> None:
    """`cleanupBuild(false)` (`derivation-builder.cc:1697`): a failed build's directory stays, opened to 0755, and
    a check that differs leaves the rebuilt output at `<path>.check` (`:1553`). A build that succeeds keeps none."""
    before = _fallthrough()
    here = await _keeps(tmp_path, OpRoute.pynixd)
    assert _fallthrough() == before, "the daemon built a keep-failed build of the pynixd route"
    there = await _keeps(tmp_path, OpRoute.daemon)

    assert here == there
    assert here["fails"][2:4] == (1, {"left": "left\n", "modes": ["0o755", "0o755"]}), here["fails"]
    assert here["differs"][2] == 1, here["differs"]
    assert here["differs"][4] == ["parity-keep-differs.check"], here["differs"]
    assert any("differs from" in line for line in here["differs"][1]), here["differs"]
    assert (here["builds"][0], here["builds"][2]) == (0, 0), here["builds"]


async def _diff_hooked(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    record = tmp_path / f"{route.name}-record"
    hook = tmp_path / f"{route.name}-hook"
    hook.write_text(f'#!/bin/sh\necho "$@" > {record}\necho hook says hi\n')
    hook.chmod(0o755)
    async with _serve(tmp_path, route, diff_hook=hook) as (url, root):
        common = [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--expr", KEEPS["differs"]]
        await run_subproc(common)
        rc, _out, err, _both = await run_subproc([*common, "--rebuild"], expected_retcode=None)
    checks = sorted(p.name.split("-", 1)[1] for p in (root / "nix" / "store").glob("*.check"))
    return {
        "rc": rc,
        "lines": err.replace(str(hook), "<hook>").replace(str(root), "<root>").splitlines(),
        "checks": checks,
        "ran": record.exists(),
    }


async def test_a_diff_hook_fails_as_the_daemon_fails_it_when_not_root(tmp_path: Path) -> None:
    """`handleDiffHook` (`derivation-builder.cc:121`) drops the groups of a daemon that is not root, which fails."""
    before = _fallthrough()
    here = await _diff_hooked(tmp_path, OpRoute.pynixd)
    assert _fallthrough() == before, "the daemon checked the pynixd route's build"
    there = await _diff_hooked(tmp_path, OpRoute.daemon)

    assert here == there
    assert (here["rc"], here["checks"], here["ran"]) == (1, ["parity-keep-differs.check"], False), here
    assert any("diff hook execution failed" in line for line in here["lines"]), here


HOOKED = (
    'derivation { name = "parity-hooked"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'outputs = [ "out" "dev" ]; args = [ "-c" "echo out > $out; echo dev > $dev" ]; }'
)
HOOKED_FAILS = _script("parity-hooked-fails", "echo fails > $out")
# Shell builtins only: the hook runs with the daemon's environment. The last line has no line feed.
POST_BUILD_HOOK = """#!/bin/sh
read code < {code}
echo "$#|$DRV_PATH|$OUT_PATHS" >> {record}
printf '%s\\n' "$NIX_CONFIG" > {record}.conf
echo one
echo two >&2
printf tail
exit $code
"""
POST_BUILD_CASES = {"builds": (0, HOOKED, ()), "checks": (0, HOOKED, ("--rebuild",)), "fails": (1, HOOKED_FAILS, ())}


def _activities(err: str) -> list[dict[str, Any]]:
    """The client's `internal-json` events of each post-build-hook activity, and the stop of each build's."""
    events = [json.loads(line.removeprefix("@nix ")) for line in err.splitlines() if line.startswith("@nix ")]
    builds = {e["id"] for e in events if e["action"] == "start" and e["type"] == 105}
    hooks = {e["id"] for e in events if e["action"] == "start" and e["type"] == 110}

    def name(act: int) -> str | int:
        return "build" if act in builds else "hook" if act in hooks else act if act == 0 else "other"

    return [
        {k: name(v) if k in {"id", "parent"} else v for k, v in e.items()}
        for e in events
        if e.get("id") in hooks or (e["action"] == "stop" and e["id"] in builds)
    ]


async def _post_build_hooked(tmp_path: Path, route: OpRoute, *, inherited: bool) -> dict[str, Any]:
    """Each case under a post-build-hook: its exit, its activities, its other lines and what the hook saw.

    With *inherited*, the daemon's environment holds `NIX_CONFIG`.
    """
    here = tmp_path / route.name
    code, record, hook = (
        tmp_path / f"{route.name}-code",
        tmp_path / f"{route.name}-record",
        tmp_path / f"{route.name}-hook",
    )
    hook.write_text(POST_BUILD_HOOK.format(code=code, record=record))
    hook.chmod(0o755)
    results: dict[str, Any] = {}
    async with _serve(tmp_path, route, post_build_hook=hook, config_file=not inherited) as (url, root):
        common = [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--log-format", "internal-json", "-v"]
        for name, (status, expr, extra) in POST_BUILD_CASES.items():
            code.write_text(f"{status}\n")
            rc, _out, err, _both = await run_subproc([*common, *extra, "--expr", expr], expected_retcode=None)
            err = err.replace(str(hook), "<hook>").replace(str(here), "<here>")
            lines = [line for line in err.splitlines() if not line.startswith("@nix ")]
            config = Path(f"{record}.conf").read_text().replace(str(hook), "<hook>").replace(str(here), "<here>")
            results[name] = rc, _activities(err), lines, config.splitlines()
    results["record"] = record.read_text().splitlines()
    results["valid"] = _rows(root)
    return results


@pytest.mark.parametrize("inherited", [False, True], ids=["dumped", "inherited"])
async def test_a_post_build_hook_runs_as_the_daemon_runs_it(tmp_path: Path, inherited: bool) -> None:
    """`runPostBuildHook` (`derivation-building-goal.cc:1105`) after a build and after a check (`:1066`).

    The hook gets the derivation, its outputs and the settings in its environment, and each line it writes is
    a result of its own activity. A `NIX_CONFIG` in the daemon's environment replaces the settings (`:1125`).
    """
    before = _fallthrough()
    here = await _post_build_hooked(tmp_path, OpRoute.pynixd, inherited=inherited)
    assert _fallthrough() == before, "the daemon built a post-build-hook build of the pynixd route"
    there = await _post_build_hooked(tmp_path, OpRoute.daemon, inherited=inherited)

    for name in POST_BUILD_CASES:
        assert here[name][:3] == there[name][:3], name
        mine, theirs = set(here[name][3]), set(there[name][3])
        assert (sorted(mine - theirs), sorted(theirs - mine)) == ([], []), name
        assert here[name][3] == there[name][3], name
    assert (here["record"], here["valid"]) == (there["record"], there["valid"])
    assert [here[name][0] for name in POST_BUILD_CASES] == [0, 0, 1], here
    assert len(here["record"]) == 3, here["record"]
    results = [e["fields"] for e in here["builds"][1] if e["action"] == "result"]
    assert results == [["one"], ["two"], ["tail\n"]], here["builds"][1]
    assert "post-build-hook = <hook>" in here["builds"][3], here["builds"][3]
    # The session sets `keep-going`, and only the dump shows it.
    assert ("keep-going = false" in here["builds"][3]) is not inherited, here["builds"][3]


def _chain(tag: str) -> tuple[str, str]:
    """A dependency and a top whose output refers to it, as two expressions."""
    dep = (
        f'derivation {{ name = "parity-repair-{tag}-dep"; system = builtins.currentSystem; builder = "/bin/sh"; '
        'args = [ "-c" "echo dep > $out" ]; }'
    )
    top = (
        f"let dep = {dep}; in "
        f'derivation {{ name = "parity-repair-{tag}-top"; system = builtins.currentSystem; builder = "/bin/sh"; '
        'args = [ "-c" "echo ${dep} > $out" ]; }'
    )
    return dep, top


# Each case breaks one path before `nix build --repair` of the top: none, the top, or the dependency.
REPAIRS = ("good", "top", "dep", "gone")


async def _repairs(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    results: dict[str, Any] = {}
    async with _serve(tmp_path, route) as (url, root):
        common = [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--print-out-paths"]
        for tag in REPAIRS:
            dep_expr, top_expr = _chain(tag)
            _rc, top, _err, _both = await run_subproc([*common, "--expr", top_expr])
            _rc, dep, _err, _both = await run_subproc([*common, "--expr", dep_expr])
            files = {"top": root / "nix" / "store" / top.strip().rsplit("/", 1)[-1]}
            files["dep"] = root / "nix" / "store" / dep.strip().rsplit("/", 1)[-1]
            if tag in ("top", "dep"):
                files[tag].chmod(0o644)
                files[tag].write_bytes(b"corrupt\n")
            elif tag == "gone":
                files["dep"].unlink()
            rc, out, err, _both = await run_subproc(
                [*common, "--repair", "--print-build-logs", "--expr", top_expr], expected_retcode=None
            )
            contents = {name: path.read_text() if path.exists() else None for name, path in files.items()}
            results[tag] = rc, out.split(), err.replace(str(root), "<root>").splitlines(), contents
    results["valid"], results["refs"] = _rows(root)
    return results


async def test_a_repair_rebuilds_what_the_daemon_finds_corrupt(tmp_path: Path) -> None:
    """`nix build --repair` sends `BuildMode.REPAIR`: each output and its closure is checked, a bad one built again.

    `DerivationGoal` (`derivation-goal.cc:78`), `repairClosure` (`:277`) and `pathContentsGood` (`worker.cc:553`).
    Both routes check the same paths and print the same lines. The daemon then builds nothing and leaves the
    corrupt path (Lillecarl/nix#393); pynixd builds it again, so its client also reads `repairing outputs of`.
    """
    before = _sent("BuildPathsWithResults"), _sent("BuildDerivation"), _fallthrough()
    here = await _repairs(tmp_path, OpRoute.pynixd)
    assert (_sent("BuildPathsWithResults"), _sent("BuildDerivation"), _fallthrough()) == before, (
        "pynixd's goals and sandbox repair every case"
    )
    there = await _repairs(tmp_path, OpRoute.daemon)

    assert (here["valid"], here["refs"]) == (there["valid"], there["refs"])
    assert here["good"] == there["good"]
    for tag in REPAIRS:
        rc, out, err, contents = here[tag]
        daemon_rc, daemon_out, daemon_err, daemon_contents = there[tag]
        assert (rc, out) == (daemon_rc, daemon_out) == (0, out), (here[tag], there[tag])
        assert [line for line in err if not line.startswith("repairing outputs of ")] == daemon_err, (
            here[tag],
            there[tag],
        )
        assert contents["dep"] == "dep\n", here[tag]
        assert (contents["top"] or "").strip().endswith(f"-parity-repair-{tag}-dep"), here[tag]
        if tag != "good":
            assert daemon_contents != contents, there[tag]
            assert any(line.startswith("repairing outputs of ") for line in err), here[tag]
    assert any("is corrupted or missing!" in line for line in here["top"][2]), here["top"]
    assert any("found corrupted or missing path" in line for line in here["gone"][2]), here["gone"]


async def _opaque_repair(tmp_path: Path, route: OpRoute) -> tuple[int, list[str], list[str]]:
    async with _serve(tmp_path, route) as (url, root):
        common = [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--print-out-paths"]
        dep_expr, _top_expr = _chain("opaque")
        _rc, dep, _err, _both = await run_subproc([*common, "--expr", dep_expr])
        rc, out, err, _both = await run_subproc([*common, "--repair", dep.strip()], expected_retcode=None)
    return rc, out.split(), err.replace(str(root), "<root>").splitlines()


async def test_a_repair_of_a_store_path_substitutes_it_as_the_daemon_does(tmp_path: Path) -> None:
    """`nix build --repair <path>`: a substitution with repair alone (`worker.cc:138`), so a good path fails here."""
    before = _sent("BuildPathsWithResults")
    here = await _opaque_repair(tmp_path, OpRoute.pynixd)
    assert _sent("BuildPathsWithResults") == before, "pynixd's goals repair a store path"
    there = await _opaque_repair(tmp_path, OpRoute.daemon)

    assert here == there
    assert here[0] != 0, here
    assert any("there is no substituter that can build it" in line for line in here[2]), here


async def _verify_repair(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    async with _serve(tmp_path, route) as (url, root):
        common = [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--print-out-paths"]
        dep_expr, top_expr = _chain("verify")
        _rc, top, _err, _both = await run_subproc([*common, "--expr", top_expr])
        _rc, dep, _err, _both = await run_subproc([*common, "--expr", dep_expr])
        source = tmp_path / route.name / "parity-verify-source"
        source.write_text("source\n")
        _rc, added, _err, _both = await run_subproc([NIX_BIN, "store", "add-file", "--store", url, str(source)])
        files = {
            name: root / "nix" / "store" / path.strip().rsplit("/", 1)[-1]
            for name, path in (("top", top), ("dep", dep), ("source", added))
        }
        for name in ("top", "source"):
            files[name].chmod(0o644)
            files[name].write_bytes(b"corrupt\n")
        files["dep"].unlink()
        rc, _out, err, _both = await run_subproc(
            [str(NIX_BIN.parent / "nix-store"), "--store", url, "--verify", "--check-contents", "--repair"],
            expected_retcode=None,
        )
        contents = {name: path.read_text() if path.exists() else None for name, path in files.items()}
    valid, refs = _rows(root)
    return {
        "rc": rc,
        "err": err.replace(str(root), "<root>").splitlines(),
        "contents": contents,
        "valid": valid,
        "refs": refs,
    }


async def test_verify_repair_rebuilds_what_the_daemon_finds_corrupt(tmp_path: Path) -> None:
    """`nix-store --verify --check-contents --repair`: `repairPath` (`build/entry-points.cc:118`) for each bad path.

    A modified output and a lost one that a valid path refers to build again from their derivers
    (`local-store.cc:1438`, `:1551`); a modified source has none, and "cannot repair path" is an error. Both
    routes print the same lines; the daemon builds nothing (Lillecarl/nix#393), pynixd restores both outputs.
    """
    before = _sent("VerifyStore"), _sent("BuildPathsWithResults"), _sent("BuildDerivation")
    here = await _verify_repair(tmp_path, OpRoute.pynixd)
    assert (_sent("VerifyStore"), _sent("BuildPathsWithResults"), _sent("BuildDerivation")) == before, (
        "pynixd verifies and repairs"
    )
    there = await _verify_repair(tmp_path, OpRoute.daemon)

    assert (here["rc"], here["valid"], here["refs"]) == (there["rc"], there["valid"], there["refs"])
    # The daemon still lacks the dependency when the top's repair checks it again, and says so a second time.
    missing = [
        i for i, line in enumerate(there["err"]) if "-parity-repair-verify-dep' is corrupted or missing!" in line
    ]
    assert len(missing) == 2, there["err"]
    expected = there["err"][: missing[1]] + there["err"][missing[1] + 1 :]
    assert [line for line in here["err"] if not line.startswith("repairing outputs of ")] == expected, (
        here["err"],
        there["err"],
    )
    assert here["rc"] != 0, here
    assert any("cannot repair path" in line for line in here["err"]), here["err"]
    assert here["contents"]["dep"] == "dep\n", here
    assert (here["contents"]["top"] or "").strip().endswith("-parity-repair-verify-dep"), here
    assert here["contents"]["source"] == there["contents"]["source"] == "corrupt\n"
    assert (there["contents"]["top"], there["contents"]["dep"]) == ("corrupt\n", None), there


# `recursive.nix` of Nix's functional suite, with what the sandbox has: a shell and no other program.
RECURSIVE = """
let
  nix = builtins.storePath "@NIX@";
  inner = builtins.toFile "parity-inner.nix" ''
    derivation { name = "parity-inner"; system = builtins.currentSystem; builder = "/bin/sh";
      args = [ "-c" "echo $fnord blaat > $out" ]; fnord = builtins.toFile "fnord" "fnord"; }
  '';
in derivation {
  name = "parity-recursive"; system = builtins.currentSystem; builder = "/bin/sh";
  requiredSystemFeatures = [ "recursive-nix" ];
  PATH = "${nix}/bin";
  dummy = builtins.toFile "dummy" "bla bla";
  unreachable = builtins.unsafeDiscardStringContext (builtins.toFile "unreachable" "x");
  inherit inner;
  args = [ (builtins.toFile "parity-recursive.sh" ''
    set -e
    opts="--extra-experimental-features nix-command"
    nix $opts path-info $dummy
    nix $opts build --no-link $dummy
    if nix $opts path-info $unreachable; then exit 10; fi
    if nix $opts build --no-link $unreachable; then exit 11; fi
    echo foobar > foobar
    foobar=$(nix $opts store add-path ./foobar)
    nix $opts path-info $foobar
    n=0
    for p in $(nix $opts path-info --all); do n=$((n+1)); done
    inner1=$(nix $opts build --no-link --print-out-paths --file $inner)
    nix $opts key generate-secret --key-name k > key
    signed=$(nix $opts store sign --key-file ./key $dummy 2>&1 || true)
    echo "$foobar $inner1 $n" > $out
    nix $opts path-info --json $inner1 >> $out
    echo "$signed" >> $out
  '') ];
}
"""


def _nix_store_path(binary: Path = NIX_BIN) -> str:
    return "/".join(os.path.realpath(binary).split("/")[:4])


async def _recursive(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    nix = _nix_store_path()
    async with _serve(tmp_path, route, experimental_features=("recursive-nix",)) as (url, root):
        await run_subproc([NIX_BIN, "copy", "--no-check-sigs", "--to", url, nix])
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
                RECURSIVE.replace("@NIX@", nix),
            ]
        )
        top = out.strip()
        _rc, info, _err, _both = await run_subproc([NIX_BIN, "path-info", "--store", url, "--json", "--recursive", top])
        contents = (root / top.lstrip("/")).read_text()
    valid, refs = _rows(root)
    return {
        "top": top,
        "contents": contents,
        "info": _without_times(json.loads(info)),
        "valid": valid,
        "refs": refs,
        "log": [line for line in err.splitlines() if line.startswith("parity-")],
    }


async def test_a_recursive_nix_build_builds_what_the_daemon_builds(tmp_path: Path) -> None:
    """`startDaemon` (`derivation-builder.cc:1153`) serves a `RestrictedStore` (`restricted-store.cc`) in the sandbox.

    The builder reaches its inputs and what it adds or builds, and nothing else (`recursive.sh`).
    """
    before = _fallthrough()
    here = await _recursive(tmp_path, OpRoute.pynixd)
    assert _fallthrough() == before, "pynixd's sandbox builds a recursive-nix derivation"
    there = await _recursive(tmp_path, OpRoute.daemon)

    for key in ("top", "contents", "info", "valid", "refs", "log"):
        assert here[key] == there[key], (key, here[key], there[key])
    first, *_info = here["contents"].splitlines()
    foobar, inner1, count = first.split()
    assert foobar.endswith("-foobar") and inner1.endswith("-parity-inner"), first
    assert int(count) > 2, first
    # `Store::unsupported` names the restricted store.
    assert "operation 'addSignatures' is not supported by store" in here["contents"], here["contents"]
    assert {foobar, inner1} <= set(here["info"][here["top"]]["references"]), here["info"]


# `builder-rpc-v0` of Nix's master branch: the builder adds each output and submits it, and sees no output path.
RPC = """
let nix = builtins.storePath "@NIX@"; in
derivation {
  name = "parity-rpc"; system = builtins.currentSystem; builder = "/bin/sh";
  __contentAddressed = true; outputHashMode = "recursive"; outputHashAlgo = "sha256";
  outputs = [ "out" "dev" ];
  requiredSystemFeatures = [ "builder-rpc-v0" ];
  PATH = "${nix}/bin";
  dummy = builtins.toFile "dummy" "bla bla";
  args = [ (builtins.toFile "parity-rpc.sh" ''
    set -e
    o="--extra-experimental-features nix-command --extra-experimental-features dynamic-derivations"
    refused=$(nix $o path-info $dummy 2>&1 || true)
    echo "dev of $dummy" > dev
    dev=$(nix $o store add --scan --name parity-rpc-dev ./dev)
    nix $o store submit-output $dev dev
    duplicate=$(nix $o store submit-output $dev dev 2>&1 || true)
    plain=$(nix $o store add --name parity-rpc-plain ./dev)
    printf '%s\\n' "out:$out" "$dev" "$plain" "$refused" "$duplicate" > out
    out=$(nix $o store add --scan --name parity-rpc ./out)
    nix $o store submit-output $out out
  '') ];
}
"""


async def _rpc(tmp_path: Path, route: OpRoute, nix_bin: Path) -> dict[str, Any]:
    nix = _nix_store_path(nix_bin)
    features = ("ca-derivations", "dynamic-derivations")
    matrix = {system: set(fs) | {"builder-rpc-v0"} for system, fs in _NO_PROBE_FEATURE_MATRIX.items()}
    async with _serve(tmp_path, route, experimental_features=features, nix_bin=nix_bin, feature_matrix=matrix) as (
        url,
        root,
    ):
        await run_subproc([nix_bin, "copy", "--no-check-sigs", "--to", url, nix])
        _rc, out, err, _both = await run_subproc(
            [
                nix_bin,
                "build",
                "--store",
                url,
                "--extra-experimental-features",
                " ".join(features),
                "--impure",
                "--no-link",
                "--print-out-paths",
                "--print-build-logs",
                "--expr",
                RPC.replace("@NIX@", nix),
            ]
        )
        paths = out.split()
        _rc, info, _err, _both = await run_subproc(
            [nix_bin, "path-info", "--store", url, "--json", "--recursive", *paths]
        )
        contents = {path: (root / path.lstrip("/")).read_text() for path in paths}
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
        "contents": contents,
        "info": _without_times(json.loads(info)),
        "valid": valid,
        "refs": refs,
        "trace": trace,
        "log": [line for line in err.splitlines() if line.startswith("parity-")],
    }


async def test_a_builder_rpc_build_builds_what_the_daemon_builds(tmp_path: Path) -> None:
    """`startDaemon` with `RecursiveSubmitted` (`unix-derivation-builder.cc:672`) and `checkSubmittedOutputs`
    (`derivation-builder-impl.cc:715`): the builder submits its outputs over the allowlist of `daemon.cc:329`."""
    if NIX_GIT_BIN is None:
        pytest.skip("NIX_GIT_BIN names no Nix of the master branch")
    before = _fallthrough()
    here = await _rpc(tmp_path, OpRoute.pynixd, NIX_GIT_BIN)
    assert _fallthrough() == before, "pynixd's sandbox builds a builder-rpc-v0 derivation"
    there = await _rpc(tmp_path, OpRoute.daemon, NIX_GIT_BIN)

    for key in ("paths", "contents", "info", "valid", "refs", "trace", "log"):
        assert here[key] == there[key], (key, here[key], there[key])
    dev, out = sorted(here["paths"], key=lambda p: not p.endswith("-dev"))
    lines = here["contents"][out].splitlines()
    assert lines[:2] == ["out:", dev], lines
    assert "not allowed inside derivation" in here["contents"][out], lines
    assert "duplicate output" in here["contents"][out], lines
    assert dev in here["info"][out]["references"], here["info"]
    assert {row[1] for row in here["trace"]} >= {"out", "dev"}, here["trace"]
