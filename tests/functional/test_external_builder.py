"""pynixd's builder runs an external builder as `nix-daemon` does (`external-derivation-builder.cc`).

The same derivation for a system no store has, `x68_46-xunil`, builds on each route through a
program in `external-builders`, as in Nix's `tests/functional/external-builders.sh:21`. The program
records its arguments, its working directory and `build.json`, prints set-up lines up to `\\2`, and
writes the output. Both routes must hand it the same things. The pynixd route's store runs no
daemon.
"""

from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING, Any

import pytest

from pynixd.op_routes import OpRoute
from tests.conftest import run_subproc
from tests.functional.test_build_without_sandbox import ALONE, _errors, _keep_store_dirs, _rows, _serve

if TYPE_CHECKING:
    from pathlib import Path

__all__ = ["_keep_store_dirs"]

PROGRAM = """#!{python}
import json, os, sys
record, job = sys.argv[1], sys.argv[-1]
with open(job, encoding="utf-8") as f:
    doc = json.load(f)
with open(record, "a", encoding="utf-8") as f:
    f.write(json.dumps({{
        "args": sys.argv[2:-1],
        "job": job,
        "cwd": os.getcwd(),
        "doc": doc,
        "inherits_the_derivation": "greeting" in os.environ,
    }}) + "\\n")
print("a set-up line", flush=True)
print("\\x02", flush=True)
print("the external builder ran", flush=True)
with open(doc["outputs"]["out"], "w", encoding="utf-8") as f:
    f.write(doc["env"]["greeting"] + "bar\\n")
"""

DRV = (
    'derivation { name = "external"; system = "x68_46-xunil"; builder = "/bin/sh"; '
    'args = [ "-c" "never run" ]; greeting = "foo"; src = builtins.toFile "input" "x"; }'
)


def _program(tmp_path: Path) -> tuple[Path, Path]:
    program, record = tmp_path / "external-builder", tmp_path / "record.jsonl"
    program.write_text(PROGRAM.format(python=sys.executable))
    program.chmod(0o755)
    return program, record


def _client(program: Path, record: Path) -> dict[str, str]:
    builders = [{"systems": ["x68_46-xunil"], "program": str(program), "args": [str(record), "bla"]}]
    return {**ALONE, "extra-experimental-features": "external-builders", "external-builders": json.dumps(builders)}


async def _build(tmp_path: Path, route: OpRoute, *, feature: bool = True) -> dict[str, Any]:
    program, record = _program(tmp_path)
    record.unlink(missing_ok=True)
    features = ("external-builders",) if feature else ()
    # The pynixd route runs no daemon, so nothing else can build it there.
    daemon = route is OpRoute.daemon
    async with _serve(tmp_path, route, "true", experimental_features=features, daemon=daemon) as (nix, env, state):
        rc, out, err, _both = await run_subproc(
            [*nix[:1], "build", *nix[1:], "--no-link", "--print-out-paths", "--print-build-logs", "--expr", DRV],
            expected_retcode=None,
            env=dict(env),
            nix_config=_client(program, record),
        )
        store = state.parent.parent / "store"
        if rc != 0:
            return {"rc": rc, "error": [line.replace(str(store), "<store>") for line in _errors(err)]}
        contents = (store / out.strip().rsplit("/", 1)[-1]).read_text()
    (seen,) = [json.loads(line) for line in record.read_text().splitlines()]
    doc = seen["doc"]
    top, tmp = doc.pop("topTmpDir"), doc.pop("tmpDir")
    assert seen.pop("job") == f"{top}/build.json"
    assert seen.pop("cwd") == tmp == f"{top}/build"
    return {
        "rc": rc,
        "contents": contents,
        "seen": seen,
        "rows": _rows(state),
        "log": [line for line in err.splitlines() if line.startswith("external>")],
        "set-up lines in the log": "a set-up line" in err or "\x02" in err,
    }


async def test_an_external_builder_sees_what_the_daemons_shows_it(tmp_path: Path) -> None:
    here = await _build(tmp_path, OpRoute.pynixd)
    there = await _build(tmp_path, OpRoute.daemon)
    assert here["rc"] == there["rc"] == 0
    assert here["contents"] == there["contents"] == "foobar\n"
    assert here["seen"]["args"] == ["bla"]
    assert not here["seen"]["inherits_the_derivation"]
    assert here["seen"]["doc"]["env"]["NIX_BUILD_TOP"] == "/build"
    assert here["seen"] == there["seen"]
    assert here["rows"] == there["rows"]
    assert here["log"] == there["log"] == ["external> the external builder ran"]
    assert not here["set-up lines in the log"]
    assert not there["set-up lines in the log"]


@pytest.mark.parametrize("route", [OpRoute.pynixd, OpRoute.daemon])
async def test_without_the_feature_the_build_fails_in_nix_words(tmp_path: Path, route: OpRoute) -> None:
    """`ExternalDerivationBuilder` requires `Xp::ExternalBuilders` (`external-derivation-builder.cc:19`)."""
    failed = await _build(tmp_path, route, feature=False)
    assert failed["rc"] == 1
    assert any("experimental Nix feature 'external-builders' is disabled" in line for line in failed["error"])
