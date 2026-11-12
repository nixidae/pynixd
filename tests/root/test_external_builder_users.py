"""An external builder runs as a build user, and pynixd keeps the build's top directory.

NIX-DEVIATION (#27) in `_write_external_job` (`pynixd_store/builder/local.py`): `startChild` gives the top
directory to the build user (`chownToBuilder(topTmpDir)`, `external-derivation-builder.cc:92`); pynixd
keeps it and gives the user `build.json` and `build`. This holds both routes to that difference: the same
program, run as a build user, writes in the top directory on the daemon's route and not on pynixd's.
nixidae/nixidae#1303.

The guest's `root` suite runs these as root; anywhere else they skip.
"""

from __future__ import annotations

import grp
import json
import os
import pwd
import sys
from pathlib import Path
from typing import Any

import pytest

from pynixd.op_routes import OpRoute
from tests._conftest.helpers import rmtree_robust
from tests.conftest import run_subproc
from tests.functional.test_build_without_sandbox import ALONE, _keep_store_dirs, _serve

__all__ = ["_keep_store_dirs"]

GROUP = "nixbld"
# Not under /tmp: with build users, Nix refuses a build directory with a world-writable parent
# (`checkNotWorldWritable`, `derivation-builder.cc:356`).
HERE = Path("/var/lib/pynixd-root-tests/external-builder")

pytestmark = pytest.mark.skipif(os.geteuid() != 0, reason="build users need root")

PROGRAM = """#!{python}
import json, os, sys
record, job = sys.argv[1], sys.argv[-1]
top = os.path.dirname(job)
def writes(directory):
    try:
        with open(os.path.join(directory, "probe"), "w"):
            return True
    except PermissionError:
        return False
with open(job, encoding="utf-8") as f:
    doc = json.load(f)
job_stat, top_stat = os.stat(job), os.stat(top)
with open(record, "w", encoding="utf-8") as f:
    json.dump({{
        "uid": os.getuid(),
        "job_uid": job_stat.st_uid,
        "job_mode": job_stat.st_mode & 0o777,
        "top_uid": top_stat.st_uid,
        "writes_top": writes(top),
        "writes_build": writes(os.getcwd()),
    }}, f)
print("\\x02", flush=True)
with open(doc["outputs"]["out"], "w", encoding="utf-8") as f:
    f.write("done\\n")
"""

DRV = 'derivation { name = "external"; system = "x68_46-xunil"; builder = "/bin/sh"; args = [ "-c" "never run" ]; }'


@pytest.fixture(autouse=True)
def _clean() -> None:
    rmtree_robust(HERE)


def _build_uids() -> set[int]:
    return {pwd.getpwnam(member).pw_uid for member in grp.getgrnam(GROUP).gr_mem}


async def _build(route: OpRoute) -> dict[str, Any]:
    work = HERE / route.name
    work.mkdir(parents=True)
    shared = HERE / f"shared-{route.name}"
    shared.mkdir()
    shared.chmod(0o777)
    program, record = HERE / f"program-{route.name}", shared / "record.json"
    program.write_text(PROGRAM.format(python=sys.executable))
    program.chmod(0o755)
    builders = [{"systems": ["x68_46-xunil"], "program": str(program), "args": [str(record)]}]
    client = {**ALONE, "extra-experimental-features": "external-builders", "external-builders": json.dumps(builders)}
    # The pynixd route runs no daemon, so nothing else can build it there.
    async with _serve(
        work,
        route,
        "true",
        experimental_features=("external-builders",),
        build_users_group=GROUP,
        daemon=route is OpRoute.daemon,
    ) as (nix, env, _state):
        await run_subproc([*nix[:1], "build", *nix[1:], "--no-link", "--expr", DRV], env=dict(env), nix_config=client)
    seen: dict[str, Any] = json.loads(record.read_text())
    return seen


async def test_an_external_builder_runs_as_a_build_user_in_a_top_directory_it_cannot_write() -> None:
    seen = await _build(OpRoute.pynixd)

    assert seen["uid"] in _build_uids(), f"uid {seen['uid']} ran the external builder, not a build user"
    assert seen["job_uid"] == seen["uid"]
    assert seen["job_mode"] == 0o600
    assert seen["top_uid"] == 0
    assert seen["writes_build"]
    assert not seen["writes_top"]


async def test_the_daemon_gives_the_top_directory_to_the_build_user() -> None:
    """The other side of the deviation: Nix's external builder may write beside `build.json`."""
    seen = await _build(OpRoute.daemon)

    assert seen["uid"] in _build_uids()
    assert seen["top_uid"] == seen["uid"]
    assert seen["writes_build"]
    assert seen["writes_top"]
