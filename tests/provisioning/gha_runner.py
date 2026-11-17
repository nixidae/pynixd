"""A GitHub Actions runner's part of one job, as a guest's user data: nixidae/nixidae#1473.

`fake_gha.py` appends `main(JOB)`, where JOB holds the job's steps with
their expressions already evaluated. Each step runs as the `runner` user in
`bash -eo pipefail`, GitHub's `shell: bash`, with its environment and its
timeout. The runner reports the job's conclusion to the fake, then powers the
guest off: a hosted runner ends with its job.
"""

from __future__ import annotations

import json
import os
import pwd
import subprocess
import sys
import urllib.request
from typing import Any

type Job = dict[str, Any]
"""`fake_gha.py`'s `job_of`: the report URL and the steps."""


def step_ok(step: dict[str, Any], workspace: str) -> bool:  # noqa: ANN401 -- one of `Job`'s steps
    runner = pwd.getpwnam("runner")
    env = {
        "PATH": os.environ["PATH"],
        "HOME": runner.pw_dir,
        "USER": "runner",
        "RUNNER_OS": "Linux",
        **step["env"],
    }
    minutes = step["timeout_minutes"]
    print(f"pynixd-runner: step {step['name']!r}", file=sys.stderr, flush=True)
    try:
        done = subprocess.run(
            ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", step["run"]],
            env=env,
            cwd=workspace,
            user=runner.pw_uid,
            group=runner.pw_gid,
            extra_groups=[],
            timeout=None if minutes is None else float(minutes) * 60,
            check=False,
        )
    except subprocess.TimeoutExpired:
        print(f"pynixd-runner: step {step['name']!r} timed out", file=sys.stderr, flush=True)
        return False
    if done.returncode:
        print(f"pynixd-runner: step {step['name']!r} failed with {done.returncode}", file=sys.stderr, flush=True)
    return done.returncode == 0


def main(job: Job) -> None:
    runner = pwd.getpwnam("runner")
    workspace = os.path.join(runner.pw_dir, "work")
    os.makedirs(workspace, exist_ok=True)
    os.chown(workspace, runner.pw_uid, runner.pw_gid)
    conclusion = "success" if all(step_ok(step, workspace) for step in job["steps"]) else "failure"
    print(f"pynixd-runner: conclusion {conclusion}", file=sys.stderr, flush=True)
    request = urllib.request.Request(
        job["report"],
        data=json.dumps({"conclusion": conclusion}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(request, timeout=30).close()
    finally:
        subprocess.run(["systemctl", "poweroff"], check=False)
