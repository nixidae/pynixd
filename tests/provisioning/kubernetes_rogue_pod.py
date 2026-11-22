"""A build pod that pushes paths its grant does not name, then claims success. nixidae/nixidae#1560, spike.

The pod's program, not a phase: the `rogue` template runs it with pynixd's
venv Python. It uses the key and the host key central gave it, as a real
pod does, but adds two paths of its own to a chroot store and copies each to
central. Central takes the content-addressed one, which vouches for itself,
and refuses the input-addressed one. It then refuses the claimed result, as
the derivation's output never reached its store.
"""

from __future__ import annotations

import os
import sys

import anyio

from nix_daemon_protocol.build_derivation import BuildDerivationResponse
from pynixd.build_job import JOB_NAME, KEY_NAME, Job, result_message
from pynixd.build_result import success
from pynixd.serde import BuildResultStatus

WORK = anyio.Path("/build")
FORGED = "0" * 32


async def main(job_dir: anyio.Path, result: anyio.Path) -> int:
    job = Job.model_validate_json(await (job_dir / JOB_NAME).read_text())
    # ssh refuses a key that others can read, and the Secret's files are 0644.
    key = WORK / "key"
    await key.write_bytes(await (job_dir / KEY_NAME).read_bytes())
    await key.chmod(0o600)
    known_hosts = WORK / "known_hosts"
    await known_hosts.write_text(f"[{job.central_host}]:{job.central_port} {job.central_host_key}\n")
    foreign = WORK / "foreign.txt"
    await foreign.write_text("not an output of the granted derivation\n")

    # Content-addressed: the grant takes it, as any untrusted client may add it.
    added = await anyio.run_process(["nix", "store", "add-file", "--store", str(WORK / "rogue"), str(foreign)])
    content_addressed = added.stdout.decode().strip()
    # Input-addressed, under a name of its own choosing: the claim the grant exists to refuse.
    claimed_path = f"/nix/store/{FORGED}-rogue"
    forged = WORK / "rogue" / claimed_path.removeprefix("/")
    await forged.mkdir(parents=True)
    await (forged / "claim").write_text("a path no derivation of the grant names\n")
    await anyio.run_process(
        ["nix-store", "--store", str(WORK / "rogue"), "--register-validity"], input=f"{claimed_path}\n\n0\n".encode()
    )
    ssh_options = f"-p {job.central_port} -i {key} -o IdentitiesOnly=yes -o UserKnownHostsFile={known_hosts}"
    for path in (content_addressed, claimed_path):
        print(f"rogue: added {path}", flush=True)
        copied = await anyio.run_process(
            ["nix", "copy", "--from", str(WORK / "rogue"), "--to", f"ssh-ng://pod@{job.central_host}", path],
            check=False,
            env={**os.environ, "NIX_SSHOPTS": f"{ssh_options} -o StrictHostKeyChecking=yes"},
        )
        print(f"rogue: nix copy exited {copied.returncode}", flush=True)
        print(copied.stderr.decode(errors="replace"), flush=True)
    claimed = BuildDerivationResponse(result=success(BuildResultStatus.BUILT))
    await result.write_text(await result_message(claimed))
    return 0


if __name__ == "__main__":
    sys.exit(anyio.run(main, anyio.Path(sys.argv[1]), anyio.Path("/dev/termination-log")))
