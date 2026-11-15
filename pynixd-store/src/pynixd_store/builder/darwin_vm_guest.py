"""The darwin build VM's side in the guest: run the helper, then hand the outputs to the host (`darwin_vm`).

    python -I -m pynixd_store.builder.darwin_vm_guest TOP/darwin-vm.json

The helper's file descriptor 3 is `top/vm-status`, as in the Linux build VM. Its output is this process's, which
the guest's agent sends to the host. The outputs are on the tmpfs over the store, which the host cannot see: each
one is copied into `top/vm-out`, a writable share. Not with macOS `cp -p`: its copyfile gave a 0644 file 0755 on
the share (measured).

The build directory is a tmpfs too: a file made on the share costs about 3 ms, and unpacking coreutils' 3786
files took 6 s there against 1 s on the host (measured). With `keep-failed`, a failed build's directory is copied
to `top/vm-build`, which the launcher puts in its place.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

from .darwin_vm import KEPT_NAME, OUTPUTS_NAME
from .vm import EXIT_NAME, STATUS_NAME


def copy(source: Path, target: Path, *, owners: bool = False) -> None:
    """*source* to *target*: contents, the mode's permission bits and symbolic links, with *owners* the owners too.

    Any other kind of file is left out: a FIFO would block the read.
    """
    st = source.lstat()
    if stat.S_ISLNK(st.st_mode):
        os.symlink(os.readlink(source), target)
    elif stat.S_ISDIR(st.st_mode):
        target.mkdir()
        for child in sorted(source.iterdir()):
            copy(child, target / child.name, owners=owners)
    elif stat.S_ISREG(st.st_mode):
        with source.open("rb") as reader, target.open("xb") as writer:
            while chunk := reader.read(1 << 20):
                writer.write(chunk)
    else:
        return
    if owners:
        os.lchown(target, st.st_uid, st.st_gid)
    if not stat.S_ISLNK(st.st_mode):
        os.chmod(target, st.st_mode & 0o7777)


def build_on_tmpfs(build: Path) -> None:
    """A tmpfs over *build*, holding what it held, with its owner and mode."""
    stage = Path(tempfile.mkdtemp(prefix="pynixd-build-")) / "build"
    copy(build, stage, owners=True)
    subprocess.run(["/sbin/mount_tmpfs", str(build)], check=True, capture_output=True)
    st = stage.lstat()
    os.chown(build, st.st_uid, st.st_gid)
    os.chmod(build, st.st_mode & 0o7777)
    for child in stage.iterdir():
        copy(child, build / child.name, owners=True)
    shutil.rmtree(stage.parent)


def main(job_path: Path) -> None:
    job = json.loads(job_path.read_text())
    top = Path(job["top"])
    build = Path(job["build"])
    try:
        build_on_tmpfs(build)
    except (OSError, subprocess.CalledProcessError) as e:
        detail = e.stderr.decode(errors="replace").strip() if isinstance(e, subprocess.CalledProcessError) else e
        (top / EXIT_NAME).write_text(f"init cannot put the build directory on a tmpfs: {detail}\n")
        return
    status = os.open(top / STATUS_NAME, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.dup2(status, 3)
    os.close(status)
    helper = subprocess.run(
        [sys.executable, "-I", "-m", "pynixd_store.builder.child", job["spec"], "3"], pass_fds=(3,), check=False
    )
    os.close(3)
    code = helper.returncode
    end = f"signal {-code}" if code < 0 else f"exit {code}"
    outputs = top / OUTPUTS_NAME
    try:
        if code != 0 and job["keep_failed"]:
            copy(build, top / KEPT_NAME)
        outputs.mkdir()
        for path in map(Path, job["outputs"]):
            if os.path.lexists(path):
                copy(path, outputs / path.name)
    except OSError as e:
        end = f"init cannot copy the outputs to the host: {e}"
    (top / EXIT_NAME).write_text(f"{end}\n")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
