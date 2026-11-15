"""The darwin build VM's side in the guest: run the helper, then hand the outputs to the host (`darwin_vm`).

    python -I -m pynixd_store.builder.darwin_vm_guest TOP/darwin-vm.json

The helper's file descriptor 3 is `top/vm-status`, as in the Linux build VM. Its output is this process's, which
the guest's agent sends to the host. The outputs are on the tmpfs over the store, which the host cannot see: each
one is copied into `top/vm-out`, a writable share. Not with macOS `cp -p`: its copyfile gave a 0644 file 0755 on
the share (measured).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from .darwin_vm import OUTPUTS_NAME
from .vm import EXIT_NAME, STATUS_NAME


def copy(source: Path, target: Path) -> None:
    """*source* to *target*: contents, the mode's permission bits and symbolic links, nothing else."""
    if source.is_symlink():
        os.symlink(os.readlink(source), target)
        return
    mode = source.lstat().st_mode & 0o7777
    if source.is_dir():
        target.mkdir()
        for child in sorted(source.iterdir()):
            copy(child, target / child.name)
    else:
        with source.open("rb") as reader, target.open("xb") as writer:
            while chunk := reader.read(1 << 20):
                writer.write(chunk)
    os.chmod(target, mode)


def main(job_path: Path) -> None:
    job = json.loads(job_path.read_text())
    top = Path(job["top"])
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
        outputs.mkdir()
        for path in map(Path, job["outputs"]):
            if os.path.lexists(path):
                copy(path, outputs / path.name)
    except OSError as e:
        end = f"init cannot copy the outputs to the host: {e}"
    (top / EXIT_NAME).write_text(f"{end}\n")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
