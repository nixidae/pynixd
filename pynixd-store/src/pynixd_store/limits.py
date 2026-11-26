"""Process resource limits, as every Nix process sets them at start."""

from __future__ import annotations

import contextlib
import resource


def bump_file_limit() -> None:
    """`bumpFileLimit` (`libmain/shared.cc:130`): the open files soft limit up to the hard one, best effort.

    Every Nix process does it at start, so a build under nix-daemon inherits it. A container's soft limit is
    containerd's 1024 (measured, a build pod), and systemd's default soft limit is 1024 too: the dynhetz daemon ran
    out at that limit under an sg1 deploy (`OSError: [Errno 24] Too many open files`, 2026-11-26).

    Linux only: on darwin Nix raises it to `kern.maxfilesperproc` instead, as a hard limit of infinity breaks
    programs that size buffers by it (`shared.cc:139`).
    """
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    # Not `soft < hard`: RLIM_INFINITY is -1 here. The soft limit never exceeds the hard one.
    if soft != hard:
        with contextlib.suppress(ValueError, OSError):  # best effort, as Nix ignores setrlimit's error
            resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
