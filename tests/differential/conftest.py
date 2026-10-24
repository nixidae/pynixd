"""Gates and fixtures for the differential suite.

Three things have to be true before a differential test can say anything, and
each one is a skip rather than a failure:

1. The host is Linux. A chroot store cannot *build* anywhere else -- Nix says
   `building using a diverted store is not supported on this platform`. The
   whole design of this suite rests on two separated stores, so on darwin
   there is nothing to run. Use `vzrun` there.
2. huggorm is installed. It is the oracle, and it is a dependency of this
   suite alone. `pynixd` itself does not depend on it and must not: the
   shipped proxy is pure Python and links no C++.
3. The case's experimental features are enabled in the linked Nix.
"""

from __future__ import annotations

import dataclasses
import importlib.util
import shutil
import sys
from typing import TYPE_CHECKING

import pytest

from tests._conftest.constants import STORE_PREFIX
from tests._conftest.helpers import rmtree_robust

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

# The root every differential store lives under. Separate from the rest of the
# suite's prefix, so a teardown here cannot remove a store the session server
# is using.
#
# Short on purpose. `LocalDaemon` puts its socket at
# `<store>/nix/var/nix/daemon-socket/pynixd-nix`, and a Unix socket path cannot
# exceed 108 bytes. `_SOCKET_BUDGET` below states that, and the fixture checks
# it, because the failure it produces otherwise says nothing: the daemon starts,
# the socket file never appears, `ensure_daemon` probes for five seconds and
# then raises about a socket "not accepting connections".
DIFFERENTIAL_PREFIX = STORE_PREFIX.parent / "pynixd-diff"

# The suffix `LocalDaemon` appends to the store path, and the limit it must fit
# in. `sun_path` is `char[108]` on Linux and `char[104]` on darwin, and the
# smaller one is used here so a path that passes on one host passes on both.
_DAEMON_SOCKET_SUFFIX = "nix/var/nix/daemon-socket/pynixd-nix"
_SUN_PATH_LIMIT = 104

_HUGGORM_PRESENT = importlib.util.find_spec("huggorm") is not None


def _mixed_trees() -> str | None:
    """Whether `pynixd` and `nix_daemon_protocol` come from different trees.

    A skip and a loud reason, like the other gates here: the suite genuinely
    cannot answer its question in that state, and a run that answers it with a
    stale wire package is worse than no run.

    `pynixd/` sits at the root of its checkout, so a
    `python -m pytest` from there imports it off the working copy through the
    current directory. `nix_daemon_protocol` sits two directories further down,
    at `nix-daemon-protocol/src/`, which is on no path by default -- so it
    resolves to whatever is installed in the environment instead.

    A run in that state tests new pynixd against an old wire package. It cost
    a full cycle to find, and it announced itself as
    `'BuildResult' object has no attribute 'for_the_wire'` in every test at
    once. Put `nix-daemon-protocol/src` on `PYTHONPATH`.
    """
    import nix_daemon_protocol
    import pynixd

    pynixd_installed = "site-packages" in (pynixd.__file__ or "")
    protocol_installed = "site-packages" in (nix_daemon_protocol.__file__ or "")
    if pynixd_installed == protocol_installed:
        return None
    return (
        "pynixd and nix_daemon_protocol come from different trees, so this run would "
        "test one against a stale copy of the other.\n"
        f"  pynixd             {pynixd.__file__}\n"
        f"  nix_daemon_protocol {nix_daemon_protocol.__file__}\n"
        "Put the checkout's `nix-daemon-protocol/src` on PYTHONPATH."
    )


def _unmet_requirement() -> str | None:
    """Why this host cannot answer the question, or `None` when it can."""
    if sys.platform != "linux":
        return (
            "a chroot store cannot build off Linux "
            "(`building using a diverted store is not supported on this platform`); "
            "run this suite under vzrun"
        )
    if not _HUGGORM_PRESENT:
        return (
            "huggorm is the oracle of this suite and is not installed; "
            "it is a test dependency only, and pynixd itself does not depend on it"
        )
    if shutil.which("nix-instantiate") is None:
        return "nix-instantiate is not on PATH, and both arms instantiate with it"
    return None


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark every test in this directory, and skip the lot on a host that cannot run it.

    The skip is added here, and not raised from a fixture, because a fixture is
    too late. `pynixd_server` in the conftest above is session-scoped and
    autouse, so pytest sets it up before any fixture of this directory, and on
    darwin it fails rather than skips -- it wants a chroot-store daemon, which
    is the very thing this platform cannot do. A test that carries a skip
    marker never reaches fixture setup at all, so the outer fixture is never
    built.
    """
    reason = _unmet_requirement() or _mixed_trees()
    for item in items:
        if "differential" not in str(item.path.parent):
            continue
        item.add_marker(pytest.mark.differential)
        if reason is not None:
            item.add_marker(pytest.mark.skip(reason=reason))


@dataclasses.dataclass(frozen=True, slots=True)
class DifferentialRoots:
    """The store roots of one case.

    `pynixd` and `nix` are the two arms that every test compares. `builder` is
    the second store of the fleet test, and the single-store test leaves it
    empty.
    """

    pynixd: Path
    builder: Path
    nix: Path
    client: Path
    """Where a real `nix` client copies to, when a test asks pynixd over the
    wire rather than reading the pynixd store off disk. Empty otherwise."""


@pytest.fixture
async def differential_roots(request: pytest.FixtureRequest) -> AsyncGenerator[DifferentialRoots]:
    """Empty store roots for one case.

    Named after the case, so a failed run leaves directories a person can open
    and compare by hand.

    The name is the parameter id and not `request.node.name`. The node name
    carries the function name too, which is 38 characters here, and the socket
    that `LocalDaemon` puts under a root then passes 108 bytes and cannot be
    bound. That was measured, not guessed: the first run of this suite produced
    a 114-byte path and `AF_UNIX path too long`.
    """
    callspec = getattr(request.node, "callspec", None)
    stem = callspec.id if callspec is not None else request.node.name
    stem = stem.replace("/", "_").replace("[", "-").replace("]", "")
    roots = DifferentialRoots(
        pynixd=DIFFERENTIAL_PREFIX / stem / "pynixd",
        builder=DIFFERENTIAL_PREFIX / stem / "builder",
        nix=DIFFERENTIAL_PREFIX / stem / "nix",
        client=DIFFERENTIAL_PREFIX / stem / "client",
    )

    # Both pynixd-side roots host a daemon, so both have to fit.
    for root in (roots.pynixd, roots.builder):
        socket_path = root / _DAEMON_SOCKET_SUFFIX
        if len(str(socket_path)) > _SUN_PATH_LIMIT:
            pytest.fail(
                f"the daemon socket of this case would be {len(str(socket_path))} bytes, "
                f"and a Unix socket path holds {_SUN_PATH_LIMIT}. Shorten the case name "
                f"or `DIFFERENTIAL_PREFIX`.\n  {socket_path}"
            )

    for root in (roots.pynixd, roots.builder, roots.nix, roots.client):
        rmtree_robust(root)
        root.mkdir(parents=True, exist_ok=True)
    yield roots
