"""pynixd reports the version of the daemon behind it, not its own name.

`store-info.sh:69` of the Nix functional suite runs `nix store info` and
greps the output for `Version: <the daemon's version>`, taking that version
from `nix daemon --version`. pynixd answered `pynixd-0.1.0`, and that was
the **one** test of the 207 that failed against pynixd and passed against
`nix-daemon`.

The field is not a name. `isDaemonNewer` in `common/functions.sh:138-142`
feeds it to `builtins.compareVersions`, and so does any client deciding what
the daemon supports. A name there answers rubbish and nothing says so.
"""

from __future__ import annotations

from typing import Any, cast

from pynixd.proxy import NIX_VERSION, DaemonProxy


class FakeStore:
    def __init__(self, nix_version: str) -> None:
        self.nix_version = nix_version


def _version(nix_version: str) -> str:
    proxy = cast("Any", object.__new__(DaemonProxy))
    proxy.ctx = cast("Any", type("Ctx", (), {"local_store": FakeStore(nix_version)})())
    return DaemonProxy._version_for_the_client(proxy)  # noqa: SLF001 -- the answer is the unit under test


class TestTheVersionItReports:
    def test_it_is_the_upstream_daemons(self) -> None:
        assert _version("2.34.8") == "2.34.8"

    def test_it_falls_back_to_its_own_name_with_no_upstream(self) -> None:
        """A store that never reached a daemon has an empty version, and an
        empty string in this field is worse than a name: a client reads it as
        a daemon that answered nothing."""
        assert _version("") == NIX_VERSION

    def test_the_fallback_is_never_empty(self) -> None:
        assert NIX_VERSION


class TestWhatTheSuiteChecks:
    def test_the_answer_compares_as_a_version(self) -> None:
        """`isDaemonNewer` runs `builtins.compareVersions` on this. The old
        answer is the negative control: it has no numeric component that a
        comparison against `2.7.0pre20220126` can use."""
        reported = _version("2.34.8")

        assert all(part.isdigit() for part in reported.split("."))
        assert not all(part.isdigit() for part in NIX_VERSION.split("."))
