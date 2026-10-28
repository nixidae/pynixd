"""The capability probe keeps its fan-out under a default `sshd`.

Each probe is a build, and over SSH each build takes a channel. `sshd`
permits ten sessions by default. The probe used to start every one of them
at once: three systems, then one build for each supported system by each
known feature. Two systems by nine features is eighteen, and eighteen at
once failed eight -- eighteen minus ten -- with
`OPEN_REQUEST_SESSION_FAILED`. Issue #15.

The probes here never reach a socket. They count themselves instead, which
is what the bound is about; whether a build answers is a different question
and the functional suite asks it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
import pytest

from pynixd.store.daemon import PROBE_CONCURRENCY, DaemonStore
from pynixd.system_features import KNOWN_FEATURES

if TYPE_CHECKING:
    from collections.abc import Callable

SSHD_DEFAULT_MAX_SESSIONS = 10
"""`MaxSessions` in `sshd_config(5)`. A client cannot read the server's
value, so the constant under test is measured against the documented one."""


class CountingProbe:
    """Stands in for `DaemonStore`, holding only what the two fan-outs read.

    The real store needs a `StoreSpecBase`, a pool and a connection. The
    methods under test use `_send_probe`, `store_id` and `_feature_matrix`
    and nothing else, so they are called unbound against this.
    """

    def __init__(self) -> None:
        self.store_id = "counting"
        self._feature_matrix: dict[str, set[str]] | None = None
        self.in_flight = 0
        self.peak = 0
        self.total = 0

    async def _send_probe(self, name: str, *_args: object, **_kwargs: object) -> tuple[str, bool, str]:
        self.in_flight += 1
        self.peak = max(self.peak, self.in_flight)
        self.total += 1
        # A checkpoint, so every started task reaches this counter before any
        # of them leaves it. Without one the peak reads 1 whatever the bound.
        await anyio.sleep(0)
        self.in_flight -= 1
        return name, True, ""


async def _run(
    fan_out: Callable[..., object],
    *args: object,
    limit: int,
) -> CountingProbe:
    probe = CountingProbe()
    limiter = anyio.CapacityLimiter(limit)
    await fan_out(probe, *args, limiter)  # type: ignore[misc]  # unbound, on a stand-in
    return probe


SYSTEMS = frozenset({"x86_64-linux", "aarch64-linux"})
"""The two the issue was measured on. `_probe_features` unions whatever it
is handed with `KNOWN_FEATURES`, so the cases below hand it nothing and
take that set, which keeps the count honest at len(SYSTEMS) * 9 = 18."""


class TestTheBound:
    @pytest.mark.anyio
    async def test_the_feature_fan_out_stays_under_the_limiter(self) -> None:
        probe = await _run(DaemonStore._probe_features, set(SYSTEMS), set(), limit=PROBE_CONCURRENCY)

        assert probe.total == len(SYSTEMS) * len(KNOWN_FEATURES)
        assert probe.total > SSHD_DEFAULT_MAX_SESSIONS, (
            f"this case has to ask for more channels than sshd allows, or it proves nothing; it asked for {probe.total}"
        )
        assert probe.peak <= PROBE_CONCURRENCY

    @pytest.mark.anyio
    async def test_the_system_fan_out_stays_under_the_limiter(self) -> None:
        systems = {f"s{n}" for n in range(12)}

        probe = await _run(DaemonStore._probe_systems, systems, limit=PROBE_CONCURRENCY)

        assert probe.total == 12
        assert probe.peak <= PROBE_CONCURRENCY

    @pytest.mark.anyio
    async def test_the_limiter_is_what_holds_it(self) -> None:
        """The negative control: a limiter wide enough is a limiter that does
        nothing, and then the peak is the whole fan-out. Without this the two
        cases above pass against a counter that never rises."""
        probe = await _run(DaemonStore._probe_features, set(SYSTEMS), set(), limit=1000)

        assert probe.peak == len(SYSTEMS) * len(KNOWN_FEATURES)


class TestTheConstant:
    def test_it_leaves_room_for_a_build_beside_a_probe(self) -> None:
        # `nix` assumes the same ten for its own channels, so a probe that
        # took all of them would fail a build running next to it.
        assert PROBE_CONCURRENCY <= SSHD_DEFAULT_MAX_SESSIONS // 2

    def test_it_is_more_than_one(self) -> None:
        assert PROBE_CONCURRENCY > 1
