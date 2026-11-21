"""What the provisioner launches and terminates, rule by rule. nixidae/nixidae#1198."""

from __future__ import annotations

import pytest

from pynixd.config import ProvisioningPoolSettings
from pynixd.provisioning.decide import Decision, EndReason, Held, InstanceState, Pool, Want, attribute, decide
from pynixd.provisioning.provisioner import pool_of

ARM = Pool(
    name="arm", systems=frozenset({"aarch64-linux"}), max_instances=3, slots=2, idle_seconds=60, launch_timeout=100
)
KVM = Pool(name="kvm", systems=frozenset({"x86_64-linux"}), features=frozenset({"kvm"}), max_instances=2)
ARM_BUILD = Want("aarch64-linux")


def _running(id_: str, builds: int = 0, idle_since: float | None = 0.0, notice: float | None = None) -> Held:
    return Held(
        id=id_,
        pool="arm",
        state=InstanceState.RUNNING,
        launched_at=0,
        builds=builds,
        idle_since=idle_since,
        notice=notice,
    )


def test_a_build_counts_against_the_first_pool_that_builds_it() -> None:
    wide = Pool(name="wide", systems=frozenset({"x86_64-linux"}), features=frozenset({"kvm", "big"}))
    counts = attribute(
        [KVM, wide],
        [Want("x86_64-linux", frozenset({"kvm"})), Want("x86_64-linux", frozenset({"big"})), Want("riscv64-linux")],
    )
    assert counts == {"kvm": 1, "wide": 1}


def test_waiting_builds_launch_one_instance_per_slots_worth() -> None:
    assert decide([ARM], [ARM_BUILD] * 3, [], now=0) == Decision(launch={"arm": 2})


def test_a_pool_launches_no_more_than_its_maximum() -> None:
    assert decide([ARM], [ARM_BUILD] * 20, [], now=0) == Decision(launch={"arm": 3})


def test_a_pending_instance_and_a_free_slot_count_as_supply() -> None:
    pending = Held(id="i-1", pool="arm", state=InstanceState.PENDING, launched_at=0)
    half = _running("i-2", builds=1, idle_since=None)
    # 4 waiting: the pending instance takes 2, the half-busy one 1, so one more instance.
    assert decide([ARM], [ARM_BUILD] * 4, [pending, half], now=10) == Decision(launch={"arm": 1})


def test_an_instance_under_notice_gives_no_slot() -> None:
    leaving = _running("i-1", notice=50.0)
    assert decide([ARM], [ARM_BUILD], [leaving], now=10) == Decision(launch={"arm": 1})


def test_an_idle_instance_ends_after_idle_seconds_only_when_nothing_waits() -> None:
    idle = _running("i-1", idle_since=0.0)
    assert decide([ARM], [], [idle], now=59) == Decision()
    assert decide([ARM], [], [idle], now=60) == Decision(terminate={"i-1": EndReason.IDLE})
    assert decide([ARM], [ARM_BUILD], [idle], now=600).terminate == {}


def test_the_idle_limit_is_the_boot_time_within_the_pools_bounds() -> None:
    bounded = Pool(name="arm", systems=frozenset({"aarch64-linux"}), idle_seconds=60, idle_max_seconds=200)
    assert [bounded.idle_limit(boot) for boot in (None, 10.0, 90.0, 500.0)] == [60, 60, 90.0, 200]
    assert ARM.idle_limit(500.0) == 500.0, "no maximum: the boot time, which launch_timeout bounds"


def test_an_instance_that_booted_long_idles_as_long() -> None:
    slow = Held(id="i-1", pool="arm", state=InstanceState.RUNNING, launched_at=0, idle_since=0.0, boot_seconds=90.0)
    assert decide([ARM], [], [slow], now=89) == Decision()
    assert decide([ARM], [], [slow], now=90) == Decision(terminate={"i-1": EndReason.IDLE})


def test_a_busy_instance_is_never_ended_as_idle() -> None:
    assert decide([ARM], [], [_running("i-1", builds=1, idle_since=None)], now=10_000) == Decision()


def test_the_minimum_stays_up_and_is_filled() -> None:
    floor = Pool(name="arm", systems=frozenset({"aarch64-linux"}), min_instances=1, max_instances=3, idle_seconds=60)
    assert decide([floor], [], [], now=0) == Decision(launch={"arm": 1})
    assert decide([floor], [], [_running("i-1"), _running("i-2", idle_since=5.0)], now=1000) == Decision(
        terminate={"i-1": EndReason.IDLE}
    )


def test_an_instance_stuck_pending_is_ended_and_replaced() -> None:
    stuck = Held(id="i-1", pool="arm", state=InstanceState.PENDING, launched_at=0)
    assert decide([ARM], [ARM_BUILD], [stuck], now=101) == Decision(
        launch={"arm": 1}, terminate={"i-1": EndReason.LAUNCH_TIMEOUT}
    )


GATED = Pool(
    name="gated",
    systems=frozenset({"x86_64-linux"}),
    features=frozenset({"aws"}),
    mandatory_features=frozenset({"aws"}),
)


def test_a_pool_with_mandatory_features_counts_only_builds_that_need_them() -> None:
    assert attribute([GATED], [Want("x86_64-linux"), Want("x86_64-linux", frozenset({"aws"}))]) == {"gated": 1}


def test_a_build_a_busy_store_holds_counts_only_after_the_pools_wait() -> None:
    patient = Pool(name="patient", systems=frozenset({"x86_64-linux"}), pressure_wait=60.0)
    assert attribute([patient], [Want("x86_64-linux", held_for=59.0)]) == {}
    assert attribute([patient], [Want("x86_64-linux", held_for=60.0)]) == {"patient": 1}
    assert attribute([patient], [Want("x86_64-linux")]) == {"patient": 1}, "a build no store can take counts at once"


SPOT = Pool(
    name="spot",
    systems=frozenset({"aarch64-linux"}),
    features=frozenset({"aws-spot"}),
    mandatory_features=frozenset({"aws-spot"}),
)


def test_a_build_that_names_the_pool_needs_not_its_mandatory_features() -> None:
    """A client's `builders` named the pool: that asks for it. Unnamed, the build must require `aws-spot` (#1425)."""
    assert not SPOT.builds(Want("aarch64-linux"))
    assert SPOT.builds(Want("aarch64-linux", frozenset({"aws-spot"})))
    assert SPOT.builds(Want("aarch64-linux", pools=frozenset({"spot"})))


def test_a_build_that_names_another_pool_never_comes_here() -> None:
    assert not SPOT.builds(Want("aarch64-linux", frozenset({"aws-spot"}), pools=frozenset({"other"})))
    assert not ARM.builds(Want("aarch64-linux", pools=frozenset({"spot"})))


def test_naming_a_pool_waives_no_system_and_no_feature() -> None:
    assert not SPOT.builds(Want("x86_64-linux", pools=frozenset({"spot"})))
    assert not SPOT.builds(Want("aarch64-linux", frozenset({"kvm"}), pools=frozenset({"spot"})))


ONCE = Pool(name="once", systems=frozenset({"x86_64-linux"}), max_instances=2, single_use=True, idle_seconds=60)
ONCE_BUILD = Want("x86_64-linux")


def _once(id_: str, builds: int, used: bool, idle_since: float | None = None) -> Held:
    return Held(
        id=id_,
        pool="once",
        state=InstanceState.RUNNING,
        launched_at=0,
        builds=builds,
        idle_since=idle_since,
        used=used,
    )


def test_a_used_instance_ends_as_its_build_ends_while_builds_wait() -> None:
    # A waiting build skips the idle rule; a used instance must end anyway, or it holds the room for ever.
    done = _once("i-1", builds=0, used=True, idle_since=10)
    assert decide([ONCE], [ONCE_BUILD], [done], now=11) == Decision(
        launch={"once": 1}, terminate={"i-1": EndReason.USED}
    )


def test_a_used_instance_that_builds_gives_no_slot_and_keeps_its_room() -> None:
    building = _once("i-1", builds=1, used=True)
    # One instance's room is left, so the three waiting builds get one launch, not two.
    assert decide([ONCE], [ONCE_BUILD] * 3, [building], now=5) == Decision(launch={"once": 1})


def test_an_idle_unused_single_use_instance_takes_a_waiting_build() -> None:
    fresh = _once("i-1", builds=0, used=False, idle_since=0)
    assert decide([ONCE], [ONCE_BUILD], [fresh], now=5) == Decision()


def test_a_single_use_pool_has_one_slot() -> None:
    pool = {"name": "once", "template": "t", "systems": ["x86_64-linux"], "single_use": True}
    assert pool_of(ProvisioningPoolSettings.model_validate(pool)).single_use
    with pytest.raises(ValueError, match="a single_use pool has one slot, not 2"):
        ProvisioningPoolSettings.model_validate({**pool, "slots": 2})


def test_used_means_nothing_without_single_use() -> None:
    # `used` comes from the store's drain mark; a notice drain on another pool must not end it early.
    drained = Held(id="i-1", pool="arm", state=InstanceState.RUNNING, launched_at=0, idle_since=0, used=True)
    assert decide([ARM], [ARM_BUILD], [drained], now=5) == Decision()
