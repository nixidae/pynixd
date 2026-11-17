"""What the provisioner launches and terminates, rule by rule. nixidae/nixidae#1198."""

from __future__ import annotations

from pynixd.provisioning.decide import Decision, EndReason, Held, InstanceState, Pool, Want, attribute, decide

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
