"""`/healthz` has to answer two questions a TCP probe cannot.

A kubelet `tcpSocket` probe calls `connect()`, and the kernel completes it from
the listen backlog whether or not the application ever accepts. So it passes
against a wedged process, and fails only when a stall outlasts its timeout --
which a busy process also does. nixidae/nixidae#432 and #53.

These test the verdict, not the endpoint: the report is what decides the status
code, and it is where the logic is.
"""

from __future__ import annotations

import time

import anyio

from pynixd.config import PynixdSettings
from pynixd.health import HealthReport, LoopLagMonitor
from pynixd.instance import Server


def _server_with_lag(lag: float, settings: PynixdSettings) -> Server:
    """A `Server` that started nothing, holding one recorded stall.

    `health()` reads the settings and the monitor, and neither needs a
    running loop. The sample is placed rather than measured: the rule under
    test is the comparison, not the sampling.
    """
    server = Server(settings=settings)
    server.loop_lag._samples.append((server.loop_lag._clock(), lag))  # noqa: SLF001
    server.loop_lag.lifetime_max = lag
    return server


def _stock() -> PynixdSettings:
    """Settings that serve nothing, so `health()` reports the loop alone."""
    return PynixdSettings(unix_path=None, ssh_port=None)


def test_a_report_names_the_check_that_failed() -> None:
    """A probe body that says only "unhealthy" moves the diagnosis elsewhere."""
    report = HealthReport(ok=False, checks={"ssh": "configured but not serving", "unix": "serving"})
    text = report.as_text()

    assert text.startswith("unhealthy")
    assert "ssh: configured but not serving" in text
    assert "unix: serving" in text


def test_a_healthy_report_still_lists_its_checks() -> None:
    """So an operator can see which interfaces were assessed, not just the verdict."""
    text = HealthReport(ok=True, checks={"ssh": "serving"}).as_text()

    assert text.startswith("ok")
    assert "ssh: serving" in text


async def test_the_monitor_records_a_stall_it_can_see() -> None:
    """A blocking sleep on the loop is what a transfer that never yields does.

    Gated on `started` and `wait_for_sample`, never on a duration. `start_soon`
    schedules a task and does not run one, so a test that sleeps to "let it
    start" is asserting on how fast this machine happens to be.
    """
    monitor = LoopLagMonitor(interval=0.01, window=30.0)

    async with anyio.create_task_group() as tg:
        tg.start_soon(monitor.run)
        await monitor.started.wait()
        # Block the loop. `time.sleep` is the point: nothing else can run,
        # which is the condition being measured.
        time.sleep(0.3)
        await monitor.wait_for_sample()
        monitor.stop()

    assert monitor.lifetime_max >= 0.2, f"missed a 300ms stall, saw {monitor.lifetime_max:.3f}s"


async def test_an_idle_loop_reports_almost_no_lag() -> None:
    """The negative control. Without it, a monitor that always reports a stall passes."""
    monitor = LoopLagMonitor(interval=0.01, window=30.0)

    async with anyio.create_task_group() as tg:
        tg.start_soon(monitor.run)
        await monitor.started.wait()
        for _ in range(5):
            await monitor.wait_for_sample()
        monitor.stop()

    assert monitor.lifetime_max < 0.1, f"idle loop reported {monitor.lifetime_max:.3f}s"


def test_the_window_forgets_an_old_stall() -> None:
    """A stall must not hold the process unhealthy once it is serving again.

    `lifetime_max` keeps it for the operator; `window_max` is what health asks,
    and it has to decay or one bad transfer restarts the pod forever.

    Time is moved, not waited for, so this asserts the decay rule rather than
    the machine's timing. No loop and no sampling: the samples are placed
    directly, because the rule under test is the pruning.
    """
    now = [1000.0]
    monitor = LoopLagMonitor(interval=0.01, window=10.0, clock=lambda: now[0])

    monitor._samples.append((now[0], 0.4))  # noqa: SLF001
    monitor.lifetime_max = 0.4
    assert monitor.window_max == 0.4

    now[0] += 9.0
    assert monitor.window_max == 0.4, "pruned a sample still inside the window"

    now[0] += 2.0
    assert monitor.window_max == 0.0, "the window kept a stall past its end"
    assert monitor.lifetime_max == 0.4, "lifetime_max must not decay"


class TestTheConfiguredThreshold:
    """`health_loop_lag_max` and `health_loop_lag_window`.

    The right value is a property of the workload -- a host pushing a large
    closure stalls longer than a laptop -- so both are settings rather than
    constants. A setting nothing reads looks exactly like one that works.
    """

    def test_a_stall_under_the_limit_is_healthy(self) -> None:
        assert _server_with_lag(1.0, _stock()).health().ok

    def test_a_lowered_limit_makes_the_same_stall_unhealthy(self) -> None:
        """The same 1 s stall, and the only difference is the setting."""
        lowered = PynixdSettings(unix_path=None, ssh_port=None, health_loop_lag_max=0.5)

        report = _server_with_lag(1.0, lowered).health()

        assert not report.ok
        assert "stalled" in report.checks["event_loop"]

    def test_a_raised_limit_tolerates_a_stall_the_default_refuses(self) -> None:
        """The direction an operator reaches for: a node whose transfers
        stall longer than a laptop's, told so rather than restarted."""
        raised = PynixdSettings(unix_path=None, ssh_port=None, health_loop_lag_max=30.0)

        assert not _server_with_lag(6.0, _stock()).health().ok
        assert _server_with_lag(6.0, raised).health().ok

    def test_the_window_setting_reaches_the_monitor(self) -> None:
        """The other half. A window nothing reads leaves every stall counted
        for 30 s whatever the configuration says."""
        settings = PynixdSettings(unix_path=None, ssh_port=None, health_loop_lag_window=7.5)

        server = Server(settings=settings)

        assert server.loop_lag._window == 7.5  # noqa: SLF001
