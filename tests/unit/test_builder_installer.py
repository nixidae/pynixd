"""The installer's builder mode and its idle guard: nixidae/nixidae#1198.

Both are scripts in `nix/installer`, outside any package, so they load by path.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pynixd.config import PynixdSettings
from pynixd.provisioning.ticket import JoinTicket, seal

if TYPE_CHECKING:
    from types import ModuleType

_DIR = Path(__file__).parent.parent.parent / "nix" / "installer"


def _load(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(f"installer_{name}", _DIR / f"{name}.py")
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {name}.py")
    module = importlib.util.module_from_spec(spec)
    # `dataclass` reads the class's module from `sys.modules`.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


install = _load("install")
guard = _load("idle_guard")

BUILD_UID = 0x34000000


def test_a_builder_conf_parses_as_pynixd_settings_with_the_initiator_on() -> None:
    conf = install.pynixd_conf(False, {"join_ticket": "/etc/pynixd/join-ticket.json", "max_jobs": 4})
    settings = PynixdSettings.model_validate_json(json.dumps(conf))
    initiator = settings.reverse_initiator
    assert initiator.enabled
    assert initiator.join_ticket == Path("/etc/pynixd/join-ticket.json")
    assert initiator.max_jobs == 4


def test_a_conf_without_the_initiator_keeps_it_off() -> None:
    assert not PynixdSettings.model_validate_json(json.dumps(install.pynixd_conf(False))).reverse_initiator.enabled


def test_the_idle_timer_runs_the_guard_with_the_limit() -> None:
    units = install.idle_units(Path("/nix/store/x-guard/bin/pynixd-idle-guard"), 900.0)
    assert "ExecStart=/nix/store/x-guard/bin/pynixd-idle-guard 900\n" in units["pynixd-idle.service"]
    assert "OnUnitActiveSec=1min" in units["pynixd-idle.timer"]


def test_a_build_user_process_is_a_build() -> None:
    assert guard.building([0, 1000, BUILD_UID + 5])
    assert not guard.building([0, 1000, 65534])
    # The first id past the range belongs to no build.
    assert not guard.building([BUILD_UID + (128 << 16)])
    assert not guard.building([BUILD_UID - 1])


def test_quiet_counts_from_the_latest_of_boot_and_changes() -> None:
    assert guard.quiet_for(1000.0, 100.0, []) == 900.0
    assert guard.quiet_for(1000.0, 100.0, [400.0, 700.0]) == 300.0
    # A file older than the boot, from the image, does not make the machine look idle sooner.
    assert guard.quiet_for(1000.0, 100.0, [50.0]) == 900.0


def test_a_lease_sets_the_limit_with_the_margin() -> None:
    assert guard.limit(600.0, None) == 600.0
    assert guard.limit(600.0, '{"idle_seconds": 90.0, "boot_seconds": 90.0}') == 90.0 + guard.LEASE_MARGIN
    assert guard.limit(20.0, '{"idle_seconds": 3600}') == 3600 + guard.LEASE_MARGIN, "a lease may lengthen it too"


@pytest.mark.parametrize(
    "text", ["", "{", "[]", '{"boot_seconds": 1}', '{"idle_seconds": "soon"}', '{"idle_seconds": -1}']
)
def test_a_lease_the_guard_cannot_read_keeps_its_own_limit(text: str) -> None:
    assert guard.limit(600.0, text) == 600.0


def test_quiet_counts_from_the_lease(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(guard, "process_uids", list)
    monkeypatch.setattr(guard, "DB_FILES", ())
    monkeypatch.setattr(guard, "SEEN", tmp_path / "build-seen")
    monkeypatch.setattr(guard, "LEASE", tmp_path / "lease.json")
    since_boot = guard.quiet()
    (tmp_path / "lease.json").write_text('{"idle_seconds": 60}')
    assert guard.quiet() < 1.0 < since_boot, "the controller counts idle from registration, when the lease comes"


def test_the_installer_puts_the_lease_where_the_guard_reads_it() -> None:
    assert install.IDLE_LEASE == guard.LEASE
    conf = install.pynixd_conf(False, {"join_ticket": "/etc/pynixd/join-ticket.json"})
    assert PynixdSettings.model_validate_json(json.dumps(conf)).reverse_initiator.idle_lease == guard.LEASE
    mine = install.pynixd_conf(False, {"idle_lease": "/run/elsewhere.json"})
    assert PynixdSettings.model_validate_json(json.dumps(mine)).reverse_initiator.idle_lease == Path(
        "/run/elsewhere.json"
    )


def test_process_uids_reads_proc(tmp_path: Path) -> None:
    (tmp_path / "1").mkdir()
    (tmp_path / "self").mkdir()
    assert guard.process_uids(tmp_path) == [(tmp_path / "1").stat().st_uid]


KEY_TEXT = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
"""32 bytes, base64: `ticket_key`'s form."""


def _sealed_env(ticket: JoinTicket, context: str = '{"pool":"gha"}') -> dict[str, str]:
    key = bytes(range(32))
    return {
        install.TICKET_ENV: seal(ticket.model_dump_json(), key, context),
        install.TICKET_KEY_ENV: KEY_TEXT,
        install.TICKET_CONTEXT_ENV: context,
    }


def test_a_sealed_ticket_opens_with_its_key_and_context() -> None:
    ticket = JoinTicket(token="t0ken", system_features=["gha"], max_jobs=2)
    assert install.open_sealed_ticket(_sealed_env(ticket)) == ticket


def test_a_sealed_ticket_with_another_context_stops_the_install() -> None:
    env = _sealed_env(JoinTicket(token="t0ken"))
    env[install.TICKET_CONTEXT_ENV] = '{"pool":"other"}'
    with pytest.raises(SystemExit, match="does not open"):
        install.open_sealed_ticket(env)


def test_a_missing_variable_is_named() -> None:
    env = _sealed_env(JoinTicket(token="t0ken"))
    del env[install.TICKET_KEY_ENV]
    with pytest.raises(SystemExit, match=install.TICKET_KEY_ENV):
        install.open_sealed_ticket(env)


def test_a_ticket_gives_the_builder_its_features_and_slots() -> None:
    initiator, extra = install.builder_settings(JoinTicket(token="t", system_features=["gha", "kvm"], max_jobs=3))
    conf = install.pynixd_conf(False, initiator)
    settings = PynixdSettings.model_validate_json(json.dumps(conf)).reverse_initiator
    assert (settings.join_ticket, settings.max_jobs) == (install.JOIN_TICKET, 3)
    assert extra == "extra-system-features = gha kvm\n"
    assert install.builder_settings(JoinTicket(token="t"))[1] == ""
