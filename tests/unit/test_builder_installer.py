"""The installer's builder mode and its idle guard: nixidae/nixidae#1198.

Both are scripts in `nix/installer`, outside any package, so they load by path.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from pynixd.config import PynixdSettings

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


def test_process_uids_reads_proc(tmp_path: Path) -> None:
    (tmp_path / "1").mkdir()
    (tmp_path / "self").mkdir()
    assert guard.process_uids(tmp_path) == [(tmp_path / "1").stat().st_uid]
