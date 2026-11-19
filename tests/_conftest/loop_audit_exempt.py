"""The sites `loop_audit`'s gate lets through: `path function` to the reason.

Each entry is a debt, not a decision. Moving the call off the loop removes its entry.
"""

from __future__ import annotations

from typing import Final

DEBT: Final = "not moved off the loop yet (nixidae/nixidae#1547)"

EXEMPT: Final[dict[str, str]] = {
    "nix_daemon_protocol/experimental_compiled.py compile_codec": DEBT,
    "nix_daemon_protocol/wire_message.py _compiled_or_none": DEBT,
    "pynixd/agent.py launch": DEBT,
    "pynixd/store/__init__.py __getattr__": DEBT,
    "pynixd/store/local_db.py close": DEBT,
    "pynixd_store/builder/usage.py read_usage": DEBT,
    "pynixd_store/builder/vm.py argv": DEBT,
    "pynixd_store/builder/vm.py ending": DEBT,
    "pynixd_store/builder/vm.py make_disk": DEBT,
    "pynixd_store/builder/vm.py status": DEBT,
    "pynixd_store/builder/vm.py uplink": DEBT,
    "pynixd_store/collect.py serve": DEBT,
    "pynixd_store/overlay_engine.py remount_if_necessary": DEBT,
}
