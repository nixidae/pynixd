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
    "pynixd/http_server.py handle_put_narinfo": DEBT,
    "pynixd/metrics.py collect": DEBT,
    "pynixd/monitor.py sample": DEBT,
    "pynixd/reverse_client.py _load_host_keys": DEBT,
    "pynixd/sftp_server.py _resolve": DEBT,
    "pynixd/sftp_server.py stat": DEBT,
    "pynixd/store/__init__.py __getattr__": DEBT,
    "pynixd/store/gha_cache.py _upload": DEBT,
    "pynixd/store/local_db.py close": DEBT,
    "pynixd/store/local_host.py read_derivation": DEBT,
    "pynixd/store_url.py substituter_spec": DEBT,
    "pynixd/unix_socket.py open_unix_connection": DEBT,
    "pynixd_store/builder/local.py _builder_lines": DEBT,
    "pynixd_store/builder/local.py _network_files": DEBT,
    "pynixd_store/builder/local.py _run": DEBT,
    "pynixd_store/builder/local.py build": DEBT,
    "pynixd_store/builder/usage.py read_usage": DEBT,
    "pynixd_store/builder/users.py chown_to": DEBT,
    "pynixd_store/builder/vm.py argv": DEBT,
    "pynixd_store/builder/vm.py ending": DEBT,
    "pynixd_store/builder/vm.py make_disk": DEBT,
    "pynixd_store/builder/vm.py status": DEBT,
    "pynixd_store/builder/vm.py uplink": DEBT,
    "pynixd_store/collect.py serve": DEBT,
    "pynixd_store/gc.py pressure_usage": DEBT,
    "pynixd_store/local_store_db.py open": DEBT,
    "pynixd_store/local_store_db.py resolve_db_path": DEBT,
    "pynixd_store/overlay_engine.py remount_if_necessary": DEBT,
    "pynixd_store/signing.py from_file": DEBT,
}
