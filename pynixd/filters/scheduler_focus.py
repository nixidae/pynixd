"""Drop noisy debug/trace logs for scheduler-focused benchmarking.

Keeps: build lifecycle, scheduler, decomposition, resolution events,
errors, warnings, and all non-debug level messages.
Drops: SSH channel lifecycle, per-operation trace, SQL queries.
"""

from typing import Any

KEEP_EVENTS = frozenset(
    {
        "build_enqueued",
        "build_derivation_enqueued",
        "build_completed",
        "build_failed",
        "build_fasttracked_local",
        "build_assigned_to_store",
        "build_sending_inputs",
        "build_executing",
        "build_executed",
        "build_observed",
        "pulling_paths",
        "pulled_paths_into_local_store",
        "scheduler_started",
        "scheduler_request_resolved",
        "scheduler_request_resolved_with_failure",
        "store_added_to_scheduler",
        "store_probed",
        "store_reconnected",
        "systems_probed",
        "features_probed",
        "resolved_derivation",
        "registering_dep_realisation_on_builder",
        "register_dep_realisation_failed",
        "trampoline_detected",
        "trampoline_build_enqueued",
        "trampoline_drv_not_found",
        "dynamic_dep_linked",
        "unix_client_connected",
        "client_protocol_negotiated",
        "client_handshake_complete",
        "local_store_db_connected",
        "local_store_db_disabled",
        "no_servers_started",
        "reverse_server_listening",
        "reverse_builder_registration",
        "reverse_store_added",
        "reverse_builder_disconnected",
        "http_cache_narinfo_upstream_hit",
        "http_cache_nar_redirect",
    },
)

KEEP_LOGGERS = frozenset(
    {
        "pynixd.scheduler",
        "pynixd.trampoline",
        "pynixd.build_queue",
        "pynixd.proxy",
        "pynixd.unix_server",
        "pynixd.store.base",
        "pynixd.instance",
        "pynixd.reverse_server",
        "pynixd.reverse_client",
        "pynixd.serde.probe_systems",
        "pynixd.serde.probe_features",
        "pynixd.serde.build_paths",
        # Collection runs rarely and its lines are the operator's whole
        # window into a pass: the plan counts, the weight ranking, the
        # delete report. Filtering them blinds `pynixd gc`, which is how a
        # dry-run went green with an empty plan and nothing said why.
        "pynixd.gc",
    },
)

KEEP_LOGGERS_DEBUG = frozenset(
    {
        "pynixd.scheduler",
        "pynixd.build_queue",
    },
)

DROP_LOGGERS = frozenset({"pynixd.serde.AddMultipleToStoreRequest"})

ALWAYS_KEEP_LEVELS = frozenset({"error", "critical"})


def filter(logger: object, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any] | None:  # noqa: A001 — plugin convention
    level = event_dict.get("level", "").lower()
    logger_name = getattr(logger, "name", "")
    if level in ALWAYS_KEEP_LEVELS or method_name in ALWAYS_KEEP_LEVELS:
        return event_dict

    if logger_name in DROP_LOGGERS:
        return None

    if level not in ("debug", "info", "trace"):
        return event_dict

    if event_dict.get("event") in KEEP_EVENTS:
        return event_dict

    if logger_name in KEEP_LOGGERS_DEBUG:
        return event_dict
    if logger_name in KEEP_LOGGERS and method_name != "debug":
        return event_dict

    return None
