"""Handler package — importing all modules registers them in HANDLER_REGISTRY.

Private op allocation (Nix owns 1-50 and 1000+; keep out of both):
101 collect_garbage, 103 query_path_infos, 104 query_closure,
105 query_closure_with_info, 106 query_derivation_output_map_batch,
107 sign_path_info, 108 probe_systems, 109 probe_features, 110 roots_report,
111 state, 112 host_health, 113 host_health stream. Next free: 100, 102, 114+.
"""

from . import (
    add_build_log,  # noqa: F401
    add_indirect_root,  # noqa: F401
    add_multiple_to_store,  # noqa: F401
    add_perm_root,  # noqa: F401
    add_temp_root,  # noqa: F401
    add_temp_roots,  # noqa: F401
    add_to_store,  # noqa: F401
    add_to_store_nar,  # noqa: F401
    build_derivation,  # noqa: F401
    collect_garbage,  # noqa: F401
    find_roots,  # noqa: F401
    nar_from_path,  # noqa: F401
    optimise_store,  # noqa: F401
    pynixd_collect_garbage,  # noqa: F401
    pynixd_host_health,  # noqa: F401
    pynixd_roots_report,  # noqa: F401
    pynixd_state,  # noqa: F401
    set_options,  # noqa: F401
    sign_path_info,  # noqa: F401
    verify_store,  # noqa: F401
)
