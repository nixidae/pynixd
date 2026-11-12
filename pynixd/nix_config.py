"""Typed Nix configuration rendering for managed daemon environments."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


BUILDER_FRONTEND_SPEC = (
    "unix://{unix_path} x86_64-linux,aarch64-linux,aarch64-darwin - 500 1 "
    "apple-virt,kvm,nixos-test,benchmark,big-parallel,ca-derivations,recursive-nix,uid-range"
)


class NixConfig(BaseModel):
    """Nix config options rendered as nix.conf-compatible text.

    All options are optional. Unset values are omitted from rendered output.
    """

    model_config = ConfigDict(
        alias_generator=lambda field_name: field_name.replace("_", "-"),
        populate_by_name=True,
    )

    abort_on_warn: bool | None = None
    accept_flake_config: bool | None = None
    allow_dirty: bool | None = None
    allow_dirty_locks: bool | None = None
    allow_import_from_derivation: bool | None = None
    allow_new_privileges: bool | None = None
    allow_symlinked_store: bool | None = None
    allow_unsafe_native_code_during_evaluation: bool | None = None
    always_allow_substitutes: bool | None = None
    auto_allocate_uids: bool | None = None
    auto_optimise_store: bool | None = None
    builders_use_substitutes: bool | None = None
    compress_build_log: bool | None = None
    eval_cache: bool | None = None
    fallback: bool | None = None
    filter_syscalls: bool | None = None
    fsync_metadata: bool | None = None
    fsync_store_paths: bool | None = None
    http2: bool | None = None
    ignore_try: bool | None = None
    impersonate_linux_26: bool | None = None
    keep_build_log: bool | None = None
    keep_derivations: bool | None = None
    keep_env_derivations: bool | None = None
    keep_failed: bool | None = None
    keep_going: bool | None = None
    keep_outputs: bool | None = None
    preallocate_contents: bool | None = None
    print_missing: bool | None = None
    pure_eval: bool | None = None
    require_drop_supplementary_groups: bool | None = None
    require_sigs: bool | None = None
    restrict_eval: bool | None = None
    run_diff_hook: bool | None = None
    sandbox_fallback: bool | None = None
    substitute: bool | None = None
    sync_before_registering: bool | None = None
    trace_function_calls: bool | None = None
    trace_import_from_derivation: bool | None = None
    trace_verbose: bool | None = None
    trust_tarballs_from_git_forges: bool | None = None
    use_case_hack: bool | None = None
    use_cgroups: bool | None = None
    use_registries: bool | None = None
    use_sqlite_wal: bool | None = None
    use_xdg_base_directories: bool | None = None
    warn_dirty: bool | None = None
    warn_short_path_literals: bool | None = None
    nix_shell_always_looks_for_shell_nix: bool | None = None
    nix_shell_shebang_arguments_relative_to_script: bool | None = None

    build_poll_interval: int | None = None
    connect_timeout: int | None = None
    cores: int | None = None
    download_attempts: int | None = None
    download_buffer_size: int | None = None
    download_speed: int | None = None
    eval_attrset_update_layer_rhs_threshold: int | None = None
    eval_profiler_frequency: int | None = None
    gc_reserved_space: int | None = None
    http_connections: int | None = None
    id_count: int | None = None
    log_lines: int | None = None
    max_build_log_size: int | None = None
    max_call_depth: int | None = None
    max_free: int | None = None
    max_jobs: int | None = None
    max_silent_time: int | None = None
    max_substitution_jobs: int | None = None
    min_free: int | None = None
    min_free_check_interval: int | None = None
    nar_buffer_size: int | None = None
    narinfo_cache_meta_ttl: int | None = None
    narinfo_cache_negative_ttl: int | None = None
    narinfo_cache_positive_ttl: int | None = None
    stalled_download_timeout: int | None = None
    start_id: int | None = None
    tarball_ttl: int | None = None
    timeout: int | None = None
    warn_large_path_threshold: int | None = None

    build_dir: str | None = None
    build_hook: str | None = None
    build_users_group: str | None = None
    diff_hook: str | None = None
    eval_profile_file: str | None = None
    eval_profiler: str | None = None
    eval_system: str | None = None
    external_builders: str | None = None
    """JSON text: a list of `{systems, program, args}` (`local-settings.hh:649`)."""
    flake_registry: str | None = None
    json_log_path: str | None = None
    lint_absolute_path_literals: str | None = None
    lint_short_path_literals: str | None = None
    lint_url_literals: str | None = None
    netrc_file: str | None = None
    post_build_hook: str | None = None
    pre_build_hook: str | None = None
    sandbox: str | None = None
    sandbox_build_dir: str | None = None
    sandbox_dev_shm_size: str | None = None
    ssl_cert_file: str | None = None
    store: str | None = None
    system: str | None = None
    upgrade_nix_store_path_url: str | None = None
    user_agent_suffix: str | None = None
    commit_lock_file_summary: str | None = None
    bash_prompt: str | None = None
    bash_prompt_prefix: str | None = None
    bash_prompt_suffix: str | None = None

    access_tokens: list[str] | None = None
    allowed_impure_host_deps: list[str] | None = None
    allowed_uris: list[str] | None = None
    allowed_users: list[str] | None = None
    builders: list[str] | None = None
    experimental_features: list[str] | None = None
    extra_platforms: list[str] | None = None
    hashed_mirrors: list[str] | None = None
    ignored_acls: list[str] | None = None
    impure_env: list[str] | None = None
    nix_path: list[str] | None = None
    plugin_files: list[str] | None = None
    sandbox_paths: list[str] | None = None
    secret_key_files: list[str] | None = None
    substituters: list[str] | None = None
    system_features: list[str] | None = None
    trusted_public_keys: list[str] | None = None
    trusted_substituters: list[str] | None = None
    trusted_users: list[str] | None = None

    _EXTRA_PREFIX_SETTINGS = frozenset(
        {
            "experimental-features",
            "substituters",
            "trusted-substituters",
        },
    )

    def _iter_set(self, *, use_extra_prefix: bool = False) -> Iterator[tuple[str, str]]:
        for name in type(self).model_fields:
            value = getattr(self, name)
            if value is None:
                continue
            if isinstance(value, bool):
                rendered = "true" if value else "false"
            elif isinstance(value, list):
                if not value:
                    continue
                rendered = " ".join(value)
            else:
                if value == "":
                    continue
                rendered = str(value)
            key = name.replace("_", "-")
            if use_extra_prefix and key in self._EXTRA_PREFIX_SETTINGS:
                key = f"extra-{key}"
            yield key, rendered

    def to_nix_conf(self) -> str:
        """Render options as a ``nix.conf``-compatible string (``key = value``)."""
        return "\n".join(f"{key} = {value}" for key, value in self._iter_set())

    def to_env(self) -> dict[str, str]:
        """Return as a ``NIX_CONFIG`` environment variable."""
        return {"NIX_CONFIG": self.to_nix_conf()}

    def to_nix_config_env(self) -> str:
        """Render with ``extra-`` prefix for settings like ``experimental-features``.

        This format is used when passing config to ``nix`` commands that
        should augment rather than replace the user's global config.
        """
        return "\n".join(f"{key} = {value}" for key, value in self._iter_set(use_extra_prefix=True))

    def to_extra_args(self) -> list[str]:
        """Render as ``--option key value`` pairs for CLI invocation."""
        args: list[str] = []
        for key, value in self._iter_set():
            args.extend(["--option", key, value])
        return args

    def to_daemon_args(self) -> list[str]:
        """Render only daemon-relevant options as ``--option`` pairs.

        Currently only ``require-sigs`` is forwarded to the daemon.
        """
        args: list[str] = []
        for key, value in self._iter_set():
            if key == "require-sigs":
                args.extend(["--option", key, value])
        return args

    def merge_builder_frontend(self, unix_path: Path) -> NixConfig:
        """Merge this config with the default builder frontend config.

        Sets ``max_jobs`` to 0 and configures ``builders`` to point at
        the local pynixd Unix socket.
        """
        return merge_builder_frontend(self, unix_path)


def builder_frontend_config(unix_path: Path) -> NixConfig:
    """Create a default builder-frontend config pointing at ``unix_path``.

    Sets ``max_jobs = 0`` (delegate all builds to pynixd) and configures
    a single builder entry referencing the pynixd Unix socket.
    """
    return NixConfig(
        max_jobs=0,
        builders=[BUILDER_FRONTEND_SPEC.format(unix_path=unix_path)],
    )


def merge_builder_frontend(user: NixConfig | None, unix_path: Path) -> NixConfig:
    frontend = builder_frontend_config(unix_path)
    if user is None:
        return frontend

    merged = user.model_copy(deep=True)
    merged.builders = frontend.builders
    if merged.max_jobs is None:
        merged.max_jobs = 0
    return merged
