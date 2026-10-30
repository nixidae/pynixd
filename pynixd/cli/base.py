"""Shared CLI helpers for pynixd subcommands."""

from __future__ import annotations

import contextlib
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import anyio
import structlog

from nix_daemon_protocol.ids import StoreId

from ..config import LocalSocketStoreSpec, PynixdSettings
from ..plugins import import_plugin
from ..store import LocalStore as LocalSocketStore

if TYPE_CHECKING:
    import argparse
    from collections.abc import AsyncIterator, Callable

PYNIXD_SOCKET = Path("/run/pynixd/pynixd.sock")
NIX_DAEMON_SOCKET = Path("/nix/var/nix/daemon-socket/socket")


def add_socket_argument(parser: argparse.ArgumentParser) -> None:
    """The `--socket` option every subcommand that dials the daemon takes."""
    parser.add_argument(
        "--socket",
        type=Path,
        default=None,
        help=f"The pynixd socket (default: PYNIXD_UNIX_PATH, NIX_REMOTE, {PYNIXD_SOCKET}, then Nix's daemon socket)",
    )


def socket_candidates(settings: PynixdSettings, explicit: Path | None) -> list[Path]:
    """Where a pynixd may listen, the most specific first.

    pynixd often serves Nix's own daemon socket, so the default of
    `unix_path` alone misses it: a NixOS host with pynixd in front of
    `nix-daemon` has no `/run/pynixd`. Issue #90.

    A path the user named -- `--socket`, or `unix_path` from the
    environment or a config file -- is the only candidate. Otherwise
    `NIX_REMOTE=unix://...` comes first, then pynixd's default, then the
    socket Nix itself dials (`NIX_DAEMON_SOCKET_PATH` moves it, as in Nix).
    """
    if explicit is not None:
        return [explicit]
    if "unix_path" in settings.model_fields_set and settings.unix_path is not None:
        return [settings.unix_path]
    found: list[Path] = []
    remote = os.environ.get("NIX_REMOTE", "")
    if remote.startswith("unix://"):
        found.append(Path(remote.removeprefix("unix://").split("?", 1)[0]))
    if settings.unix_path is not None:
        found.append(settings.unix_path)
    found.append(Path(os.environ.get("NIX_DAEMON_SOCKET_PATH") or NIX_DAEMON_SOCKET))
    return list(dict.fromkeys(found))


@contextlib.asynccontextmanager
async def open_pynixd(
    settings: PynixdSettings, explicit: Path | None, operation: str
) -> AsyncIterator[LocalSocketStore]:
    """Connect to the first candidate socket whose daemon serves *operation*.

    The handshake names every extension operation pynixd serves, so a plain
    `nix-daemon` on the same path is told apart before any request goes out.
    """
    tried: list[str] = []
    for path in socket_candidates(settings, explicit):
        if not await anyio.Path(path).exists():
            tried.append(f"{path}: no such socket")
            continue
        store = LocalSocketStore(
            LocalSocketStoreSpec(store_id=StoreId("cli"), socket_path=path, probe=False, monitor=False),
        )
        async with contextlib.AsyncExitStack() as stack:
            try:
                await stack.enter_async_context(store)
                async with store.transfer_conn(None) as conn:
                    features = set(conn.features)
            except Exception as ex:  # noqa: BLE001 -- reported below, with every other candidate
                tried.append(f"{path}: {type(ex).__name__}: {ex}")
                continue
            if operation in features:
                yield store
                return
        tried.append(f"{path}: a Nix daemon that does not serve {operation}, so not pynixd")
    lines = "\n  ".join(tried)
    raise SystemExit(f"pynixd: no pynixd daemon found. Tried:\n  {lines}\nName one with --socket or PYNIXD_UNIX_PATH.")


_DROP_SENTINEL = "_pynixd_drop"


def _load_filter_from_plugins(plugins: list[Any]) -> Callable[..., Any] | None:
    """Import each plugin path and return the first callable named ``filter``."""
    log = structlog.get_logger(__name__)
    for path in plugins:
        module = import_plugin(path)
        if module is None:
            continue
        if hasattr(module, "filter") and callable(module.filter):
            filter_fn: Callable[..., Any] = module.filter
            return filter_fn
        if callable(module):
            return module  # type: ignore[return-value]
        log.warning("log_filter_plugin_missing_filter", path=str(path))
    return None


def _build_filter_processor(filter_fn: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap a plugin filter for the structlog chain.

    Raises ``DropEvent`` when the filter drops the event.
    """

    def _proc(logger: object, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
        try:
            result = filter_fn(logger, method_name, event_dict)
        except structlog.DropEvent:
            raise
        except Exception:
            return event_dict
        if not result:
            raise structlog.DropEvent
        if isinstance(result, dict):
            return result
        return event_dict

    return _proc


def _build_foreign_filter_processor(filter_fn: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap a plugin filter for ``ProcessorFormatter.foreign_pre_chain``.

    ``ProcessorFormatter`` does not catch ``DropEvent``, so this variant
    returns a sentinel dict ``{"_pynixd_drop": True}``.  The renderer
    in ``processors`` detects the sentinel and discards the entry.
    """

    def _proc(logger: object, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
        try:
            result = filter_fn(logger, method_name, event_dict)
        except structlog.DropEvent:
            return {**event_dict, _DROP_SENTINEL: True}
        except Exception:
            return event_dict
        if not result:
            return {**event_dict, _DROP_SENTINEL: True}
        if isinstance(result, dict):
            return result
        return {**event_dict, _DROP_SENTINEL: True}

    return _proc


def _build_processors(is_foreign: bool, filter_fn: Callable[..., Any] | None) -> list[Any]:
    """Build the structlog processor chain.

    Args:
        is_foreign: True for ``ProcessorFormatter.processors`` (formatter
            downstream), False for the structlog.configure processor list.
        filter_fn: Plugin filter callable or None.
    """
    procs: list[Any] = []
    if is_foreign:
        # Shared formatter chain: strip metadata, render.
        procs.append(_DropFilteringRenderer())
    else:
        # Structlog-native chain.
        procs.extend(
            [
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.contextvars.merge_contextvars,
            ],
        )
        if filter_fn is not None:
            procs.append(_build_filter_processor(filter_fn))
        procs.extend(
            [
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
            ],
        )
    return procs


def _build_foreign_pre_chain(filter_fn: Callable[..., Any] | None) -> list[Any]:
    """Build ``foreign_pre_chain`` for ``ProcessorFormatter``.

    These processors run only on non-structlog log records and prepare
    the event dict for the shared rendering chain.
    """
    chain: list[Any] = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
    ]
    if filter_fn is not None:
        chain.append(_build_foreign_filter_processor(filter_fn))
    return chain


def _sanitize_for_journal(value: Any) -> Any:
    """Replace surrogate escapes with U+FFFD, recursively.

    Log and error text travels as bytes (`surrogateescape` keeps each one),
    and the journal is UTF-8: `StreamHandler.emit` encodes the rendered line,
    and a lone surrogate raises `UnicodeEncodeError` there. The wire copy
    stays exact; only what a human reads is lossy. Issue nixidae/nixidae#662.
    """
    if isinstance(value, str):
        return value.encode("utf-8", errors="surrogateescape").decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {key: _sanitize_for_journal(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_for_journal(item) for item in value]
    return value


class _DropFilteringRenderer:
    """A ``ProcessorFormatter`` processor that renders to JSON.

    Strips ``_record`` and ``_from_structlog`` metadata added by the
    formatter, and discards entries marked with the filter sentinel.
    The sentinel is set by the plugin filter in ``foreign_pre_chain``.
    """

    def __init__(self) -> None:
        self._json = structlog.processors.JSONRenderer()

    def __call__(self, logger: object, method_name: str, event_dict: dict[str, Any]) -> str:
        if event_dict.get(_DROP_SENTINEL):
            return ""
        event_dict.pop("_record", None)
        event_dict.pop("_from_structlog", None)
        rendered = self._json(logger, method_name, _sanitize_for_journal(event_dict))
        if isinstance(rendered, bytes):
            return rendered.decode("utf-8")
        return rendered


def setup_logging(settings: PynixdSettings) -> None:
    """Configure structlog and stdlib logging with plugin-defined filtering."""
    log_level_str = settings.log_level.upper()
    filter_fn = _load_filter_from_plugins(settings.plugins)

    # ── Structlog configure ────────────────────────────────────────
    structlog.configure(
        processors=_build_processors(is_foreign=False, filter_fn=filter_fn),
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # ── Stdlib bridge via ProcessorFormatter ───────────────────────
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=_build_foreign_pre_chain(filter_fn),
        processors=_build_processors(is_foreign=True, filter_fn=filter_fn),
    )

    logging.basicConfig(
        level=log_level_str,
        handlers=[logging.StreamHandler()],
        force=True,
    )
    for handler in logging.getLogger().handlers:
        handler.setFormatter(formatter)


def load_settings() -> PynixdSettings:
    """Load and return pynixd settings from config files and environment variables."""
    return PynixdSettings()
