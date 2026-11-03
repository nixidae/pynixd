"""Substituter URLs, and the store spec each one opens: Nix's store registry.

Nix maps a URL scheme to a store config (`Implementations`,
`store-registration.hh:54`). Here a scheme maps to a function from the parsed
URL to a spec. A scheme with no entry, or a parameter its entry does not
read, gives `None`, and the daemon answers for that substituter: pynixd never
drops a setting the user wrote. `docs/design/store-model.md`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Final
from urllib.parse import parse_qs, urlsplit

import structlog

from nix_daemon_protocol.ids import StoreId

from .config import ExternalUnixStoreSpec, HTTPBinaryCacheSpec, LocalBinaryCacheSpec

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from .config import PynixdSettings, StoreSpecBase

log = structlog.get_logger(__name__)

_LOOPBACK_HOSTS: Final = frozenset({"127.0.0.1", "::1", "localhost"})
_SUBSTITUTER_PARAMS: Final = frozenset({"trusted", "priority"})


@dataclass(frozen=True, slots=True)
class StoreReference:
    """A store URL taken apart, as Nix's `StoreReference` (`store-reference.hh`)."""

    scheme: str
    authority: str
    path: str
    params: Mapping[str, str]

    @classmethod
    def parse(cls, url: str) -> StoreReference:
        """*url* in parts; a repeated parameter keeps its last value."""
        split = urlsplit(url)
        params = {key: values[-1] for key, values in parse_qs(split.query, keep_blank_values=True).items()}
        return cls(split.scheme, split.netloc, split.path, MappingProxyType(params))


def _flag(value: str | None) -> bool | None:
    """Nix's boolean setting: `true`, `1`, `false`, `0`; `None` for anything else."""
    return {None: False, "true": True, "1": True, "false": False, "0": False}.get(value)


def _substituter_settings(ref: StoreReference, known: frozenset[str]) -> dict[str, object] | None:
    """`trusted` and `priority` from *ref*, or `None` for a parameter outside *known* or a bad value."""
    if not set(ref.params) <= known | _SUBSTITUTER_PARAMS:
        return None
    trusted = _flag(ref.params.get("trusted"))
    if trusted is None:
        return None
    settings: dict[str, object] = {"trusted": trusted}
    # Unset, not the spec's default: a cache takes its `nix-cache-info` priority then.
    if (priority := ref.params.get("priority")) is not None:
        try:
            settings["priority"] = float(priority)
        except ValueError:
            return None
    return settings


def _name(*parts: str) -> str:
    stripped = (part.strip("/").replace("/", "-").replace(":", "-") for part in parts)
    return "-".join(part for part in stripped if part)[:120]


def _http(ref: StoreReference) -> StoreSpecBase | None:
    settings = _substituter_settings(ref, frozenset())
    if settings is None:
        return None
    url = f"{ref.scheme}://{ref.authority}{ref.path}"
    name = _name("http", ref.authority, ref.path, "trusted" if settings["trusted"] else "")
    return HTTPBinaryCacheSpec.model_validate({"store_id": StoreId(name), "url": url, **settings})


def _file(ref: StoreReference) -> StoreSpecBase | None:
    path = Path(ref.authority + ref.path)
    settings = _substituter_settings(ref, frozenset())
    if settings is None or not path.is_absolute():
        return None
    name = _name("file", str(path), "trusted" if settings["trusted"] else "")
    return LocalBinaryCacheSpec.model_validate({"store_id": StoreId(name), "path": path, **settings})


def _unix(ref: StoreReference) -> StoreSpecBase | None:
    settings = _substituter_settings(ref, frozenset({"root"}))
    if settings is None or not ref.path:
        return None
    del settings["trusted"]
    socket_path, store_path = Path(ref.path), Path(ref.params.get("root", "/"))
    name = _name("unix", str(socket_path), str(store_path))
    return ExternalUnixStoreSpec.model_validate(
        {"store_id": StoreId(name), "socket_path": socket_path, "store_path": store_path, **settings}
    )


SCHEMES: Final[Mapping[str, Callable[[StoreReference], StoreSpecBase | None]]] = MappingProxyType(
    {
        "http": _http,
        "https": _http,
        "file": _file,
        "unix": _unix,
    }
)
"""Each scheme pynixd opens a store for. Any other stays with the daemon."""


def spec_for(url: str) -> StoreSpecBase | None:
    """The spec *url* opens, its `store_id` derived from the URL; `None` when the daemon answers for it.

    Pure: it reads no file and opens nothing, so a caller may use it only to
    compare `reference_key`s.
    """
    ref = StoreReference.parse(url)
    factory = SCHEMES.get(ref.scheme)
    return None if factory is None else factory(ref)


def reference_key(url: str) -> object | None:
    """The `reference_key` of the store *url* opens, or `None` when the daemon answers for it."""
    spec = spec_for(url)
    return None if spec is None else spec.reference_key()


def is_own_cache(url: str, settings: PynixdSettings) -> bool:
    """Whether *url* is this pynixd's own HTTP cache.

    A node's nix.conf can list it, for Nix. As a substituter of pynixd, a
    narinfo miss there asks pynixd itself, joins the race it is part of, and
    waits out `substitution_query_timeout`: 2 s on every new local build on
    dynhetz, 675 timeouts in 30 minutes (nixidae/nixidae#1125).
    """
    split = urlsplit(url)
    port = settings.http_port if split.scheme == "http" else settings.https_port
    if port is None or split.port != port:
        return False
    host = split.hostname or ""
    return host in _LOOPBACK_HOSTS or (bool(settings.http_host) and host == settings.http_host)


def substituter_spec(url: str, settings: PynixdSettings, *, prefix: str = "") -> StoreSpecBase | None:
    """The spec of a backend for the substituter *url*, or `None` when the daemon answers for it.

    `None` also for this pynixd's own cache, and for a `file://` directory
    pynixd cannot read: the daemon may read it.
    """
    spec = spec_for(url)
    if spec is None:
        return None
    if is_own_cache(url, settings):
        log.info("substituter_is_own_cache", url=url)
        return None
    if isinstance(spec, LocalBinaryCacheSpec) and not os.access(spec.path, os.R_OK | os.X_OK):
        log.info("substituter_unreadable", url=url)
        return None
    if prefix:
        spec = spec.model_copy(update={"store_id": StoreId(f"{prefix}{spec.store_id}")})
    return spec
