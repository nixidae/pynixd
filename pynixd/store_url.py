"""Substituter URLs, and the store spec each one opens: Nix's store registry.

Nix maps a URL scheme to a store config (`Implementations`,
`store-registration.hh:54`). Here a scheme maps to a function from the parsed
URL to a spec. A scheme with no entry, or a parameter its entry does not
read, gives `None`, and the daemon answers for that substituter: pynixd never
drops a setting the user wrote. `docs/design/store-model.md`.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import os
import posixpath
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Final
from urllib.parse import parse_qs, urlsplit

import structlog

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.store_dir import store_dir

from .config import (
    ExternalUnixStoreSpec,
    HTTPBinaryCacheSpec,
    LocalBinaryCacheSpec,
    S3BinaryCacheSpec,
    SSHSubprocessStoreSpec,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from .config import PynixdSettings, StoreSpecBase

log = structlog.get_logger(__name__)

_LOOPBACK_HOSTS: Final = frozenset({"127.0.0.1", "::1", "localhost"})
_SUBSTITUTER_PARAMS: Final = frozenset({"trusted", "priority"})
_BINARY_CACHE_PARAMS: Final = frozenset({"store"})
"""What each binary cache reads beside a substituter's own: `StoreDirConfig::storeDir`."""


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
    if (directory := ref.params.get("store")) is not None:
        if not directory.startswith("/"):
            return None
        # Nix canonicalises the setting, so `/nix/store/` is pynixd's own directory and the same store.
        directory = posixpath.normpath(directory)
        if directory != store_dir():
            settings["store_dir"] = directory
    # Unset here: a cache takes its `nix-cache-info` priority, any other store Nix's 0 (`store-api.hh:286`).
    if (priority := ref.params.get("priority")) is not None:
        try:
            settings["priority"] = float(priority)
        except ValueError:
            return None
    return settings


def _name(*parts: str) -> str:
    stripped = (part.strip("/").replace("/", "-").replace(":", "-") for part in parts)
    return "-".join(part for part in stripped if part)[:120]


def _store_dir_part(settings: Mapping[str, object]) -> str:
    return str(settings.get("store_dir") or "")


def _http(ref: StoreReference) -> StoreSpecBase | None:
    settings = _substituter_settings(ref, _BINARY_CACHE_PARAMS)
    if settings is None:
        return None
    url = f"{ref.scheme}://{ref.authority}{ref.path}"
    name = _name("http", ref.authority, ref.path, _store_dir_part(settings), "trusted" if settings["trusted"] else "")
    return HTTPBinaryCacheSpec.model_validate({"store_id": StoreId(name), "url": url, **settings})


def _file(ref: StoreReference) -> StoreSpecBase | None:
    path = Path(ref.authority + ref.path)
    settings = _substituter_settings(ref, _BINARY_CACHE_PARAMS)
    if settings is None or not path.is_absolute():
        return None
    name = _name("file", str(path), _store_dir_part(settings), "trusted" if settings["trusted"] else "")
    return LocalBinaryCacheSpec.model_validate({"store_id": StoreId(name), "path": path, **settings})


def _unix(ref: StoreReference) -> StoreSpecBase | None:
    settings = _substituter_settings(ref, frozenset({"root"}))
    if settings is None or not ref.path:
        return None
    settings.setdefault("priority", 0.0)
    socket_path, store_path = Path(ref.path), Path(ref.params.get("root", "/"))
    name = _name("unix", str(socket_path), str(store_path), "trusted" if settings["trusted"] else "")
    return ExternalUnixStoreSpec.model_validate(
        {"store_id": StoreId(name), "socket_path": socket_path, "store_path": store_path, **settings}
    )


_S3_PARAMS: Final = frozenset({"profile", "region", "scheme", "endpoint", "addressing-style"})
"""What `S3BinaryCacheStoreConfig` reads on a read (`s3-binary-cache-store.hh`); `versionId` stays with the daemon."""


def _s3(ref: StoreReference) -> StoreSpecBase | None:
    settings = _substituter_settings(ref, _S3_PARAMS | _BINARY_CACHE_PARAMS)
    style = ref.params.get("addressing-style", "auto")
    if settings is None or not ref.authority or style not in ("auto", "path", "virtual"):
        return None
    key = ref.path.strip("/")
    # NIX-DEFECT (#23): `HttpBinaryCacheStore` keys its narinfo disk cache by the reference without parameters
    # (`http-binary-cache-store.cc:76`), so `s3://cache` on two endpoints shares one cache, and a path in one
    # reads as present in the other. pynixd names the store by every parameter that picks the bucket.
    # Lillecarl/nix#406.
    where = (ref.params.get(param, "") for param in ("endpoint", "region", "profile"))
    name = _name("s3", ref.authority, key, *where, _store_dir_part(settings), "trusted" if settings["trusted"] else "")
    spec = S3BinaryCacheSpec.model_validate(
        {
            "store_id": StoreId(name),
            "bucket": ref.authority,
            "key": key,
            "region": ref.params.get("region"),
            "scheme": ref.params.get("scheme"),
            "endpoint": ref.params.get("endpoint"),
            "addressing_style": style,
            "profile": ref.params.get("profile"),
            **settings,
        }
    )
    try:
        spec.url()
    except ValueError:
        # Nix refuses the address when it opens the store, and drops the substituter with a warning.
        return None
    return spec


_SSH_NG_PARAMS: Final = frozenset(
    {"ssh-key", "base64-ssh-public-host-key", "compress", "remote-program", "remote-store"}
)
OPENSSH_KNOWN_HOSTS: Final = (
    "~/.ssh/known_hosts",
    "~/.ssh/known_hosts2",
    "/etc/ssh/ssh_known_hosts",
    "/etc/ssh/ssh_known_hosts2",
)
"""OpenSSH's `UserKnownHostsFile` and `GlobalKnownHostsFile` defaults, which Nix's `ssh` reads."""
SSH_NG_CONNECT_TIMEOUT: Final = 10.0
"""A dead host fails a substituter's start in this many seconds, not TCP's two minutes."""


def _host_public_key(encoded: str | None) -> str | None:
    """`base64-ssh-public-host-key` as an OpenSSH key line; `ValueError` when it has not that shape.

    The shape only: `asyncssh` parses the key at connect, so the daemon does
    not load it (`tests/unit/test_import_budget.py`).
    """
    if not encoded:
        return None
    try:
        line = base64.b64decode(encoded, validate=True).decode().strip()
        _, blob, *_ = line.split()
        base64.b64decode(blob, validate=True)
    except (binascii.Error, UnicodeDecodeError) as e:
        raise ValueError(encoded) from e
    return line


def _ssh_ng(ref: StoreReference) -> StoreSpecBase | None:
    """Nix's `SSHStoreConfig` (`ssh-store.hh`), as a store that only substitutes."""
    settings = _substituter_settings(ref, _SSH_NG_PARAMS)
    compress = _flag(ref.params.get("compress"))
    key = ref.params.get("ssh-key")
    program = ref.params.get("remote-program", "nix-daemon").split()
    if settings is None or compress is None or ref.path not in {"", "/"} or not program:
        return None
    if key is not None and not Path(key).is_absolute():
        return None
    try:
        authority = urlsplit(f"//{ref.authority}")
        host, port = authority.hostname, authority.port or 22
        host_public_key = _host_public_key(ref.params.get("base64-ssh-public-host-key"))
    except ValueError:
        return None
    if not host:
        return None
    settings.setdefault("priority", 0.0)
    store = ref.params.get("remote-store")
    command = [*program, "--stdio", *(["--store", store] if store else [])]
    spec = SSHSubprocessStoreSpec.model_validate(
        {
            "store_id": StoreId("ssh-ng"),
            "host": host,
            "port": port,
            "username": authority.username,
            "known_hosts": list(OPENSSH_KNOWN_HOSTS),
            "host_public_key": host_public_key,
            "client_keys": [key] if key is not None else [],
            "remote_command": command,
            "compress": compress,
            "connect_timeout": SSH_NG_CONNECT_TIMEOUT,
            "monitor": False,
            "probe": False,
            "scheduleable": False,
            "no_schedule": True,
            "gc_enabled": False,
            **settings,
        }
    )
    # Two URLs to one host differ in a key or a command; the hash keeps their ids apart.
    digest = hashlib.sha256(repr(spec.reference_key()).encode()).hexdigest()[:8]
    return spec.model_copy(update={"store_id": StoreId(_name("ssh-ng", ref.authority, digest))})


SCHEMES: Final[Mapping[str, Callable[[StoreReference], StoreSpecBase | None]]] = MappingProxyType(
    {
        "http": _http,
        "https": _http,
        "file": _file,
        "unix": _unix,
        "ssh-ng": _ssh_ng,
        "s3": _s3,
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
    waits on itself. With a 2 s `substitution_query_timeout` that cost 2 s on
    every new local build on dynhetz, 675 timeouts in 30 minutes
    (nixidae/nixidae#1125); with none, it never ends.
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
