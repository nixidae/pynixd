"""Public API for pynixd stores.

**A store type loads when a configuration names it, and not before.**
`http_binary_cache` pulls `aiohttp`, and `ssh` pulls `asyncssh`, which
`reverse` pulls in turn. A daemon that serves a Unix socket uses none of the
three, and this file used to load all of them for every start: 232 modules and
0.28 s. Issue #30 holds the measurement.

`__getattr__` (PEP 562) resolves each name on the first read, so
`from pynixd.store import SSHSocketStore` still works and pays for `asyncssh`
only at that moment. `config.py` already imports each store class inside its
`to_store`, so a spec that no configuration holds now costs nothing.

The table is written twice, once for the interpreter and once for the type checker.
`tests/unit/test_import_budget.py` compares the two.
"""

from __future__ import annotations

import importlib
import sys
from typing import TYPE_CHECKING, TypeGuard

if TYPE_CHECKING:
    from .base import (
        Store as Store,
        get_current_system as get_current_system,
    )
    from .daemon import (
        DaemonStore as DaemonStore,
        ProbeState as ProbeState,
    )
    from .external_unix import ExternalUnixStore as ExternalUnixStore
    from .http_binary_cache import HTTPBinaryCacheStore as HTTPBinaryCacheStore
    from .local import LocalSocketStore as LocalSocketStore
    from .local_daemon import LocalStore as LocalStore
    from .local_db import LocalDBStore as LocalDBStore
    from .reverse import ReverseStore as ReverseStore
    from .ssh import (
        SSHSocketStore as SSHSocketStore,
        SSHSubprocessStore as SSHSubprocessStore,
    )

ORIGIN: dict[str, str] = {
    "DaemonStore": "daemon",
    "ExternalUnixStore": "external_unix",
    "HTTPBinaryCacheStore": "http_binary_cache",
    "LocalDBStore": "local_db",
    "LocalSocketStore": "local",
    "LocalStore": "local_daemon",
    "ProbeState": "daemon",
    "ReverseStore": "reverse",
    "SSHSocketStore": "ssh",
    "SSHSubprocessStore": "ssh",
    "Store": "base",
    "get_current_system": "base",
}
"""Each public name, and the submodule that defines it."""

__all__ = [
    "DaemonStore",
    "ExternalUnixStore",
    "HTTPBinaryCacheStore",
    "LocalDBStore",
    "LocalSocketStore",
    "LocalStore",
    "ProbeState",
    "ReverseStore",
    "SSHSocketStore",
    "SSHSubprocessStore",
    "Store",
    "get_current_system",
    "is_http_binary_cache",
]


def __getattr__(name: str) -> object:
    """Import the submodule that defines *name*, and keep the name here."""
    origin = ORIGIN.get(name)
    if origin is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(importlib.import_module(f".{origin}", __name__), name)
    globals()[name] = value
    return value


def is_http_binary_cache(store: Store) -> TypeGuard[HTTPBinaryCacheStore]:
    """Whether *store* is an `HTTPBinaryCacheStore`, and never load one to ask.

    A `TypeGuard` and not a `bool`, so a caller past the check may reach the
    methods only this store has. With `bool` a checker keeps the argument at
    `Store`, and `substitution_queue` calling `get_narinfo` and `stream_nar`
    inside the guarded branch read as errors. The class is imported under
    TYPE_CHECKING already, so the annotation loads nothing at runtime, which
    is the whole point of the function.

    `False` when nothing imported `http_binary_cache`, because an instance of
    a class cannot exist before the class does. `HTTPBinaryCacheSpec.to_store`
    is the only maker of one, and it imports the module itself, so a `True`
    answer always finds the module already there.

    This exists so that an `isinstance` check over a set of stores does not
    load `aiohttp` for a configuration that holds no cache. Issue #30.
    """
    module = sys.modules.get(f"{__name__}.http_binary_cache")
    if module is None:
        return False
    return isinstance(store, module.HTTPBinaryCacheStore)
