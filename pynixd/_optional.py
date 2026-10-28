"""The four servers that a configuration may never start.

Each one loads `asyncssh` or `aiohttp`, and `Server.start` runs it only when
its port or its `enabled` flag is set. A daemon that serves a Unix socket
starts none of them, and it used to load all four anyway: 232 modules and
0.28 s of every start. Issue #30 holds the measurement.

**Read the module through this one, and not with an import.** `instance.py`
does `from . import _optional` at the top, and
`_optional.ssh_server.start_ssh_server(...)` inside `Server.start`. That is an
attribute read on a module object, so `__getattr__` below runs and imports the
module at the moment the configuration asks for it.

The table is written twice, once for the interpreter and once for the type checker.
`tests/unit/test_import_budget.py` compares the two.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType

    from . import (
        http_server as http_server,
        reverse_client as reverse_client,
        reverse_server as reverse_server,
        ssh_server as ssh_server,
    )

MODULES = frozenset({"http_server", "reverse_client", "reverse_server", "ssh_server"})
"""The submodules of `pynixd` that this module resolves. Nothing else."""


def __getattr__(name: str) -> ModuleType:
    """Import `pynixd.<name>` on the first read, and keep it here."""
    if name not in MODULES:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = importlib.import_module(f".{name}", __package__)
    globals()[name] = module
    return module
