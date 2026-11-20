"""The sites `loop_audit`'s gate lets through: `path function` to the reason.

An entry is a debt (`DEBT`) or a measured reason the call stays. Moving a call off the loop removes its entry.
"""

from __future__ import annotations

from typing import Final

DEBT: Final = "not moved off the loop yet (nixidae/nixidae#1547)"

EXEMPT: Final[dict[str, str]] = {
    "pynixd/ssh_server.py _listed_role": (
        "asyncssh's `match_options` (`auth_keys.py:180`) subscripts `List['X509NamePattern']`;"
        " typing compiles that forward reference once per process and caches it"
    ),
}
