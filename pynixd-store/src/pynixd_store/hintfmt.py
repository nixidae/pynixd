"""The colours of a Nix message, which the daemon sends as bytes whatever the client's terminal.

`HintFmt` (`fmt.hh`) wraps each argument of `Error(format, args...)` and
`warn(format, args...)` in magenta; a message with no argument has none.
`ansicolor.hh` defines the codes.
"""

from __future__ import annotations

ANSI_NORMAL = "\x1b[0m"
ANSI_RED = "\x1b[31;1m"
ANSI_MAGENTA = "\x1b[35;1m"


def magenta(value: object) -> str:
    """One argument of a Nix message, as `HintFmt` renders it."""
    return f"{ANSI_MAGENTA}{value}{ANSI_NORMAL}"
