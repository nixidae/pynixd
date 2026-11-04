"""The colours of a Nix message, which the daemon sends as bytes whatever the client's terminal.

`HintFmt` (`fmt.hh`) wraps each argument of `Error(format, args...)` and
`warn(format, args...)` in magenta; a message with no argument has none.
`ansicolor.hh` defines the codes.
"""

from __future__ import annotations

ANSI_NORMAL = "\x1b[0m"
ANSI_BOLD = "\x1b[1m"
ANSI_RED = "\x1b[31;1m"
ANSI_MAGENTA = "\x1b[35;1m"


ERROR_PREFIX = f"{ANSI_RED}error:{ANSI_NORMAL} "
"""The word `showErrorInfo` writes in front of an error. The daemon colours it whatever the client wants."""


def magenta(value: object) -> str:
    """One argument of a Nix message, as `HintFmt` renders it."""
    return f"{ANSI_MAGENTA}{value}{ANSI_NORMAL}"


def show_error(message: str) -> str:
    """*message* as `showErrorInfo` renders it, which `Error::msg` answers.

    Each line after the first carries seven spaces, the width of "error: ",
    and no line feed ends the text.
    """
    return ERROR_PREFIX + "\n       ".join(message.split("\n"))
