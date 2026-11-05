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
WARNING_PREFIX = f"{ANSI_MAGENTA}warning:{ANSI_NORMAL} "
"""`showErrorInfo`'s word at `lvlWarn` (`error.cc:245`); `ANSI_WARNING` is magenta."""


def magenta(value: object) -> str:
    """One argument of a Nix message, as `HintFmt` renders it."""
    return f"{ANSI_MAGENTA}{value}{ANSI_NORMAL}"


def _chomp(line: str) -> str:
    return line.rstrip(" \n\r\t")


def show_error(message: str, traces: tuple[str, ...] = ()) -> str:
    """*message* as `showErrorInfo` renders it (`error.cc:229`), which `Error::msg` answers.

    Each *trace* is an `addTrace` hint, innermost last: a `… ` line above a
    second "error: ". `indent` (`error.cc:120`) gives each line after the first
    seven spaces, the width of "error: ", and chomps every line, so an empty
    line carries none. No line feed ends the text.
    """
    if traces:
        message = "".join(f"\n… {hint}\n" for hint in traces) + "\n" + ERROR_PREFIX + message
    lines = _chomp(message).split("\n")
    return "\n".join(_chomp(("       " if i else ERROR_PREFIX) + line) for i, line in enumerate(lines))
