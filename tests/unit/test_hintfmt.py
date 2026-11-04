"""`pynixd_store.hintfmt.show_error`: `showErrorInfo` (`error.cc:229`) and its `indent` (`error.cc:120`).

The expected text is what the daemon wrote for a `builtin:fetchurl` of a missing file under Nix 2.35.
"""

from __future__ import annotations

from pynixd_store.hintfmt import ERROR_PREFIX, show_error


def test_each_line_after_the_first_carries_seven_spaces_and_an_empty_one_none() -> None:
    assert show_error("one\n\ntwo  ") == f"{ERROR_PREFIX}one\n\n       two"


def test_a_trace_comes_above_a_second_error_line() -> None:
    message = "unable to download 'u': HTTP error 404\n\nresponse body:\n\nno such file"
    assert show_error(message, ('writing file "/nix/store/x"',)) == "\n".join(
        [
            ERROR_PREFIX.rstrip(),
            '       … writing file "/nix/store/x"',
            "",
            f"       {ERROR_PREFIX}unable to download 'u': HTTP error 404",
            "",
            "       response body:",
            "",
            "       no such file",
        ]
    )
