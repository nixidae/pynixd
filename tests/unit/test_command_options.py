"""pynixd's own settings on an `ssh-ng://` client's SSH command (`pynixd/command_options.py`).

Nix runs `remote-program`'s words, then `--stdio` (`ssh-store.cc:227-233`), and sends no `SetOptions`
(`ssh-store.cc:92-99`).
"""

from __future__ import annotations

from pynixd.command_options import CommandOptions, PynixdOption, parse_command
from pynixd.serde.auth import Role


def test_a_trusted_client_sets_its_pool() -> None:
    parsed = parse_command("nix-daemon --option pynixd-pool big --stdio", Role.ADMIN)
    assert parsed == CommandOptions({PynixdOption.POOL: "big"})


def test_the_plain_command_sets_nothing() -> None:
    assert parse_command("nix-daemon --stdio --store /mnt", Role.ADMIN) == CommandOptions()


def test_an_untrusted_client_may_not_steer() -> None:
    parsed = parse_command("nix-daemon --option pynixd-pool big --stdio", Role.USER)
    assert parsed.options == {}
    assert parsed.warnings == (
        "ignoring the client-specified setting 'pynixd-pool', because it is a restricted setting and you are not a"
        " trusted user",
    )


def test_a_nix_setting_is_refused_with_a_warning() -> None:
    """It would reach no upstream daemon, so pynixd says so instead of taking it in part."""
    parsed = parse_command("nix-daemon --option max-jobs 0 --stdio", Role.ADMIN)
    assert parsed.options == {}
    assert parsed.warnings == ("ignoring the setting 'max-jobs' on the SSH command: pynixd reads only its own there",)


def test_an_unknown_pynixd_setting_is_named() -> None:
    parsed = parse_command("nix-daemon --option pynixd-pol big --stdio", Role.ADMIN)
    assert parsed == CommandOptions(warnings=("unknown setting 'pynixd-pol'",))


def test_a_truncated_option_is_named() -> None:
    parsed = parse_command("nix-daemon --option pynixd-pool", Role.ADMIN)
    assert parsed.options == {}
    assert len(parsed.warnings) == 1


def test_an_unbalanced_quote_sets_nothing() -> None:
    parsed = parse_command("nix-daemon --option pynixd-pool 'big --stdio", Role.ADMIN)
    assert parsed.options == {}
    assert parsed.warnings[0].startswith("ignoring the SSH command's settings")
