"""`external-builders`: Nix's setting, its selection before the platform check, and its set-up protocol.

Nix's `tests/functional/external-builders.sh` builds a derivation for `x68_46-xunil` through one
(`:21`); pynixd's builder runs it the same way when the store runs no daemon.
`tests/functional/test_external_builder.py` compares a whole build with `nix-daemon`'s.
"""

from __future__ import annotations

import json

import pytest

from nix_daemon_protocol.basic_derivation import BasicDerivation
from nix_daemon_protocol.derivation_output import DerivationOutput
from nix_daemon_protocol.ids import LOCAL_STORE_ID
from nix_daemon_protocol.store_path import StorePath
from pynixd.build_queue import BuildQueue
from pynixd.serde import BuildResultStatus
from pynixd_store.builder.local import SetupMessages, external_builder, hands_off, rejection, unsupported
from pynixd_store.builder.settings import BuildSettings, ExternalBuilder
from tests._conftest.execution import FakeSandbox, fake_executor, queued
from tests._conftest.placement import FakeStore

XUNIL = ExternalBuilder(frozenset({"x68_46-xunil"}), "/bin/external", ("bla",))
ENTRY = {"systems": ["x68_46-xunil"], "program": "/bin/external", "args": ["bla"]}
DRV_PATH = StorePath("/nix/store/00000000000000000000000000000000-external.drv")


def _drv(platform: str = "x68_46-xunil") -> BasicDerivation:
    out = DerivationOutput(path="/nix/store/00000000000000000000000000000000-external")
    return BasicDerivation(outputs={"out": out}, platform=platform, builder="/bin/sh")


def _settings(overrides: dict[str, str] | None = None, config: dict[str, object] | None = None) -> BuildSettings:
    return BuildSettings.from_nix_config(config or {}, {"system": "x86_64-linux", **(overrides or {})})


def test_an_override_is_json_text_and_the_config_a_list() -> None:
    """The client sends the setting's text; `nix config show --json` holds the parsed list."""
    assert _settings({"external-builders": json.dumps([ENTRY])}).external_builders == (XUNIL,)
    assert _settings(config={"external-builders": {"value": [ENTRY]}}).external_builders == (XUNIL,)
    assert _settings().external_builders == ()


def test_a_bad_override_is_ignored_and_the_config_stays() -> None:
    """`ClientSettings::apply` warns and keeps the setting (`daemon.cc:330`)."""
    config: dict[str, object] = {"external-builders": {"value": [ENTRY]}}
    for bad in ("[{", '[{"systems": ["x"]}]', '{"systems": []}'):
        assert _settings({"external-builders": bad}, config).external_builders == (XUNIL,)


def test_a_missing_key_is_an_error() -> None:
    """`from_json` reads each key with `valueAt` (`derivation-builder.cc:8`)."""
    with pytest.raises(ValueError, match="'program'"):
        ExternalBuilder.parse_all([{"systems": ["x"], "args": []}])


def test_the_first_builder_for_the_system_is_chosen() -> None:
    """`findExternalDerivationBuilderIfSupported` (`globals.cc:283`)."""
    other = {"systems": ["x68_46-xunil", "aarch64-linux"], "program": "/bin/other", "args": []}
    settings = _settings({"external-builders": json.dumps([{**ENTRY, "systems": ["riscv64-linux"]}, ENTRY, other])})
    assert external_builder(_drv(), settings) == XUNIL
    assert external_builder(_drv("aarch64-linux"), settings) == ExternalBuilder(
        frozenset({"x68_46-xunil", "aarch64-linux"}), "/bin/other", ()
    )
    assert external_builder(_drv("x86_64-linux"), settings) is None


def test_an_external_builder_takes_a_foreign_system_before_any_check() -> None:
    """`derivation-building-goal.cc:343` returns the capability before the platform, the features and
    `max-jobs = 0` (`:352`); the hook alone comes first, so `hands_off` ignores an external builder."""
    with_it = _settings({"external-builders": json.dumps([ENTRY]), "max-jobs": "0"})
    without = _settings({"max-jobs": "0"})
    assert unsupported(_drv(), with_it, store_dir="/nix/store", relocated=False) is None
    assert unsupported(_drv(), without, store_dir="/nix/store", relocated=False) == "the platform x68_46-xunil"
    assert rejection(DRV_PATH, _drv(), with_it) is None
    refused = rejection(DRV_PATH, _drv(), without)
    assert refused is not None
    assert "platform mismatch" in refused
    assert "local builds are disabled" in refused
    assert not hands_off(with_it)


def test_the_feature_comes_from_the_store_config() -> None:
    """`ExternalDerivationBuilder` requires `Xp::ExternalBuilders` (`external-derivation-builder.cc:19`)."""
    assert _settings({"experimental-features": "external-builders"}).external_builders_feature
    assert not _settings().external_builders_feature


def test_set_up_lines_end_at_the_line_that_starts_with_2() -> None:
    """`processSandboxSetupMessages` (`derivation-builder.cc:1029`): the lines before `\\2` are not log."""
    setup = SetupMessages()
    assert setup.feed(b"starting\nhalf") == b""
    assert setup.feed(b" a line\n\x02") == b""
    assert setup.feed(b"\nfirst log line\n") == b"first log line\n"
    assert setup.done
    assert setup.messages == ["starting", "half a line"]
    assert setup.feed(b"more\n") == b"more\n"


def test_a_line_that_starts_with_1_is_a_set_up_error() -> None:
    setup = SetupMessages()
    assert setup.feed(b"starting\n\x01no VM here\n") == b""
    assert setup.error == "no VM here"
    assert not setup.done


async def test_a_store_with_no_daemon_refuses_once_and_names_the_reason() -> None:
    """A build pynixd's builder cannot run, on a store with no daemon, fails at once: no retry changes it."""
    store = FakeStore(LOCAL_STORE_ID)
    store.runs_daemon = False
    queue = BuildQueue()
    build = await queued(queue)
    executor = fake_executor(queue, {LOCAL_STORE_ID: store}, here=FakeSandbox())
    await executor.run(build, store)
    assert build.answer.done()
    assert build.retries == 0
    result = build.answer.result().result
    assert result.status == BuildResultStatus.INPUT_REJECTED
    assert "the test's sandbox builds nothing" in str(result.error_msg)
    assert "runs no Nix daemon to build it" in str(result.error_msg)
