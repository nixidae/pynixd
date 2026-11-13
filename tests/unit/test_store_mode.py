"""`StoreMode`: the legal combinations of a local store's five mode flags, by name (nixidae/nixidae#1318)."""

from __future__ import annotations

import itertools

import pytest
from pydantic import ValidationError

from pynixd.config import LocalSocketStoreSpec
from pynixd.store_mode import Answers, Storage, StoreMode

FLAGS = ("use_db", "daemon", "pynixd_writes", "read_only", "immutable")

# Every legal combination, as (use_db, daemon, pynixd_writes, read_only, immutable). Each one left out has no mode.
LEGAL: dict[tuple[bool, ...], StoreMode] = {
    (False, True, False, False, False): StoreMode(Answers.PROXY),
    # `pynixd_writes` with no database: the daemon still takes every write.
    (False, True, True, False, False): StoreMode(Answers.PROXY),
    (True, True, False, False, False): StoreMode(Answers.DAEMON_WRITES),
    (True, True, False, True, False): StoreMode(Answers.DAEMON_WRITES, Storage.READ_ONLY),
    (True, True, False, True, True): StoreMode(Answers.DAEMON_WRITES, Storage.IMMUTABLE),
    (True, True, True, False, False): StoreMode(Answers.SHARED),
    (True, False, True, False, False): StoreMode(Answers.DAEMONLESS),
    (True, False, True, True, False): StoreMode(Answers.DAEMONLESS, Storage.READ_ONLY),
    (True, False, True, True, True): StoreMode(Answers.DAEMONLESS, Storage.IMMUTABLE),
}
COMBINATIONS = list(itertools.product((False, True), repeat=len(FLAGS)))


@pytest.mark.parametrize("flags", COMBINATIONS, ids=lambda flags: "-".join(str(int(flag)) for flag in flags))
def test_each_combination_has_its_mode_or_none(flags: tuple[bool, ...]) -> None:
    kwargs = dict(zip(FLAGS, flags, strict=True))
    if flags in LEGAL:
        assert StoreMode.of(**kwargs) == LEGAL[flags]
        spec = LocalSocketStoreSpec.model_validate({"store_id": "s", "store_path": "/tmp/s", **kwargs})
        assert spec.mode == LEGAL[flags]
        return
    with pytest.raises(ValueError, match="needs"):
        StoreMode.of(**kwargs)
    with pytest.raises(ValidationError, match="needs"):
        LocalSocketStoreSpec.model_validate({"store_id": "s", "store_path": "/tmp/s", **kwargs})


def test_the_default_spec_reads_and_leaves_writes_to_the_daemon() -> None:
    mode = LocalSocketStoreSpec.model_validate({"store_id": "s", "store_path": "/tmp/s"}).mode
    assert mode == StoreMode(Answers.DAEMON_WRITES)
    assert (mode.has_daemon, mode.reads_database, mode.pynixd_writes, mode.read_only) == (True, True, False, False)


@pytest.mark.parametrize(
    ("mode", "properties"),
    [
        (StoreMode(Answers.PROXY), (True, False, False)),
        (StoreMode(Answers.DAEMON_WRITES), (True, True, False)),
        (StoreMode(Answers.SHARED), (True, True, True)),
        (StoreMode(Answers.DAEMONLESS), (False, True, True)),
    ],
)
def test_who_answers_gives_daemon_database_and_writes(mode: StoreMode, properties: tuple[bool, bool, bool]) -> None:
    assert (mode.has_daemon, mode.reads_database, mode.pynixd_writes) == properties
