"""The mode of a local store: who answers it, and whether its storage may change.

A `LocalSocketStoreSpec` sets five flags (`use_db`, `daemon`, `pynixd_writes`,
`read_only`, `immutable`). Of their 32 combinations, `StoreMode.of` accepts the
legal ones and names each by two axes; the store reads the mode, not the flags.
nixidae/nixidae#1318.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

__all__ = ["Answers", "Storage", "StoreMode"]


class Answers(StrEnum):
    """Who answers the operations of the store."""

    PROXY = "proxy"
    """The daemon answers every operation; pynixd reads no database."""
    DAEMON_WRITES = "daemon-writes"
    """pynixd answers reads from the database; the daemon makes every change."""
    SHARED = "shared"
    """pynixd reads and may write, each operation on its route; the daemon answers the rest."""
    DAEMONLESS = "daemonless"
    """pynixd answers every operation; no daemon runs."""


class Storage(StrEnum):
    """Whether the store's storage may change."""

    LIVE = "live"
    READ_ONLY = "read-only"
    """Nothing changes the store; another process may (`local?read-only=true`)."""
    IMMUTABLE = "immutable"
    """Read-only, and no process changes the storage: a read-only medium or a snapshot."""


@dataclass(frozen=True, slots=True)
class StoreMode:
    answers: Answers
    storage: Storage = Storage.LIVE

    @property
    def has_daemon(self) -> bool:
        return self.answers is not Answers.DAEMONLESS

    @property
    def reads_database(self) -> bool:
        return self.answers is not Answers.PROXY

    @property
    def pynixd_writes(self) -> bool:
        """pynixd may answer an operation that changes the store (`LocalSocketStoreSpec.pynixd_writes`)."""
        return self.answers in {Answers.SHARED, Answers.DAEMONLESS}

    @property
    def read_only(self) -> bool:
        return self.storage is not Storage.LIVE

    @classmethod
    def of(cls, *, use_db: bool, daemon: bool, pynixd_writes: bool, read_only: bool, immutable: bool) -> StoreMode:
        """The mode the five flags name; `ValueError` for a combination with none."""
        if immutable and not read_only:
            raise ValueError("immutable needs read_only: storage that cannot change takes no write")
        if read_only:
            if not use_db:
                raise ValueError("read_only needs use_db: pynixd reads the database to answer a read-only store")
            if daemon and pynixd_writes:
                raise ValueError(
                    "read_only with a daemon needs pynixd_writes off: the read-only daemon takes each write"
                )
        if not daemon:
            if not use_db:
                raise ValueError("daemon = false needs use_db: with no daemon, pynixd reads the database itself")
            if not pynixd_writes:
                raise ValueError("daemon = false needs pynixd_writes: with no daemon, pynixd writes the store itself")
        storage = Storage.IMMUTABLE if immutable else Storage.READ_ONLY if read_only else Storage.LIVE
        if not use_db:
            # `pynixd_writes` changes nothing here: with no database, the daemon takes every write.
            return cls(Answers.PROXY, storage)
        if not daemon:
            return cls(Answers.DAEMONLESS, storage)
        return cls(Answers.SHARED if pynixd_writes else Answers.DAEMON_WRITES, storage)
