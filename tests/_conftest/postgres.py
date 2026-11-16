"""A PostgreSQL server for the store tests on PostgreSQL (nixidae/nixidae#1209).

One cluster per test process, and a new database for each test. The server is a child of the
fixture, not of `pg_ctl`, so it ends with the session. It listens on a unix socket alone.

The cluster collates with ICU's root locale, not with `C`. Under `C` a path column that lacks
`COLLATE "C"` sorts the same as one that has it, and no test could see the difference.

`PYNIXD_STORE_DATABASE=postgres` is a lane: with `PYNIXD_NO_DAEMON=1`, each test store with no
daemon keeps its database on the cluster, one database per store path (`lane_postgres`).
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import shutil
import signal
import sqlite3
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import psycopg
import pytest
from psycopg import sql
from pydantic import SecretStr

from pynixd.config import PostgresSpec

if TYPE_CHECKING:
    from collections.abc import Iterator

_START_SECONDS = 30.0
_STOP_SECONDS = 30.0


@dataclass(frozen=True, slots=True)
class PostgresCluster:
    socket_dir: Path

    def conninfo(self, dbname: str = "postgres") -> str:
        return psycopg.conninfo.make_conninfo(host=str(self.socket_dir), dbname=dbname)


def _program(name: str) -> str:
    """*name* in `POSTGRES_BIN`, which the dev shell and the guest set; a missing server fails, never skips."""
    directory = os.environ.get("POSTGRES_BIN")
    if directory is None:
        raise RuntimeError("POSTGRES_BIN is unset: the dev shell and the test guests set it")
    return str(Path(directory) / name)


def _wait_ready(cluster: PostgresCluster, server: subprocess.Popen[bytes]) -> None:
    # The server sends no notice when it accepts connections, so this asks until it does.
    deadline = time.monotonic() + _START_SECONDS
    while True:
        if server.poll() is not None:
            raise RuntimeError(f"postgres exited with {server.returncode} before it accepted a connection")
        try:
            psycopg.connect(cluster.conninfo(), connect_timeout=2).close()
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.05)
        else:
            return


@contextlib.contextmanager
def running_cluster() -> Iterator[PostgresCluster]:
    # `mkdtemp`, not `tmp_path`: a unix socket path holds at most 107 bytes.
    root = Path(tempfile.mkdtemp(prefix="pynixd-pg-"))
    data = root / "data"
    cluster = PostgresCluster(socket_dir=root)
    made = subprocess.run(
        [
            _program("initdb"),
            "--pgdata",
            str(data),
            "--auth=trust",
            "--encoding=UTF8",
            # `C`, not `C.UTF-8`: a container guest has no libc locale but `C`. ICU collates either way.
            "--locale=C",
            "--locale-provider=icu",
            "--icu-locale=und",
            "--no-sync",
        ],
        check=False,
        capture_output=True,
    )
    if made.returncode != 0:
        raise RuntimeError(f"initdb exited with {made.returncode}: {made.stderr.decode(errors='replace')}")
    with (root / "server.log").open("wb") as server_log:
        server = subprocess.Popen(
            [
                _program("postgres"),
                "-D",
                str(data),
                "-k",
                str(root),
                "-c",
                "listen_addresses=",
                "-c",
                "fsync=off",
            ],
            stdout=subprocess.DEVNULL,
            stderr=server_log,
        )
    try:
        _wait_ready(cluster, server)
        yield cluster
    finally:
        # SIGINT is the fast shutdown: open sessions roll back and end.
        server.send_signal(signal.SIGINT)
        try:
            server.wait(timeout=_STOP_SECONDS)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
        shutil.rmtree(root, ignore_errors=True)


@pytest.fixture(scope="session")
def postgres_cluster() -> Iterator[PostgresCluster]:
    with running_cluster() as cluster:
        yield cluster


@pytest.fixture
def postgres_conninfo(postgres_cluster: PostgresCluster) -> Iterator[str]:
    """The conninfo of a new, empty database, dropped after the test."""
    name = f"test_{uuid.uuid4().hex}"
    with psycopg.connect(postgres_cluster.conninfo(), autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        yield postgres_cluster.conninfo(name)
    finally:
        with psycopg.connect(postgres_cluster.conninfo(), autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


@dataclass
class _Lane:
    cluster: PostgresCluster | None = None
    databases: dict[str, Path] = field(default_factory=dict)
    """Each lane database, and the store path it serves."""


_LANE = _Lane()


def lane_wanted() -> bool:
    return os.environ.get("PYNIXD_STORE_DATABASE") == "postgres"


@pytest.fixture(scope="session", autouse=True)
def postgres_lane() -> Iterator[None]:
    """The lane's cluster, for the whole session; nothing without `PYNIXD_STORE_DATABASE=postgres`."""
    if not lane_wanted():
        yield
        return
    with running_cluster() as cluster:
        _LANE.cluster = cluster
        try:
            yield
        finally:
            _LANE.cluster = None
            _LANE.databases.clear()


def lane_postgres(store_path: Path) -> PostgresSpec | None:
    """The lane's database for the store at *store_path*, made on first use; `None` outside the lane."""
    if not lane_wanted():
        return None
    cluster = _LANE.cluster
    if cluster is None:
        raise RuntimeError("PYNIXD_STORE_DATABASE=postgres, and the postgres_lane fixture has not started its cluster")
    name = "store_" + hashlib.sha256(str(store_path).encode()).hexdigest()[:32]
    if name not in _LANE.databases:
        with psycopg.connect(cluster.conninfo(), autocommit=True) as admin:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        _LANE.databases[name] = store_path
    return PostgresSpec(conninfo=SecretStr(cluster.conninfo(name)))


def drop_lane_databases_of_removed_stores() -> None:
    """A store whose files are gone loses its database too, so the next store at that path starts empty."""
    cluster = _LANE.cluster
    if cluster is None:
        return
    gone = [name for name, path in _LANE.databases.items() if not path.exists()]
    if not gone:
        return
    with psycopg.connect(cluster.conninfo(), autocommit=True) as admin:
        for name in gone:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
            del _LANE.databases[name]


def store_rows(db_file: Path, query: str) -> list[tuple[Any, ...]]:
    """The rows of *query* in the database of the store whose SQLite file is *db_file*.

    The lane's database when a lane store's root holds *db_file*, else the file itself, read-only. *query* binds no
    parameter: it runs as it is on either engine.
    """
    cluster = _LANE.cluster
    if cluster is not None:
        for name, root in _LANE.databases.items():
            if db_file.is_relative_to(root):
                with psycopg.connect(cluster.conninfo(name), autocommit=True) as server:
                    return [tuple(row) for row in server.execute(query.encode()).fetchall()]
    with contextlib.closing(sqlite3.connect(f"file:{db_file}?mode=ro", uri=True)) as file:
        return file.execute(query).fetchall()
