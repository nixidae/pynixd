"""A PostgreSQL server for the store tests on PostgreSQL (nixidae/nixidae#1209).

One cluster per test process, and a new database for each test. The server is a child of the
fixture, not of `pg_ctl`, so it ends with the session. It listens on a unix socket alone.

The cluster collates with ICU's root locale, not with `C`. Under `C` a path column that lacks
`COLLATE "C"` sorts the same as one that has it, and no test could see the difference.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import psycopg
import pytest
from psycopg import sql

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


@pytest.fixture(scope="session")
def postgres_cluster() -> Iterator[PostgresCluster]:
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
