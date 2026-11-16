"""A store spec that keeps its database on a PostgreSQL server (`LocalSocketStoreSpec.postgres`)."""

from __future__ import annotations

import contextlib
import os
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from pydantic import SecretStr, ValidationError

from nix_daemon_protocol.ids import StoreId
from pynixd import experiments
from pynixd.config import LocalSocketStoreSpec, PostgresSpec
from pynixd.store import open_postgres_db
from pynixd_store.create_store import open_local_store
from pynixd_store.database import DatabaseError
from pynixd_store.sql import POSTGRES
from pynixd_store.store_layout import StoreLayout

PASSWORD = "hunter2"
CONNINFO = f"host=/run/postgresql dbname=nix password={PASSWORD}"


def _spec(**update: Any) -> LocalSocketStoreSpec:
    return LocalSocketStoreSpec(
        **{
            "store_id": StoreId("local"),
            "store_path": Path("/s"),
            "daemon": False,
            "pynixd_writes": True,
            "postgres": {"conninfo": CONNINFO},
            **update,
        }
    )


@pytest.mark.parametrize(
    ("update", "message"),
    [
        ({"daemon": True}, "postgres needs daemon = false"),
        ({"overlay": {"lower_store": "/l", "upper_layer": "/u"}}, "postgres takes no overlay"),
        ({"read_only": True}, "postgres takes no read_only"),
    ],
)
def test_a_store_on_a_server_refuses_what_reads_sqlite(update: dict[str, Any], message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        _spec(**update)


def test_the_conninfo_stays_out_of_every_report() -> None:
    spec = _spec()
    rows = experiments.active(spec, "local")
    assert [row.value for row in rows if row.switch == "postgres"] == [
        {"conninfo": "**********", "max_conns": 8, "open_seconds": 30.0}
    ]
    for text in (repr(spec), str(spec), spec.model_dump_json(), repr(rows)):
        assert PASSWORD not in text


async def test_the_spec_opens_the_store_on_its_server(postgres_conninfo: str, tmp_path: Path) -> None:
    db = await open_postgres_db(
        PostgresSpec(conninfo=SecretStr(postgres_conninfo), max_conns=2), StoreLayout.chroot(tmp_path)
    )
    try:
        assert (db.active, db.schema.usable, db.db_path) == (True, True, None)
        assert db.sql is POSTGRES
    finally:
        await db.close()


@pytest.mark.parametrize("sqlite", [True, False])
def test_a_store_on_a_server_gets_no_sqlite_database(tmp_path: Path, sqlite: bool) -> None:
    """The directories and the big lock are the store's own; the database is the server's."""
    layout = StoreLayout.chroot(tmp_path)
    os.close(open_local_store(layout, {}, "2.35.0", sqlite=sqlite))
    db_dir = layout.db_path.parent
    assert (db_dir / "big-lock").exists()
    assert layout.real_store_dir.is_dir()
    assert layout.db_path.exists() is sqlite
    assert (db_dir / "schema").exists() is sqlite


@pytest.mark.parametrize("paths", [0, 1])
async def test_a_store_whose_sqlite_holds_paths_does_not_open_on_an_empty_server(
    postgres_conninfo: str, tmp_path: Path, paths: int
) -> None:
    """A collection on the empty server database would find each path that Nix registered dead."""
    layout = StoreLayout.chroot(tmp_path)
    os.close(open_local_store(layout, {}, "2.35.0"))
    with contextlib.closing(sqlite3.connect(layout.db_path)) as file, file:
        for n in range(paths):
            file.execute(
                "INSERT INTO ValidPaths (path, hash, registrationTime) VALUES (?, ?, 0)",
                (f"/nix/store/{'a' * 32}-p{n}", "sha256:" + "0" * 64),
            )
    spec = PostgresSpec(conninfo=SecretStr(postgres_conninfo), max_conns=2)
    if paths:
        with pytest.raises(DatabaseError, match="holds valid paths and the PostgreSQL database holds none"):
            await open_postgres_db(spec, layout)
        return
    db = await open_postgres_db(spec, layout)
    await db.close()
