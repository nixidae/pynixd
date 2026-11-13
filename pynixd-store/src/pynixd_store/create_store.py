"""A local store opened as Nix's `LocalStore` constructor opens one (`local-store.cc:122`).

A daemon makes its store when it starts. A store with no daemon under it gets
the same directories, database, schema and lock from pynixd. Nix 2.35 is the
reference: schema version 10 and its two migrations. A store of Nix 2.34 keeps
`IndexReferrer` and takes 2.34's one migration, whose realisations are keyed by
the derivation's hash.
"""

from __future__ import annotations

import contextlib
import fcntl
import grp
import os
import sqlite3
from typing import TYPE_CHECKING, Any

import structlog

from .errors import PynixdError
from .nix_config import nix_at_least

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from .store_layout import StoreLayout

log = structlog.get_logger(__name__)

#: `nixSchemaVersion` (`local-store.hh:26`).
SCHEMA_VERSION = 10

#: `schema.sql`, as Nix 2.35 ships it. SQLite keeps the text of each statement, comments in it too.
SCHEMA = """
create table if not exists ValidPaths (
    id               integer primary key autoincrement not null,
    path             text unique not null,
    hash             text not null, -- base16 representation
    registrationTime integer not null,
    deriver          text,
    narSize          integer,
    ultimate         integer, -- null implies "false"
    sigs             text, -- space-separated
    ca               text -- if not null, an assertion that the path is content-addressed; see ValidPathInfo
);

create table if not exists Refs (
    referrer  integer not null,
    reference integer not null,
    primary key (referrer, reference),
    foreign key (referrer) references ValidPaths(id) on delete cascade,
    foreign key (reference) references ValidPaths(id) on delete restrict
);

create index if not exists IndexReference on Refs(reference);

create trigger if not exists DeleteSelfRefs before delete on ValidPaths
  begin
    delete from Refs where referrer = old.id and reference = old.id;
  end;

create table if not exists DerivationOutputs (
    drv  integer not null,
    id   text not null, -- symbolic output id, usually "out"
    path text not null,
    primary key (drv, id),
    foreign key (drv) references ValidPaths(id) on delete cascade
);

create index if not exists IndexDerivationOutputs on DerivationOutputs(path);
"""

#: `ca-specific-schema.sql`, as Nix 2.35 ships it.
CA_SCHEMA = """
create table if not exists BuildTraceV3 (
    id integer primary key autoincrement not null,
    drvPath text not null,
    outputName text not null, -- symbolic output id, usually "out"
    outputPath text not null,
    signatures text -- space-separated list
);

create index if not exists IndexBuildTraceV3 on BuildTraceV3(drvPath, outputName);
"""

#: `ca-specific-schema.sql`, as Nix 2.34 ships it.
CA_SCHEMA_2_34 = """
-- Extension of the sql schema for content-addressing derivations.
-- Won't be loaded unless the experimental feature `ca-derivations`
-- is enabled

create table if not exists Realisations (
    id integer primary key autoincrement not null,
    drvPath text not null,
    outputName text not null, -- symbolic output id, usually "out"
    outputPath integer not null,
    signatures text, -- space-separated list
    foreign key (outputPath) references ValidPaths(id) on delete cascade
);

create index if not exists IndexRealisations on Realisations(drvPath, outputName);

-- We can end-up in a weird edge-case where a path depends on itself because
-- it’s an output of a CA derivation, that happens to be the same as one of its
-- dependencies.
-- In that case we have a dependency loop (path -> realisation1 -> realisation2
-- -> path) that we need to break by removing the dependencies between the
-- realisations
create trigger if not exists DeleteSelfRefsViaRealisations before delete on ValidPaths
  begin
    delete from RealisationsRefs where realisationReference in (
      select id from Realisations where outputPath = old.id
    );
  end;

create table if not exists RealisationsRefs (
    referrer integer not null,
    realisationReference integer,
    foreign key (referrer) references Realisations(id) on delete cascade,
    foreign key (realisationReference) references Realisations(id) on delete restrict
);
-- used by deletion trigger
create index if not exists IndexRealisationsRefsRealisationReference on RealisationsRefs(realisationReference);

-- used by QueryRealisationReferences
create index if not exists IndexRealisationsRefs on RealisationsRefs(referrer);
-- used by cascade deletion when ValidPaths is deleted
create index if not exists IndexRealisationsRefsOnOutputPath on Realisations(outputPath);
"""  # noqa: RUF001 -- Nix 2.34's file, byte for byte

#: The index of `schema.sql` in Nix 2.34 that Nix 2.35 drops (`20260309-drop-redundant-indexreferrer`).
INDEX_REFERRER_2_34 = "create index if not exists IndexReferrer on Refs(referrer);"

PROVENANCE_MIGRATION = "20241024-provenance"
PROVENANCE_COLUMN = "alter table ValidPaths add column provenance text"

_RESERVED_SIZE = 8 * 1024 * 1024  # `gc-reserved-space` (`local-settings.hh:46`)


def _value(config: Mapping[str, Any], name: str, default: Any) -> Any:
    entry = config.get(name)
    return default if entry is None else entry["value"]


def _migrations(config: Mapping[str, Any], nix_version: str) -> list[tuple[str, str]]:
    """`upgradeDBSchema` (`local-store.cc:582`): each migration a Nix of *nix_version* runs on this configuration."""
    features = _value(config, "experimental-features", ())
    ca = "ca-derivations" in features
    found: list[tuple[str, str]] = []
    if not nix_at_least(nix_version, 2, 35):
        if ca:
            found.append(("20220326-ca-derivations", CA_SCHEMA_2_34))
    else:
        if ca:
            found.append(("20251017-ca-derivations", CA_SCHEMA))
        found.append(("20260309-drop-redundant-indexreferrer", "drop index if exists IndexReferrer"))
    # Determinate Nix runs this one last, with its `provenance` experimental feature alone (`local-store.cc:679` of
    # v3.24.0). Another Nix names no such feature, so its configuration never holds it.
    if "provenance" in features:
        found.append((PROVENANCE_MIGRATION, PROVENANCE_COLUMN))
    return found


def _create_dirs(layout: StoreLayout, config: Mapping[str, Any]) -> None:
    real = layout.real_store_dir
    real.mkdir(parents=True, exist_ok=True)
    # NIX-DEVIATION (#27): `makeStoreWritable` (`local-store.cc:645`) remounts
    # a read-only store directory in a private mount namespace. pynixd shares
    # one mount namespace with every task it runs, so it refuses the store. To
    # reverse: a deployment with a read-only bind-mounted store.
    if not os.access(real, os.W_OK):
        raise PynixdError(f"pynixd cannot write the store directory {real}, and it remounts no read-only store")
    profiles, gcroots = layout.state_dir / "profiles", layout.state_dir / "gcroots"
    for directory in (layout.links_dir, profiles, layout.state_dir / "temproots", layout.db_path.parent, gcroots):
        directory.mkdir(parents=True, exist_ok=True)
    for per_user in (profiles / "per-user", gcroots / "per-user"):
        per_user.mkdir(exist_ok=True)
        # `chmodIfNeeded`: a user that may not change the mode still opens a store whose mode is right.
        if per_user.stat().st_mode & 0o777 != 0o755:
            per_user.chmod(0o755)
    group = _value(config, "build-users-group", "")
    if os.geteuid() == 0 and group:
        try:
            gid = grp.getgrnam(group).gr_gid
        except KeyError:
            log.error("build_users_group_missing", group=group)
        else:
            st = real.stat()
            if st.st_uid != 0 or st.st_gid != gid or st.st_mode & 0o7777 != 0o1775:
                os.chown(real, 0, gid)
                real.chmod(0o1775)
    if not _value(config, "allow-symlinked-store", False):
        for path in (real, *real.parents):
            if path.is_symlink():
                raise PynixdError(
                    f"the path '{path}' is a symlink; this is not allowed for the Nix store and its parent directories"
                )


def _reserve(path: Path, size: int) -> None:
    """The space a collection frees before it opens the database on a full disk."""
    with contextlib.suppress(OSError):
        if path.exists() and path.stat().st_size == size:
            return
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_CLOEXEC, 0o600)
        try:
            try:
                os.posix_fallocate(fd, 0, size)
            except OSError:
                os.write(fd, b"X" * size)
                os.ftruncate(fd, size)
        finally:
            os.close(fd)


def _schema(path: Path) -> int:
    """`getSchema` (`local-store.cc:490`)."""
    if not path.exists():
        return 0
    text = path.read_text()
    if not text.strip().isdigit():
        raise PynixdError(f"'{path}' is corrupt")
    return int(text)


def _open_db(layout: StoreLayout, config: Mapping[str, Any], nix_version: str, *, create: bool) -> sqlite3.Connection:
    """`openDB` (`local-store.cc:503`): the pragmas Nix sets, and the schema on *create*."""
    uri = f"file:{layout.db_path}" + ("" if create else "?mode=rw")
    db = sqlite3.connect(uri, uri=True, isolation_level=None)
    db.execute(f"pragma synchronous = {'normal' if _value(config, 'fsync-metadata', True) else 'off'}")
    mode = "wal" if _value(config, "use-sqlite-wal", True) else "truncate"
    if db.execute("pragma main.journal_mode").fetchone()[0] != mode:
        db.execute(f"pragma main.journal_mode = {mode}")
    if mode == "wal":
        # Nix also sets `SQLITE_FCNTL_PERSIST_WAL`, which Python's sqlite3 cannot reach.
        db.execute("pragma main.journal_size_limit = 1099511627776")
        db.execute("pragma wal_autocheckpoint = 40000")
    if create:
        db.executescript(SCHEMA)
        if not nix_at_least(nix_version, 2, 35):
            db.executescript(INDEX_REFERRER_2_34)
    return db


def _upgrade(db: sqlite3.Connection, config: Mapping[str, Any], nix_version: str) -> list[tuple[str, str]]:
    """The migrations of `upgradeDBSchema` this database lacks, with `SchemaMigrations` made."""
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='SchemaMigrations'").fetchone() is None:
        db.execute("create table SchemaMigrations (migration text primary key not null)")
    done = {row[0] for row in db.execute("select migration from SchemaMigrations")}
    return [(name, statement) for name, statement in _migrations(config, nix_version) if name not in done]


def open_local_store(layout: StoreLayout, config: Mapping[str, Any], nix_version: str) -> int:
    """Make what the store lacks, and return the big lock, held shared for as long as the store is open.

    *config* is `nix config show --json` of the store's Nix, and *nix_version* its version. It blocks: run it in a thread.
    """
    _create_dirs(layout, config)
    db_dir = layout.db_path.parent
    _reserve(db_dir / "reserved", int(_value(config, "gc-reserved-space", _RESERVED_SIZE)))
    lock = os.open(db_dir / "big-lock", os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
    try:
        fcntl.flock(lock, fcntl.LOCK_SH)
        schema_path = db_dir / "schema"
        current = _schema(schema_path)
        if current > SCHEMA_VERSION:
            raise PynixdError(f"current Nix store schema is version {current}, but I only support {SCHEMA_VERSION}")
        if 0 < current < SCHEMA_VERSION:
            # Nix upgrades such a store under the big lock; only a Nix older than 2.4 leaves one.
            raise PynixdError(f"pynixd opens no store of schema version {current}; open it once with Nix")
        db = _open_db(layout, config, nix_version, create=current == 0)
        try:
            if current == 0:
                _write_synced(schema_path, f"{SCHEMA_VERSION}".encode())
            if pending := _upgrade(db, config, nix_version):
                fcntl.flock(lock, fcntl.LOCK_EX)
                for name, statement in _upgrade(db, config, nix_version):
                    db.executescript(
                        f"begin;\n{statement};\ninsert or ignore into SchemaMigrations values('{name}');\ncommit;"
                    )
                fcntl.flock(lock, fcntl.LOCK_SH)
                log.debug("store_schema_migrated", migrations=[name for name, _ in pending])
        finally:
            db.close()
    except BaseException:
        os.close(lock)
        raise
    return lock


def check_read_only_store(layout: StoreLayout) -> None:
    """What `LocalStore`'s constructor asks of a read-only store (`local-store.cc:259-279`): a current schema.

    It creates no directory, takes no big lock, and writes nothing.
    """
    current = _schema(layout.db_path.parent / "schema")
    if current == 0:
        raise PynixdError("database does not exist, and cannot be created in read-only mode")
    if current < SCHEMA_VERSION:
        raise PynixdError("database schema needs migrating, but this cannot be done in read-only mode")
    if current > SCHEMA_VERSION:
        raise PynixdError(f"current Nix store schema is version {current}, but I only support {SCHEMA_VERSION}")


def _write_synced(path: Path, data: bytes) -> None:
    """`writeFile(path, data, 0666, FsSync::Yes)`: the umask applies, as it does to Nix's `open`."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_CLOEXEC, 0o666)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)
