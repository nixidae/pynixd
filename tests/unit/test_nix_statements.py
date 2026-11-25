"""Nix's statements (`pynixd_store/sql/nix.py`) on both engines: one store, the same answers.

`test_postgres_statements.py` has the server's parse of each statement.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from nix_daemon_protocol.signature import Signature
from pynixd_store import register

if TYPE_CHECKING:
    from pynixd_store.local_store_db import LocalStoreDB

LIBC = "/nix/store/00000000000000000000000000000001-libc"
HELLO = "/nix/store/00000000000000000000000000000002-hello"
DRV = "/nix/store/00000000000000000000000000000003-hello.drv"
ABSENT = "/nix/store/00000000000000000000000000000009-absent"
HASH = bytes([0x11]) * 32
OTHER_HASH = bytes([0x22]) * 32
SIG_A = Signature("cache-a:YWJj")
SIG_B = Signature("cache-b:ZGVm")


def _registration(
    path: str, references: frozenset[str] = frozenset(), outputs: dict[str, str] | None = None
) -> register.Registration:
    return register.Registration(
        path=path,
        nar_hash=HASH,
        registration_time=1_700_000_000,
        deriver=DRV if path == HELLO else None,
        nar_size=1 << 33,
        ultimate=path == HELLO,
        sigs=(SIG_B.to_str(),) if path == HELLO else (),
        ca=None,
        references=references,
        derivation_outputs=outputs or {},
        provenance='{"type":"test"}' if path == HELLO else None,
    )


async def _store(db: LocalStoreDB) -> None:
    await db.nix.register_valid_paths(
        [
            _registration(LIBC),
            _registration(HELLO, frozenset({LIBC, HELLO})),
            _registration(DRV, outputs={"out": HELLO}),
        ]
    )


async def test_the_reads_answer_what_the_registration_wrote(store_db: LocalStoreDB) -> None:
    await _store(store_db)
    assert store_db.sql.nix.provenance

    info = await store_db.nix.path_info(HELLO)
    assert info is not None
    assert {str(r) for r in info.info.references} == {LIBC, HELLO}
    assert (str(info.info.deriver), info.info.nar_size, info.info.ultimate) == (DRV, 1 << 33, True)
    assert info.info.provenance == '{"type":"test"}'
    assert await store_db.nix.path_info(ABSENT) is None

    assert await store_db.nix.is_valid_path(LIBC)
    assert not await store_db.nix.is_valid_path(ABSENT)
    assert {str(p) for p in await store_db.nix.all_valid_paths()} == {LIBC, HELLO, DRV}
    assert {str(p) for p in await store_db.nix.valid_paths([LIBC, ABSENT])} == {LIBC}
    assert {str(p) for p in await store_db.nix.referrers(LIBC)} == {HELLO}
    assert {str(p) for p in await store_db.nix.valid_derivers(HELLO)} == {DRV}
    assert {str(p) for p in await store_db.nix.closure([HELLO])} == {LIBC, HELLO}
    sizes = await store_db.nix.closure_sizes([HELLO])
    assert {str(p): size for p, size in sizes.items()}.keys() == {LIBC, HELLO}
    assert {str(p): size for p, size in sizes.items()}[HELLO] == 1 << 33
    assert await store_db.nix.closure_sizes([ABSENT]) == {}
    closure = await store_db.nix.closure_with_info([HELLO])
    assert [str(i.path) for i in closure] == [LIBC, HELLO]
    assert {str(r) for r in closure[1].info.references} == {LIBC, HELLO}
    infos = {str(i.path): i for i in await store_db.nix.path_infos([HELLO, LIBC, ABSENT])}
    assert infos.keys() == {LIBC, HELLO}
    assert {str(r) for r in infos[HELLO].info.references} == {LIBC, HELLO}

    outputs = await store_db.nix.derivation_outputs(DRV)
    assert outputs is not None
    assert {name: str(path) for name, path in outputs.items()} == {"out": HELLO}
    assert await store_db.nix.derivation_outputs(LIBC) == {}
    assert await store_db.nix.derivation_outputs(ABSENT) is None
    maps = await store_db.nix.derivation_output_maps([DRV, LIBC])
    assert {str(d): {n: str(p) for n, p in m.items()} for d, m in maps.items()} == {DRV: {"out": HELLO}}

    lower = "/nix/store/00000000000000000000000000000002"
    assert str(await store_db.nix.path_in_range(lower, lower + "\x7f")) == HELLO
    assert await store_db.nix.path_in_range(lower + "-z", lower + "\x7f") is None
    assert await store_db.nix.path_from_nar_hash(HASH) is not None
    assert await store_db.nix.path_from_nar_hash(OTHER_HASH) is None


async def test_the_writes_change_what_the_reads_answer(store_db: LocalStoreDB) -> None:
    await _store(store_db)
    await _store(store_db)
    assert len(await store_db.nix.all_valid_paths()) == 3

    assert await store_db.nix.add_signatures(HELLO, [SIG_A])
    assert not await store_db.nix.add_signatures(ABSENT, [SIG_A])
    info = await store_db.nix.path_info(HELLO)
    assert info is not None
    assert {sig.to_str() for sig in info.info.sigs} == {SIG_A.to_str(), SIG_B.to_str()}

    await store_db.nix.update_path_hashes(LIBC, nar_hash=OTHER_HASH, nar_size=7)
    libc = await store_db.nix.path_info(LIBC)
    assert libc is not None
    assert (libc.info.nar_hash.hash, libc.info.nar_size) == (OTHER_HASH.hex(), 7)

    trace = DRV.removeprefix("/nix/store/")
    await store_db.nix.register_drv_output(trace, "out", HELLO, [SIG_B], by_path=True)
    await store_db.nix.register_drv_output(trace, "out", HELLO, [SIG_A], by_path=True)
    found = await store_db.nix.realisation(trace, "out", by_path=True)
    assert found is not None
    assert (str(found[0]), found[1]) == (HELLO, [SIG_A.to_str(), SIG_B.to_str()])
    assert await store_db.nix.realisation(trace, "dev", by_path=True) is None

    await store_db.nix.invalidate_path(DRV)
    assert not await store_db.nix.is_valid_path(DRV)
    assert await store_db.nix.valid_derivers(HELLO) == set()
