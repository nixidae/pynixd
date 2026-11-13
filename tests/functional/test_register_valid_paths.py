"""pynixd registers a path in Nix's tables as Nix does (`LocalStore::registerValidPaths`).

Nix fills store A: a source path, a flat file, a text file, and derivations
with two outputs, a fixed output, and an input derivation. pynixd copies the
files into store B, a store Nix made, and registers the same path infos. The
rows of `ValidPaths`, `Refs` and `DerivationOutputs` must then be Nix's, and
`nix store verify` and `nix path-info` must read store B as they read store A.
"""

from __future__ import annotations

import io
import json
import re
import sqlite3
from contextlib import asynccontextmanager, closing
from typing import TYPE_CHECKING, Any

import anyio
import pytest

from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.protocol import Verbosity
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time
from pynixd import Server
from pynixd.serde import QueryPathInfoRequest, StorePath
from pynixd.store import LocalDBStore, LocalStore, Store
from pynixd_store.drv_parser import parse_drv
from pynixd_store.hintfmt import magenta
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.nar_fs import dump_path, restore_path
from pynixd_store.register import RegistrationError
from tests.conftest import NIX_BIN, make_test_spec

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

_EXPR = """
let
  src = builtins.toFile "src" "hello";
  base = derivation { name = "base"; system = "x86_64-linux"; builder = "/bin/sh"; args = [ src ]; };
in {
  two = derivation {
    name = "two"; system = "x86_64-linux"; builder = "/bin/sh"; outputs = [ "out" "doc" ];
    dep = base; args = [ src ];
  };
  fixed = derivation {
    name = "fixed"; system = "x86_64-linux"; builder = "/bin/sh";
    outputHashMode = "flat"; outputHashAlgo = "sha256";
    outputHash = "0000000000000000000000000000000000000000000000000000000000000000";
  };
}
"""

_VALID_PATHS = "select path, hash, registrationTime, deriver, narSize, ultimate, sigs, ca from ValidPaths order by path"
_REFS = (
    "select a.path, b.path from Refs join ValidPaths a on a.id = referrer join ValidPaths b on b.id = reference "
    "order by a.path, b.path"
)
_OUTPUTS = (
    "select v.path, d.id, d.path from DerivationOutputs d join ValidPaths v on v.id = d.drv order by v.path, d.id"
)


async def _nix(*args: str) -> str:
    result = await anyio.run_process([str(NIX_BIN), *args], check=True)
    return result.stdout.decode().strip()


def _db(root: Path) -> Path:
    return root / "nix" / "var" / "nix" / "db" / "db.sqlite"


def _rows(root: Path, query: str) -> list[tuple[Any, ...]]:
    with closing(sqlite3.connect(f"file:{_db(root)}?mode=ro", uri=True)) as conn:
        return conn.execute(query).fetchall()


def _infos(root: Path) -> list[ValidPathInfo]:
    """Every path of the store at *root*, as a client would send it: the wire's fields."""
    refs: dict[str, set[StorePath]] = {}
    for referrer, reference in _rows(root, _REFS):
        refs.setdefault(referrer, set()).add(StorePath(path=reference))
    infos = []
    for path, nar_hash, registered, deriver, nar_size, ultimate, sigs, ca in _rows(root, _VALID_PATHS):
        assert not sigs, "the lab paths carry no signature"
        info = UnkeyedValidPathInfo(
            deriver=StorePath(path=deriver or ""),
            nar_hash=NARHash(hash=nar_hash),
            references=refs.get(path, set()),
            registration_time=Time(ts=registered),
            nar_size=nar_size or 0,
            ultimate=bool(ultimate),
            sigs=set(),
            ca=ContentAddress(value=ca or ""),
        )
        infos.append(ValidPathInfo(path=StorePath(path=path), info=info))
    return infos


def _copy_files(source: Path, dest: Path, infos: list[ValidPathInfo]) -> None:
    for info in infos:
        name = str(info.path).rsplit("/", 1)[-1]
        nar = io.BytesIO(b"".join(dump_path(source / "nix" / "store" / name)))
        restore_path(dest / "nix" / "store" / name, nar.read)


@asynccontextmanager
async def _store(root: Path, **spec: Any) -> AsyncIterator[LocalDBStore]:
    store = LocalDBStore(make_test_spec(store_id="register", store_path=root, no_probe=True, **spec))
    async with await LocalStoreDB.open(store.layout) as store.engine.db:
        yield store


async def _lab(tmp_path: Path) -> tuple[Path, Path, list[ValidPathInfo]]:
    """Store A as Nix filled it, and store B as Nix made it, empty but for the files."""
    tree = tmp_path / "tree"
    (tree / "bin").mkdir(parents=True)
    (tree / "bin" / "hello").write_bytes(b"#!/bin/sh\necho hi\n")  # noqa: ASYNC240 -- test setup
    (tree / "bin" / "hello").chmod(0o755)
    flat = tmp_path / "flat"
    flat.write_bytes(b"flat\n")  # noqa: ASYNC240 -- test setup
    nix_a, nix_b = tmp_path / "a", tmp_path / "b"
    await _nix("store", "add-path", "--store", str(nix_a), str(tree))
    await _nix("store", "add-file", "--store", str(nix_a), str(flat))
    await _nix("eval", "--store", str(nix_a), "--json", "--expr", f"builtins.mapAttrs (_: d: d.drvPath) ({_EXPR})")
    await _nix("store", "info", "--store", str(nix_b))
    infos = _infos(nix_a)
    _copy_files(nix_a, nix_b, infos)
    return nix_a, nix_b, infos


async def test_pynixd_writes_the_rows_nix_writes(tmp_path: Path) -> None:
    nix_a, nix_b, infos = await _lab(tmp_path)
    assert any(str(info.path).endswith("-two.drv") for info in infos)

    async with _store(nix_b) as store:
        await store.register_valid_paths(infos)

    for query in (_VALID_PATHS, _REFS, _OUTPUTS):
        assert _rows(nix_b, query) == _rows(nix_a, query)
    assert len(_rows(nix_b, _OUTPUTS)) == 4, "`base`, the two outputs of `two`, and the fixed output"

    paths = [str(info.path) for info in infos]
    await _nix("store", "verify", "--store", str(nix_b), "--no-trust", *paths)
    shown_a = json.loads(await _nix("path-info", "--store", str(nix_a), "--json", *paths))
    shown_b = json.loads(await _nix("path-info", "--store", str(nix_b), "--json", *paths))
    assert shown_b == shown_a


async def test_a_second_registration_updates_the_row(tmp_path: Path) -> None:
    """`updatePathInfo` (`local-store.cc:776`): the path keeps its id, deriver and time."""
    nix_a, nix_b, infos = await _lab(tmp_path)
    async with _store(nix_b) as store:
        await store.register_valid_paths(infos)
        before = _rows(nix_b, "select id, path, registrationTime from ValidPaths order by path")
        await store.register_valid_paths(infos)
    assert _rows(nix_b, "select id, path, registrationTime from ValidPaths order by path") == before
    assert _rows(nix_b, _VALID_PATHS) == _rows(nix_a, _VALID_PATHS)


def _with(info: ValidPathInfo, **changes: Any) -> ValidPathInfo:
    return info.model_copy(update={"info": info.info.model_copy(update=changes)})


async def _refused(nix_b: Path, infos: list[ValidPathInfo], match: str) -> RegistrationError:
    async with _store(nix_b) as store:
        with pytest.raises(RegistrationError, match=match) as refused:
            await store.register_valid_paths(infos)
    assert _rows(nix_b, _VALID_PATHS) == [], "a refusal leaves no row"
    return refused.value


def _input_addressed(infos: list[ValidPathInfo], *suffixes: str) -> list[ValidPathInfo]:
    """The lab paths named by *suffixes*, without their address: a new reference would falsify it."""
    return [_with(info, ca=ContentAddress(value="")) for info in infos if str(info.path).endswith(suffixes)]


async def test_a_reference_that_is_not_valid_is_refused(tmp_path: Path) -> None:
    _, nix_b, infos = await _lab(tmp_path)
    [tree] = _input_addressed(infos, "-tree")
    gone = StorePath(path="/nix/store/00000000000000000000000000000000-gone")
    await _refused(nix_b, [_with(tree, references={gone})], "is not valid")


async def test_a_cycle_is_refused(tmp_path: Path) -> None:
    _, nix_b, infos = await _lab(tmp_path)
    one, two = _input_addressed(infos, "-tree", "-flat")
    await _refused(
        nix_b,
        [_with(one, references={two.path}), _with(two, references={one.path})],
        "cycle detected in the references",
    )


async def test_a_false_content_address_is_refused(tmp_path: Path) -> None:
    _, nix_b, infos = await _lab(tmp_path)
    source = next(info for info in infos if str(info.info.ca).startswith("fixed:r:"))
    flat = next(info for info in infos if str(info.info.ca).startswith("fixed:sha256:"))
    await _refused(nix_b, [_with(source, ca=flat.info.ca)], "claims to be content-addressed but isn't")


async def test_a_derivation_with_a_wrong_output_is_refused(tmp_path: Path) -> None:
    """`checkInvariants` (`derivations.cc:1384`) reads the file: the database cannot vouch for it.

    It traces the error with the derivation (`derivations.cc:1398`), and so does pynixd.
    """
    _, nix_b, infos = await _lab(tmp_path)
    two = next(info for info in infos if str(info.path).endswith("-two.drv"))
    drv = nix_b / "nix" / "store" / str(two.path).rsplit("/", 1)[-1]
    text = drv.read_text()  # noqa: ASYNC240 -- test setup
    doc = next(output.path for output in parse_drv(text).outputs if output.name == "doc")
    drv.chmod(0o644)
    drv.write_text(text.replace(doc, doc[:-1] + "x"))  # noqa: ASYNC240 -- test setup
    error = await _refused(nix_b, [_with(two, ca=ContentAddress(value=""))], "derivation has incorrect output")
    assert error.traces == (f"while checking derivation '{magenta(str(two.path))}'",)


async def test_a_deferred_output_needs_a_name_a_path_may_carry(tmp_path: Path) -> None:
    """`processDerivationOutputPaths` (`derivations.cc:1315`) makes the path a deferred output would have.

    The client suite found it (nixidae/nixidae#1249): pynixd skipped a deferred output and accepted the name.
    """
    _, nix_b, infos = await _lab(tmp_path)
    [template] = _input_addressed(infos, "-flat")
    name = "swga9llxp3gm8jblg04y5c5gb07ssxjb-.drv"
    (nix_b / "nix" / "store" / name).write_text(  # noqa: ASYNC240 -- test setup
        'Derive([("out","","","")],[],[],"x86_64-linux","/bin/sh",["-c","true"],'
        '[("builder","/bin/sh"),("name",""),("out",""),("system","x86_64-linux")])'
    )
    drv = template.model_copy(update={"path": StorePath(path=f"/nix/store/{name}")})
    error = await _refused(nix_b, [drv], "name must not be empty")
    assert error.traces == (f"while checking derivation '{magenta(str(drv.path))}'",)


_DEFERRED = (
    'Derive([("out","","","")],[],[],"x86_64-linux","/bin/sh",["-c","true"],'
    '[("builder","/bin/sh"),("name","x"),("out",""),("system","x86_64-linux")])'
)
_ANSI = re.compile(r"\x1b\[[0-9;]*m")


async def test_a_deferred_output_warns_as_nix_does(tmp_path: Path) -> None:
    """`processDerivationOutputPaths` warns at registration (`derivations.cc:1334`); pynixd sends the same text.

    The control is Nix registering the same `.drv`: `nix store add` reaches `addValidPath` and `checkInvariants`.
    """
    drv = tmp_path / "x.drv"
    drv.write_text(_DEFERRED)  # noqa: ASYNC240 -- test setup
    nix_a, nix_b = tmp_path / "a", tmp_path / "b"
    added = await anyio.run_process(
        [str(NIX_BIN), "store", "add", "--store", str(nix_a), "--mode", "text", str(drv)], check=True
    )
    nix_says = [line for line in added.stderr.decode().splitlines() if line]
    assert any("incorrect deferred output" in line for line in nix_says), nix_says
    await _nix("store", "info", "--store", str(nix_b))
    infos = _infos(nix_a)
    _copy_files(nix_a, nix_b, infos)

    reported: list[tuple[Verbosity, str]] = []

    async def report(level: Verbosity, text: str) -> None:
        reported.append((level, text))

    async with _store(nix_b) as store:
        await store.engine.register_valid_paths(infos, report)
    assert [level for level, _ in reported] == [Verbosity.WARN]
    assert _ANSI.sub("", reported[0][1]).splitlines() == [_ANSI.sub("", line) for line in nix_says]
    assert _rows(nix_b, _VALID_PATHS) == _rows(nix_a, _VALID_PATHS)


async def test_pynixd_writes_needs_the_opt_in(tmp_path: Path) -> None:
    _, nix_b, infos = await _lab(tmp_path)
    async with _store(nix_b, pynixd_writes=False) as store:
        with pytest.raises(RuntimeError, match="pynixd_writes is off"):
            await store.register_valid_paths(infos)
    assert _rows(nix_b, _VALID_PATHS) == []


async def test_a_pooled_worker_sees_a_registration_at_once(tmp_path: Path) -> None:
    """A daemon worker keeps no path info: `runDaemon` sets `pathInfoCacheSize = 0` (`daemon.cc:439` of 2.34).

    `Store::queryPathInfo` caches "not valid" for `narinfo-cache-negative-ttl`
    (`store-api.cc:665`), and a pooled worker serves many clients. Without
    that line, the worker would answer from before pynixd registered the
    path, so the pool would have to retire it after every write.
    """
    _, nix_b, infos = await _lab(tmp_path)
    plain = [info for info in infos if not str(info.path).endswith(".drv")]
    asked = QueryPathInfoRequest(path=plain[0].path)
    # The idle worker must outlive the registration: under the full suite's load the test took 12 s, past the
    # default 10 s `idle_ttl`, and the pool retired the worker it asserts on.
    spec = make_test_spec(
        store_id="local", store_path=nix_b, no_probe=True, op_routes={"QueryPathInfo": "daemon"}, idle_ttl=600
    )
    stores: dict[StoreId, Store] = {StoreId("local"): LocalDBStore(spec)}
    async with Server(stores=stores, ssh_port=None, http_port=None) as server:
        local = server.ctx.local_store
        assert isinstance(local, LocalDBStore)
        assert (await local.execute(asked, skip_probe=True)).valid is False
        daemon = local.daemon_client
        assert isinstance(daemon, LocalStore)
        [(worker, _)] = daemon.transfer_pool.idle_conns
        assert "QueryPathInfo" in " ".join(worker.op_log), worker.op_log
        await local.register_valid_paths(plain)
        assert (await local.execute(asked, skip_probe=True)).valid is True
        assert worker.op_log.count("QueryPathInfoRequest") == 2, "the same worker answers both"


_RPC_EXPR = """
derivation {
  name = "rpc"; system = "x86_64-linux"; builder = "/bin/sh"; requiredSystemFeatures = [ "builder-rpc-v0" ];
}
"""


async def test_an_input_addressed_builder_rpc_derivation_is_refused(tmp_path: Path) -> None:
    """`checkInvariants` of Nix master (`derivations.cc:397`). Nix 2.35 knows no builder-rpc-v0 and writes it."""
    nix_a, nix_b = tmp_path / "a", tmp_path / "b"
    await _nix("eval", "--store", str(nix_a), "--raw", "--expr", f"({_RPC_EXPR}).drvPath")
    await _nix("store", "info", "--store", str(nix_b))
    infos = _infos(nix_a)
    _copy_files(nix_a, nix_b, infos)
    await _refused(nix_b, infos, "may only be used with content-addressing derivations")
