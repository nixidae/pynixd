"""Registration of valid paths in Nix's own tables: `LocalStore::registerValidPaths`.

`prepare` checks one path the way `addValidPath` (`local-store.cc:671` of Nix
2.34) does before it writes, and reads no table. `register_valid_paths` then
writes every prepared path in one transaction (`local-store.cc:896`).
"""

from __future__ import annotations

import base64
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import anyio

from nix_daemon_protocol.derivation_output import DerivationOutput, OutputKind
from nix_daemon_protocol.signature import Signature

from .content_address import (
    ContentAddress,
    content_addressed_path,
    fixed_output_address,
    is_content_addressed,
    parse_any_sha256,
    store_path_name,
)
from .drv_hash import make_output_path, output_hashes, output_path_name
from .drv_parser import BUILDER_RPC, read_drv_file
from .errors import PynixdError
from .hintfmt import magenta, missing_feature
from .queries import (
    ADD_DERIVATION_OUTPUT,
    ADD_REFERENCE,
    INVALIDATE_PATH,
    QUERY_PATH_INFO_FOR_UPDATE,
    QUERY_REALISATION_BY_HASH,
    QUERY_REALISATION_BY_PATH,
    QUERY_VALID_PATH_ID,
    REGISTER_BUILD_TRACE_BY_PATH,
    REGISTER_REALISATION_BY_HASH,
    UPDATE_BUILD_TRACE_BY_PATH,
    UPDATE_PATH_INFO,
    UPDATE_REALISATION_BY_HASH,
)

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable, Mapping

    import aiosqlite

    from nix_daemon_protocol.valid_path_info import ValidPathInfo

    from .drv_parser import Derivation
    from .queries import PathInfoStatements
    from .store_layout import StoreLayout

_DRV_EXTENSION = ".drv"


class RegistrationError(PynixdError):
    """Nix refuses this registration, and the message is Nix's."""


@dataclass(frozen=True)
class Registration:
    """One row of `ValidPaths` as Nix binds it, with its references and derivation outputs."""

    path: str
    nar_hash: str
    registration_time: int
    deriver: str | None
    nar_size: int | None
    ultimate: int | None
    sigs: str | None
    ca: str | None
    references: frozenset[str]
    derivation_outputs: Mapping[str, str]
    provenance: str | None = None


def _signatures(sigs: Iterable[Signature]) -> str | None:
    """`Signature::toStrings` of a `std::set<Signature>`: ordered by key name, then by the decoded bytes."""
    ordered = sorted(sigs, key=lambda sig: (sig.name, base64.b64decode(sig.signature)))
    return " ".join(sig.to_str() for sig in ordered) or None


async def prepare(info: ValidPathInfo, layout: StoreLayout, features: Collection[str]) -> Registration:
    """What `addValidPath` would write for *info*, or `RegistrationError` where Nix refuses."""
    path = str(info.path)
    store_dir = str(layout.store_dir)
    unkeyed = info.info
    # `registerValidPaths` asserts SHA-256 (`local-store.cc:910`) and writes Base16. A daemon's
    # wire carries Base16 and a narinfo Nix32; `Hash::parseAny` reads both (`worker-protocol.cc:372`).
    try:
        nar_hash = parse_any_sha256(str(unkeyed.nar_hash)).hex()
    except ValueError as error:
        raise RegistrationError(
            f"path '{magenta(path)}' has a NAR hash that is not SHA-256: {magenta(unkeyed.nar_hash)}"
        ) from error
    references = frozenset(str(ref) for ref in unkeyed.references)

    ca: str | None = None
    if unkeyed.ca:
        if not is_content_addressed(path, str(unkeyed.ca), references, store_dir):
            raise RegistrationError(
                f"cannot add path '{magenta(path)}' to the Nix store because it claims to be content-addressed but isn't"
            )
        ca = ContentAddress.parse(str(unkeyed.ca)).render()

    outputs: dict[str, str] = {}
    if path.endswith(_DRV_EXTENSION):
        outputs = await _derivation_outputs(path, layout, features)

    deriver = str(unkeyed.deriver) if unkeyed.deriver else None
    return Registration(
        path=path,
        nar_hash=f"sha256:{nar_hash}",
        registration_time=unkeyed.registration_time.ts or int(time.time()),
        deriver=deriver or None,
        nar_size=unkeyed.nar_size or None,
        ultimate=1 if unkeyed.ultimate else None,
        sigs=_signatures(unkeyed.sigs),
        ca=ca,
        references=references,
        derivation_outputs=outputs,
        provenance=unkeyed.provenance or None,
    )


async def _derivation_outputs(drv_path: str, layout: StoreLayout, features: Collection[str]) -> dict[str, str]:
    """`readInvalidDerivation`, `checkInvariants` and `outputsAndOptPaths` (`local-store.cc:731`).

    Only an output with a path gets a row: a floating, deferred or impure one has none. The parse requires
    the store's own *features*.
    """
    parsed = await read_drv_file(drv_path, layout)
    if parsed is None:
        raise RegistrationError(f"getting status of '{magenta(drv_path)}': No such file or directory")
    # The name is the path's, never `env["name"]`: `parseDerivation` takes it from the path (`derivations.cc:444`),
    # so the name check of `checkInvariants` cannot fail here. A structured-attrs derivation has no `env["name"]`.
    drv_name = store_path_name(drv_path).removesuffix(_DRV_EXTENSION)
    store_dir = str(layout.store_dir)
    if (missing := _missing_feature(parsed, drv_name, features)) is not None:
        raise RegistrationError(missing)
    _check_output_types(parsed)
    # `checkInvariants` of Nix master (`derivations.cc:381`, `:397`): a builder-rpc-v0 builder sees no output path,
    # so no variable names one, and only a content-addressed derivation may use it.
    rpc_outputs = BUILDER_RPC in parsed.required_system_features
    kinds = [DerivationOutput(path=o.path, method=o.hash_algo, hash_digest=o.hash_value).kind for o in parsed.outputs]
    if rpc_outputs and OutputKind.INPUT_ADDRESSED in kinds:
        raise RegistrationError(
            f"derivation uses the '{magenta(BUILDER_RPC)}' feature, which may only be used with content-addressing "
            "derivations"
        )

    async def read(path: str) -> Derivation | None:
        return await read_drv_file(path, layout)

    hashes: dict[str, str] | None = None
    outputs: dict[str, str] = {}
    for output in parsed.outputs:
        kind = DerivationOutput(path=output.path, method=output.hash_algo, hash_digest=output.hash_value).kind
        if kind is OutputKind.INPUT_ADDRESSED:
            if hashes is None:
                hashes = await output_hashes(parsed, read)
                if hashes is None:
                    raise RegistrationError(f"cannot hash the inputs of derivation '{magenta(drv_path)}'")
            digest = hashes.get(output.name)
            if digest is None:
                raise RegistrationError(f"derivation produced no hash for output '{magenta(output.name)}'")
            expected = make_output_path(output.name, bytes.fromhex(digest), drv_name, store_dir)
            if output.path != expected:
                raise RegistrationError(
                    f"derivation has incorrect output '{magenta(output.path)}', should be '{magenta(expected)}'"
                )
        elif kind is OutputKind.CA_FIXED:
            address = fixed_output_address(output.hash_algo, output.hash_value)
            expected = content_addressed_path(address, output_path_name(drv_name, output.name), (), store_dir)
        else:
            continue
        outputs[output.name] = expected
        if rpc_outputs:
            continue
        actual = parsed.env.get(output.name)
        if actual is None:
            raise RegistrationError(
                f"derivation has missing environment variable '{magenta(output.name)}', "
                f"should be '{magenta(expected)}' but is not present"
            )
        if actual != expected:
            raise RegistrationError(
                f"derivation has incorrect environment variable '{magenta(output.name)}', "
                f"should be '{magenta(expected)}' but is actually '{magenta(actual)}'"
            )
    return outputs


def _missing_feature(parsed: Derivation, drv_name: str, features: Collection[str]) -> str | None:
    """The first feature `parseDerivation` requires and *features* lacks (`derivations.cc:459`, `:322`)."""
    if parsed.is_dynamic and "dynamic-derivations" not in features:
        return missing_feature("dynamic-derivations", f"derivation '{drv_name}', ATerm format version 'xp-dyn-drv'")
    for output in parsed.outputs:
        if not output.hash_algo:
            continue
        # `ContentAddressMethod::parsePrefix` (`content-address.cc:88`) comes first.
        if output.hash_algo.startswith("git:") and "git-hashing" not in features:
            return missing_feature("git-hashing")
        if output.hash_algo.startswith("text:") and "dynamic-derivations" not in features:
            return missing_feature("dynamic-derivations", "text-hashed derivation output")
        if output.hash_value == "impure":
            if "impure-derivations" not in features:
                return missing_feature("impure-derivations")
        elif not output.hash_value and "ca-derivations" not in features:
            return missing_feature("ca-derivations")
    return None


def _check_output_types(parsed: Derivation) -> None:
    """`BasicDerivation::type` (`derivations.cc:800`): the combinations of output kinds Nix accepts."""
    if not parsed.outputs:
        raise RegistrationError("must have at least one output")
    kinds = parsed.output_kinds()
    if len(set(kinds)) > 1:
        raise RegistrationError("can't mix derivation output types")
    if kinds[0] is OutputKind.CA_FIXED:
        if len(parsed.outputs) > 1:
            raise RegistrationError("only one fixed output is allowed for now")
        if parsed.outputs[0].name != "out":
            raise RegistrationError('single fixed output must be named "out"')
    if kinds[0] is OutputKind.CA_FLOATING and len({o.hash_algo for o in parsed.outputs}) > 1:
        raise RegistrationError("all floating outputs must use the same hash algorithm")


async def _path_id(conn: aiosqlite.Connection, path: str) -> int | None:
    async with conn.execute(QUERY_VALID_PATH_ID, (path,)) as cursor:
        row = await cursor.fetchone()
    return None if row is None else int(row[0])


def _cycle(rows: Mapping[str, Registration]) -> tuple[str, str] | None:
    """`topoSort` (`topo-sort.hh`): the first `(path, parent)` that closes a cycle, in Nix's order."""
    visited: set[str] = set()
    parents: set[str] = set()

    def visit(path: str, parent: str | None) -> tuple[str, str] | None:
        if path in parents:
            return (path, parent or "")
        if path in visited:
            return None
        visited.add(path)
        parents.add(path)
        for child in sorted(rows[path].references, key=_base_name):
            if child != path and child in rows:
                found = visit(child, path)
                if found is not None:
                    return found
        parents.discard(path)
        return None

    for path in sorted(rows, key=_base_name):
        found = visit(path, None)
        if found is not None:
            return found
    return None


def _base_name(path: str) -> str:
    """`StorePath` orders by its base name."""
    return path.rsplit("/", 1)[-1]


async def register_valid_paths(
    conn: aiosqlite.Connection, registrations: Iterable[Registration], statements: PathInfoStatements
) -> None:
    """Write *registrations* in one transaction, as `registerValidPaths` does.

    The caller holds the connection and the right to write; a refusal rolls
    every row back.
    """
    rows = {row.path: row for row in registrations}
    order = sorted(rows, key=_base_name)
    # Nix's connection enforces the foreign keys of `Refs` (`openDB`); a pooled one does not by default.
    await conn.execute("PRAGMA foreign_keys = 1")
    # IMMEDIATE takes the write lock at once. A deferred transaction that
    # upgrades in WAL mode can fail with SQLITE_BUSY_SNAPSHOT without waiting.
    await conn.execute("BEGIN IMMEDIATE")
    try:
        for path in order:
            row = rows[path]
            if await _path_id(conn, path) is not None:
                await conn.execute(UPDATE_PATH_INFO, (row.nar_size, row.nar_hash, row.ultimate, row.sigs, row.ca, path))
                continue
            values: tuple[object, ...] = (
                path,
                row.nar_hash,
                row.registration_time,
                row.deriver,
                row.nar_size,
                row.ultimate,
                row.sigs,
                row.ca,
            )
            # A table without the column drops the provenance, as Determinate Nix does without the experimental feature.
            if statements.provenance:
                values += (row.provenance,)
            cursor = await conn.execute(statements.register_valid_path, values)
            drv_id = cursor.lastrowid
            await cursor.close()
            for name, output in sorted(row.derivation_outputs.items()):
                await conn.execute(ADD_DERIVATION_OUTPUT, (drv_id, name, output))
        for path in order:
            referrer = await _path_id(conn, path)
            for reference in sorted(rows[path].references, key=_base_name):
                target = await _path_id(conn, reference)
                if target is None:
                    raise RegistrationError(f"path '{magenta(reference)}' is not valid")
                await conn.execute(ADD_REFERENCE, (referrer, target))
        cycle = _cycle(rows)
        if cycle is not None:
            raise RegistrationError(
                f"cycle detected in the references of '{magenta(cycle[0])}' from '{magenta(cycle[1])}'"
            )
        await conn.commit()
    except BaseException:
        with anyio.CancelScope(shield=True):
            await conn.rollback()
        raise


async def add_signatures(conn: aiosqlite.Connection, path: str, sigs: Iterable[Signature]) -> bool:
    """Merge *sigs* into the signature set of *path*, as `LocalStore::addSignatures` (`local-store.cc:1583`) does.

    Return `False` for a path that is not valid, where Nix dereferences a null
    pointer. The caller decides what to answer. `_signatures` reorders the
    union the way `Signature::toStrings` of Nix's `std::set` does, so the
    stored string is the byte Nix would write.
    """
    await conn.execute("BEGIN IMMEDIATE")
    try:
        async with conn.execute(QUERY_PATH_INFO_FOR_UPDATE, (path,)) as cursor:
            row = await cursor.fetchone()
        if row is None:
            await conn.rollback()
            return False
        nar_size, nar_hash, ultimate, existing, ca = row
        merged = {Signature(sig) for sig in (existing or "").split()} | set(sigs)
        await conn.execute(UPDATE_PATH_INFO, (nar_size, nar_hash, ultimate, _signatures(merged), ca, path))
        await conn.commit()
    except BaseException:
        with anyio.CancelScope(shield=True):
            await conn.rollback()
        raise
    return True


async def invalidate_path(conn: aiosqlite.Connection, path: str) -> None:
    """`LocalStore::invalidatePath` (`local-store.cc:1016`): remove *path* from the valid-paths table.

    `verifyStore` calls it for a path the store directory no longer holds and
    whose referrers it can invalidate too. The `Refs` foreign keys cascade the
    path's referrer entries away, so the connection enforces them, as Nix's
    `openDB` does and a pooled connection does not.
    """
    await conn.execute("PRAGMA foreign_keys = 1")
    await conn.execute("BEGIN IMMEDIATE")
    try:
        await conn.execute(INVALIDATE_PATH, (path,))
        await conn.commit()
    except BaseException:
        with anyio.CancelScope(shield=True):
            await conn.rollback()
        raise


async def update_path_hashes(conn: aiosqlite.Connection, path: str, *, nar_hash: str, nar_size: int) -> None:
    """`verifyStore`'s fix-up of a missing hash or size (`local-store.cc:1444`): write them, keep the rest.

    Nix calls `updatePathInfo` with the whole record after it fills a null
    `narHash` or a zero `narSize`. This reads the stored row for update, as
    `addSignatures` does, so the signatures, the content address and the
    ultimate flag stay the bytes Nix wrote.
    """
    await conn.execute("BEGIN IMMEDIATE")
    try:
        async with conn.execute(QUERY_PATH_INFO_FOR_UPDATE, (path,)) as cursor:
            row = await cursor.fetchone()
        if row is None:
            await conn.rollback()
            return
        _nar_size, _hash, ultimate, sigs, ca = row
        await conn.execute(UPDATE_PATH_INFO, (nar_size, nar_hash, ultimate, sigs, ca, path))
        await conn.commit()
    except BaseException:
        with anyio.CancelScope(shield=True):
            await conn.rollback()
        raise


async def register_drv_output(
    conn: aiosqlite.Connection,
    drv_key: str,
    output_name: str,
    out_path: str,
    sigs: Iterable[Signature],
    *,
    by_path: bool,
) -> None:
    """Record one build-trace entry, as `LocalStore::registerDrvOutput` (`local-store.cc:664`) does.

    *drv_key* keys the table this store's Nix reads: with *by_path* the
    derivation's base name in `BuildTraceV3`, else the derivation's hash
    (`sha256:<hex>`) in `Realisations`. An id already present with the same
    output path merges the signatures in Nix's set order; one with another
    output path raises Nix's own message. `_signatures` orders the set the way
    `Signature::toStrings` does; the empty set writes `""`, not NULL.
    """
    select = QUERY_REALISATION_BY_PATH if by_path else QUERY_REALISATION_BY_HASH
    update = UPDATE_BUILD_TRACE_BY_PATH if by_path else UPDATE_REALISATION_BY_HASH
    insert = REGISTER_BUILD_TRACE_BY_PATH if by_path else REGISTER_REALISATION_BY_HASH
    await conn.execute("BEGIN IMMEDIATE")
    try:
        async with conn.execute(select, (drv_key, output_name)) as cursor:
            row = await cursor.fetchone()
        if row is None:
            await conn.execute(insert, (drv_key, output_name, out_path, _signatures(sigs) or ""))
        elif row[0] != out_path:
            separator = "^" if by_path else "!"
            raise RegistrationError(
                f"Trying to register a realisation of '{magenta(f'{drv_key}{separator}{output_name}')}', "
                "but we already have another one locally.\n"
                f"Local:  {magenta(row[0])}\n"
                f"Remote: {magenta(out_path)}"
            )
        else:
            merged = {Signature(sig) for sig in (row[1] or "").split()} | set(sigs)
            await conn.execute(update, (_signatures(merged) or "", drv_key, output_name))
        await conn.commit()
    except BaseException:
        with anyio.CancelScope(shield=True):
            await conn.rollback()
        raise
