"""Registration of valid paths in Nix's own tables: `LocalStore::registerValidPaths`.

`prepare` checks one path the way `addValidPath` (`local-store.cc:671` of Nix
2.34) does before it writes, and reads no table. `register_valid_paths` then
writes every prepared path in one transaction (`local-store.cc:896`).

Each write takes the session of one `LocalStoreDB.transaction`, which commits
or rolls back the whole function.
"""

from __future__ import annotations

import base64
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

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
from .hintfmt import WARNING_PREFIX, magenta, missing_feature

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable, Mapping

    from nix_daemon_protocol.valid_path_info import ValidPathInfo

    from .database import Session
    from .drv_parser import Derivation
    from .store_layout import StoreLayout

_DRV_EXTENSION = ".drv"


class RegistrationError(PynixdError):
    """Nix refuses this registration, and the message is Nix's."""


@dataclass(frozen=True)
class Registration:
    """One row of `ValidPaths` as Nix binds it, with its references and derivation outputs."""

    path: str
    #: The SHA-256 digest of the NAR.
    nar_hash: bytes
    registration_time: int
    deriver: str | None
    nar_size: int | None
    ultimate: bool
    sigs: tuple[str, ...]
    ca: str | None
    references: frozenset[str]
    derivation_outputs: Mapping[str, str]
    provenance: str | None = None
    #: What Nix's `warn` prints while it checks the path, in order, prefix and colours included.
    warnings: tuple[str, ...] = ()


def _signatures(sigs: Iterable[Signature]) -> tuple[str, ...]:
    """`Signature::toStrings` of a `std::set<Signature>`: ordered by key name, then by the decoded bytes."""
    ordered = sorted(sigs, key=lambda sig: (sig.name, base64.b64decode(sig.signature)))
    return tuple(sig.to_str() for sig in ordered)


async def prepare(info: ValidPathInfo, layout: StoreLayout, features: Collection[str]) -> Registration:
    """What `addValidPath` would write for *info*, or `RegistrationError` where Nix refuses."""
    path = str(info.path)
    store_dir = str(layout.store_dir)
    unkeyed = info.info
    # `registerValidPaths` asserts SHA-256 (`local-store.cc:910`) and writes Base16. A daemon's
    # wire carries Base16 and a narinfo Nix32; `Hash::parseAny` reads both (`worker-protocol.cc:372`).
    try:
        nar_hash = parse_any_sha256(str(unkeyed.nar_hash))
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
    warnings: list[str] = []
    if path.endswith(_DRV_EXTENSION):
        outputs = await _derivation_outputs(path, layout, features, warnings)

    deriver = str(unkeyed.deriver) if unkeyed.deriver else None
    return Registration(
        path=path,
        nar_hash=nar_hash,
        registration_time=unkeyed.registration_time.ts or int(time.time()),
        deriver=deriver or None,
        nar_size=unkeyed.nar_size or None,
        ultimate=bool(unkeyed.ultimate),
        sigs=_signatures(unkeyed.sigs),
        ca=ca,
        references=references,
        derivation_outputs=outputs,
        provenance=unkeyed.provenance or None,
        warnings=tuple(warnings),
    )


async def _derivation_outputs(
    drv_path: str, layout: StoreLayout, features: Collection[str], warnings: list[str]
) -> dict[str, str]:
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
    if (missing := _missing_feature(parsed, drv_name, features)) is not None:
        raise RegistrationError(missing)
    try:
        return await _check_invariants(parsed, drv_path, drv_name, layout, warnings)
    except RegistrationError as error:
        # `Derivation::checkInvariants(store, drvPath)` (`derivations.cc:1398`) traces every check below the name.
        raise error.with_trace(f"while checking derivation '{magenta(drv_path)}'") from None


async def _check_invariants(
    parsed: Derivation, drv_path: str, drv_name: str, layout: StoreLayout, warnings: list[str]
) -> dict[str, str]:
    """`Derivation::checkInvariants(store)`: the output types and paths, and the variable that names each."""
    store_dir = str(layout.store_dir)
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
        elif kind is OutputKind.DEFERRED:
            # With inputs that hash (`DrvHash`), Nix makes the path a deferred output would have, refuses a name
            # no path may carry, and warns that the output should have that path (`derivations.cc:1334`).
            # A deferred output needs no row.
            if hashes is None:
                hashes = await output_hashes(parsed, read)
            digest = None if hashes is None else hashes.get(output.name)
            if digest is not None:
                try:
                    should_be = make_output_path(output.name, bytes.fromhex(digest), drv_name, store_dir)
                except ValueError as error:
                    raise RegistrationError(str(error)) from None
                warnings.append(
                    f"{WARNING_PREFIX}derivation has incorrect deferred output, should be '{magenta(should_be)}'.\n"
                    "This will be an error in future versions of Nix; compatibility of CA derivations will be broken."
                )
            continue
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


async def _path_id(session: Session, path: str) -> int | None:
    row = await session.fetchone(session.sql.nix.valid_path_id, (path,))
    return None if row is None else int(row[0])


async def _path_ids(session: Session, paths: Iterable[str]) -> dict[str, int]:
    """The id of each valid path of *paths*."""
    found = await session.fetchall(session.sql.nix.valid_path_ids, (session.sql.codec.path_set(paths),))
    return {str(path): int(row_id) for path, row_id in found}


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


async def register_valid_paths(session: Session, registrations: Iterable[Registration]) -> None:
    """Write *registrations* in the transaction of *session*, as `registerValidPaths` does.

    A refusal raises, and the caller's transaction rolls every row back.
    """
    rows = {row.path: row for row in registrations}
    order = sorted(rows, key=_base_name)
    codec = session.sql.codec
    for path in order:
        row = rows[path]
        ultimate, sigs = codec.ultimate(row.ultimate), codec.path_signatures(row.sigs)
        nar_hash = codec.nar_hash(row.nar_hash)
        if await _path_id(session, path) is not None:
            await session.execute(
                session.sql.nix.update_path_info, (row.nar_size, nar_hash, ultimate, sigs, row.ca, path)
            )
            continue
        values: tuple[object, ...] = (
            path,
            nar_hash,
            row.registration_time,
            row.deriver,
            row.nar_size,
            ultimate,
            sigs,
            row.ca,
        )
        # A table without the column drops the provenance, as Determinate Nix does without the experimental feature.
        if session.sql.nix.provenance:
            values += (row.provenance,)
        drv_id = await session.insert(session.sql.nix.register_valid_path, values)
        for name, output in sorted(row.derivation_outputs.items()):
            await session.execute(session.sql.nix.add_derivation_output, (drv_id, name, output))
    # One statement for every id, and one for every row: a statement per reference costs a thread hop each, and a
    # derivation with 2000 inputs then took 0.33 s to register (nixidae/nixidae#1594).
    named = {path for row in rows.values() for path in (row.path, *row.references)}
    ids = await _path_ids(session, named)
    references: list[tuple[int, int]] = []
    for path in order:
        for reference in sorted(rows[path].references, key=_base_name):
            if reference not in ids:
                raise RegistrationError(f"path '{magenta(reference)}' is not valid")
            references.append((ids[path], ids[reference]))
    if references:
        await session.executemany(session.sql.nix.add_reference, references)
    cycle = _cycle(rows)
    if cycle is not None:
        raise RegistrationError(f"cycle detected in the references of '{magenta(cycle[0])}' from '{magenta(cycle[1])}'")


async def add_signatures(session: Session, path: str, sigs: Iterable[Signature]) -> bool:
    """Merge *sigs* into the signature set of *path*, as `LocalStore::addSignatures` (`local-store.cc:1583`) does.

    Return `False` for a path that is not valid, where Nix dereferences a null
    pointer. The caller decides what to answer. `_signatures` reorders the
    union the way `Signature::toStrings` of Nix's `std::set` does, so the
    stored string is the byte Nix would write.
    """
    row = await session.fetchone(session.sql.nix.path_info_for_update, (path,))
    if row is None:
        return False
    nar_size, nar_hash, ultimate, existing, ca = row
    codec = session.sql.codec
    merged = {Signature(sig) for sig in codec.strings(existing)} | set(sigs)
    await session.execute(
        session.sql.nix.update_path_info,
        (nar_size, nar_hash, ultimate, codec.path_signatures(_signatures(merged)), ca, path),
    )
    return True


async def invalidate_path(session: Session, path: str) -> None:
    """`LocalStore::invalidatePath` (`local-store.cc:1016`): remove *path* from the valid-paths table.

    `verifyStore` calls it for a path the store directory no longer holds and
    whose referrers it can invalidate too. The `Refs` foreign keys cascade the
    path's referrer entries away.
    """
    await session.execute(session.sql.nix.invalidate_path, (path,))


async def update_path_hashes(session: Session, path: str, *, nar_hash: bytes, nar_size: int) -> None:
    """`verifyStore`'s fix-up of a missing hash or size (`local-store.cc:1444`): write them, keep the rest.

    Nix calls `updatePathInfo` with the whole record after it fills a null
    `narHash` or a zero `narSize`. This reads the stored row for update, as
    `addSignatures` does, so the signatures, the content address and the
    ultimate flag stay the bytes Nix wrote.
    """
    row = await session.fetchone(session.sql.nix.path_info_for_update, (path,))
    if row is None:
        return
    _nar_size, _hash, ultimate, sigs, ca = row
    await session.execute(
        session.sql.nix.update_path_info, (nar_size, session.sql.codec.nar_hash(nar_hash), ultimate, sigs, ca, path)
    )


async def _require_valid_output(session: Session, realisation: str, out_path: str) -> None:
    if await session.fetchone(session.sql.nix.valid_path_id, (out_path,)) is None:
        raise RegistrationError(
            f"cannot register realisation '{magenta(realisation)}': path '{magenta(out_path)}' is not valid"
        )


async def register_drv_output(
    session: Session,
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
    `Signature::toStrings` does.

    NIX-DEFECT (#23): `LocalStore::registerDrvOutput` (`local-store.cc:653`, Nix 2.34) inserts the output
    path through `(select id from ValidPaths where path = ?)` without checking it, so an output path that
    is not valid fails the NOT NULL constraint and the client reads the raw SQLite statement and the
    database's file name. pynixd refuses that realisation first and names the path. Reported as
    Lillecarl/nix#423.
    """
    select = session.sql.nix.realisation_by_path if by_path else session.sql.nix.realisation_by_hash
    update = session.sql.nix.update_build_trace_by_path if by_path else session.sql.nix.update_realisation_by_hash
    insert = session.sql.nix.register_build_trace_by_path if by_path else session.sql.nix.register_realisation_by_hash
    codec = session.sql.codec
    row = await session.fetchone(select, (drv_key, output_name))
    if row is None:
        if not by_path:
            await _require_valid_output(session, f"{drv_key}!{output_name}", out_path)
        await session.execute(insert, (drv_key, output_name, out_path, codec.trace_signatures(_signatures(sigs))))
    elif row[0] != out_path:
        separator = "^" if by_path else "!"
        raise RegistrationError(
            f"Trying to register a realisation of '{magenta(f'{drv_key}{separator}{output_name}')}', "
            "but we already have another one locally.\n"
            f"Local:  {magenta(row[0])}\n"
            f"Remote: {magenta(out_path)}"
        )
    else:
        merged = {Signature(sig) for sig in codec.strings(row[1])} | set(sigs)
        await session.execute(update, (codec.trace_signatures(_signatures(merged)), drv_key, output_name))
