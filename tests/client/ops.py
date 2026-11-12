"""Store operations as data, the strategies that generate them, and one run of a list of them on both arms.

An operation names the paths it uses by index into the paths known so far.
Index 0 is a path no example ever adds, so a query or a reference can name an
absent path, and hypothesis shrinks a failing list to the fewest operations
with the smallest indices.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

import anyio
import huggorm  # type: ignore[import-not-found]  # the type gate has no huggorm; see default.nix
from huggorm.errors import NixError  # type: ignore[import-not-found]  # the type gate has no huggorm; see default.nix
from hypothesis import event, strategies as st

from pynixd_store.drv_hash import make_output_path

if TYPE_CHECKING:
    from pathlib import Path

    from huggorm import (  # type: ignore[import-not-found]  # the type gate has no huggorm; see default.nix
        AsyncStore,
        PathInfo,
        StorePath,
    )

STORE_URI = "<store>"
ABSENT = huggorm.StorePath("00000000000000000000000000000000-absent")
# What a store path name may hold (`nix/store/path.cc`, `checkName`), plus a few it may not.
NAME_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+-._?="
BAD_NAME_CHARS = "/ ~!"
METHODS = [huggorm.ContentAddressMethod.FLAT, huggorm.ContentAddressMethod.NAR, huggorm.ContentAddressMethod.TEXT]
# Whether the control parses a `.drv` as Nix master does, refusing a duplicate (NixOS/nix#16284). A lane's
# huggorm and its `nix daemon` are one release, so the client's own version answers for the daemon.
_VERSION = re.match(r"(\d+)\.(\d+)", huggorm.nix_version())
CANONICAL_ATERM = _VERSION is not None and (int(_VERSION[1]), int(_VERSION[2])) >= (2, 36)
# 2.34 keys a realisation by the derivation's hash modulo, 2.35 by its store path (`nix/store/realisation.hh`).
KEYED_BY_PATH = _VERSION is not None and (int(_VERSION[1]), int(_VERSION[2])) >= (2, 35)
OUTPUT_NAMES = ["out", "dev"]


class Query(StrEnum):
    VALID = "is_valid_path"
    INFO = "query_path_info"
    VALID_PATHS = "query_valid_paths"
    REFERRERS = "query_referrers"
    CLOSURE = "compute_fs_closure"
    REVERSE_CLOSURE = "compute_fs_closure(flip_direction=True)"
    HASH_PART = "query_path_from_hash_part"
    ALL_VALID = "query_all_valid_paths"
    DERIVERS = "query_valid_derivers"
    TOPO_SORT = "topo_sort_paths"
    OUTPUT_MAP = "query_derivation_output_map"
    MISSING = "query_missing"
    NAR = "nar_from_path"
    TRUSTED = "is_trusted_client"
    REALISATION = "query_realisation"


@dataclass(frozen=True, slots=True)
class Add:
    name: str
    data: bytes
    method: huggorm.ContentAddressMethod
    references: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class Ask:
    query: Query
    targets: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class Derive:
    """A `.drv` that names `inputs` as sources, written as `writeDerivation` writes one: text, with references."""

    name: str
    inputs: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class Import:
    """A NAR and its path info from another store, as `nix copy` and `nix-store --import` send them."""

    name: str
    data: bytes


@dataclass(frozen=True, slots=True)
class Realise:
    """Say that output `output` of `drv` turned out to be `out`, as a CA build or `nix copy` of one does."""

    drv: int
    output: str
    out: int


@dataclass(frozen=True, slots=True)
class Copy:
    """A path from another store, as `nix copy` sends it: `AddMultipleToStore`, and op 49 to Determinate."""

    name: str
    data: bytes


@dataclass(frozen=True, slots=True)
class Root:
    """A temporary root, held by the client's connection."""

    target: int


@dataclass(frozen=True, slots=True)
class Collect:
    """A garbage collection: the whole store's live or dead paths, or a delete of `targets`."""

    action: huggorm.GCAction
    targets: tuple[int, ...]


type Op = Add | Ask | Derive | Import | Realise | Copy | Root | Collect
# A plain value both arms can be compared on: strings, numbers, bools and tuples of them.
type Plain = str | int | bool | tuple[Plain, ...] | None


@dataclass(frozen=True, slots=True)
class Outcome:
    value: Plain = None
    error: tuple[str, str] | None = None


@dataclass(frozen=True, slots=True)
class Divergence:
    step: int
    op: Op
    control: Outcome
    pynixd: Outcome

    def describe(self) -> str:
        return f"step {self.step}: {self.op!r}\n  nix-daemon: {self.control!r}\n  pynixd:     {self.pynixd!r}"


# `one_of` picks a branch evenly, so three valid branches to one that may not be.
_valid_names = st.text(alphabet=NAME_CHARS, min_size=1, max_size=40)
names = st.one_of(_valid_names, _valid_names, _valid_names, st.text(alphabet=NAME_CHARS + BAD_NAME_CHARS, max_size=8))
# 0 is the absent path, about one pick in thirteen.
indices = st.integers(min_value=0, max_value=12)
adds = st.builds(
    Add,
    name=names,
    data=st.binary(max_size=256),
    method=st.sampled_from(METHODS),
    references=st.lists(indices, max_size=3).map(tuple),
)
asks = st.builds(Ask, query=st.sampled_from(Query), targets=st.lists(indices, min_size=1, max_size=3).map(tuple))
derives = st.builds(Derive, name=names, inputs=st.lists(indices, max_size=3).map(tuple))
imports = st.builds(Import, name=names, data=st.binary(max_size=256))
output_names = st.one_of(st.sampled_from(OUTPUT_NAMES), names)
realises = st.builds(Realise, drv=indices, output=output_names, out=indices)
copies = st.builds(Copy, name=names, data=st.binary(max_size=256))
roots = st.builds(Root, target=indices)
# Not `DELETE_DEAD`: it empties the store, and every later step then asks about nothing.
GC_ACTIONS = [huggorm.GCAction.RETURN_LIVE, huggorm.GCAction.RETURN_DEAD, huggorm.GCAction.DELETE_SPECIFIC]
collects = st.builds(
    Collect, action=st.sampled_from(GC_ACTIONS), targets=st.lists(indices, min_size=1, max_size=3).map(tuple)
)
operations = st.lists(
    st.one_of(adds, asks, derives, imports, realises, copies, roots, collects), min_size=1, max_size=12
)


class _Nar(huggorm.Sink):  # type: ignore[misc]  # the type gate has no huggorm; see default.nix
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[bytes] = []

    def write(self, data: bytes) -> None:
        self.parts.append(data)

    def nar(self) -> bytes:
        return b"".join(self.parts)


class _Bytes(huggorm.Source):  # type: ignore[misc]  # the type gate has no huggorm; see default.nix
    def __init__(self, data: bytes) -> None:
        super().__init__()
        self.data: bytes = data

    def read(self, n: int) -> bytes:
        out = self.data[:n]
        self.data = self.data[n:]
        return out


def _pick(known: list[StorePath], index: int) -> StorePath:
    """Index 0 is the absent path; any other index names one of the added paths, while there is one."""
    if index == 0 or len(known) == 1:
        return known[0]
    return known[1 + (index - 1) % (len(known) - 1)]


def _paths(paths: list[StorePath]) -> tuple[str, ...]:
    return tuple(sorted(str(path) for path in paths))


def _info(info: PathInfo) -> Plain:
    """Everything but `registration_time`, which is a clock reading."""
    deriver = info.deriver()
    ca = info.ca()
    return (
        str(info.path()),
        str(info.nar_hash()),
        info.nar_size(),
        None if deriver is None else str(deriver),
        None if ca is None else str(ca),
        info.ultimate(),
        _paths(info.references()),
        tuple(sorted(str(sig) for sig in info.sigs())),
    )


async def _ask(store: AsyncStore, ask: Ask, known: list[StorePath]) -> Plain:
    targets = [_pick(known, index) for index in ask.targets]
    first = targets[0]
    match ask.query:
        case Query.VALID:
            return bool(await store.is_valid_path(first))
        case Query.INFO:
            return _info(await store.query_path_info(first))
        case Query.VALID_PATHS:
            return _paths(await store.query_valid_paths(targets))
        case Query.REFERRERS:
            return _paths(await store.query_referrers(first))
        case Query.CLOSURE:
            return _paths(await store.compute_fs_closure(targets))
        case Query.REVERSE_CLOSURE:
            return _paths(await store.compute_fs_closure(targets, flip_direction=True))
        case Query.HASH_PART:
            found = await store.query_path_from_hash_part(first.to_string()[:32])
            return None if found is None else str(found)
        case Query.ALL_VALID:
            return _paths(await store.query_all_valid_paths())
        case Query.DERIVERS:
            return _paths(await store.query_valid_derivers(first))
        case Query.TOPO_SORT:
            return tuple(str(path) for path in await store.topo_sort_paths(targets))
        case Query.OUTPUT_MAP:
            return tuple(
                sorted((name, str(path)) for name, path in (await store.query_derivation_output_map(first)).items())
            )
        case Query.MISSING:
            missing = await store.query_missing([_target(path) for path in targets])
            return (
                _paths(missing.will_build()),
                _paths(missing.will_substitute()),
                _paths(missing.unknown()),
                missing.download_size(),
                missing.nar_size(),
            )
        case Query.NAR:
            sink = _Nar()
            await store.nar_from_path(first, sink)
            return hashlib.sha256(sink.nar()).hexdigest()
        case Query.TRUSTED:
            return str(await store.is_trusted_client())
        case Query.REALISATION:
            found = await store.query_realisation(_drv_output(first, OUTPUT_NAMES[0]))
            if found is None:
                return None
            return (
                found.id().to_string(),
                str(found.out_path()),
                tuple(str(sig) for sig in found.signatures()),
            )


def _drv_output(drv: StorePath, output: str) -> object:
    if KEYED_BY_PATH:
        return huggorm.DrvOutput(drv, output)
    # Any hash is a key; this one follows the path, so one derivation has one key.
    digest = hashlib.sha256(str(drv).encode()).digest()
    return huggorm.DrvOutput(huggorm.Hash(huggorm.HashAlgorithm.SHA256, digest), output)


def _target(path: StorePath) -> object:
    """Every output of a derivation, or the path itself."""
    if path.to_string().endswith(".drv"):
        return huggorm.DerivedPathBuilt(path, huggorm.OutputsSpec(all=True))
    return path


def _quote(text: str) -> str:
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _aterm(name: str, inputs: list[StorePath]) -> str:
    """`_unparse` with the output path `checkInvariants` expects.

    With no input derivation, `hashDerivationModulo` hashes the text with every output path blank.
    The control daemon checks the path, so an error here shows as a refused add in both arms.
    """
    masked = hashlib.sha256(_unparse(name, "", inputs).encode()).digest()
    try:
        out = make_output_path("out", masked, name, "/nix/store")
    except ValueError:
        # A name no output path may carry: the output stays blank, for the daemons to refuse.
        out = ""
    return _unparse(name, out, inputs)


def _unparse(name: str, out: str, inputs: list[StorePath]) -> str:
    """A derivation with one input-addressed output, in Nix's ATerm form (`Derivation::unparse`)."""
    sources = ",".join(_quote(f"/nix/store/{path}") for path in sorted(str(p) for p in inputs))
    env = ",".join(
        f"({_quote(key)},{_quote(value)})"
        for key, value in sorted({"builder": "/bin/sh", "name": name, "out": out, "system": "x86_64-linux"}.items())
    )
    return f'Derive([("out",{_quote(out)},"","")],[],[{sources}],"x86_64-linux","/bin/sh",["-c","true"],[{env}])'


async def _add(store: AsyncStore, add: Add, known: list[StorePath], scratch: Path) -> StorePath:
    references = [_pick(known, i) for i in add.references]
    if add.method is not huggorm.ContentAddressMethod.NAR:
        return await store.add_to_store(add.name, add.data, method=add.method, references=references)
    # `add_to_store` sends a flat dump, and `RemoteStore::addToStoreFromDump` (`remote-store.cc:429`)
    # refuses a flat dump with a NAR hash on every remote store (nixidae/nixidae#1289). `add_path_to_store`
    # sends a NAR.
    await anyio.Path(scratch).write_bytes(add.data)
    return await store.add_path_to_store(add.name, str(scratch), method=add.method, references=references)


async def _import(store: AsyncStore, staging: AsyncStore, op: Import) -> StorePath:
    """Add to `staging`, then send its NAR and path info: unsigned, which a trusted client may send."""
    path = await staging.add_to_store(op.name, op.data)
    info = await staging.query_path_info(path)
    sink = _Nar()
    await staging.nar_from_path(path, sink)
    await store.add_to_store_nar(info, _Bytes(sink.nar()), check_sigs=False)
    return path


def _gc_options(op: Collect, known: list[StorePath]) -> object:
    if op.action is not huggorm.GCAction.DELETE_SPECIFIC:
        return huggorm.GCOptions(action=op.action)
    targets = [_pick(known, i) for i in op.targets]
    paths = huggorm.GCSpecificPaths(targets) if KEYED_BY_PATH else targets
    return huggorm.GCOptions(action=op.action, paths_to_delete=paths)


async def _apply(
    store: AsyncStore, uri: str, op: Op, known: list[StorePath], scratch: Path, staging: AsyncStore
) -> tuple[Outcome, StorePath | None]:
    try:
        match op:
            case Add():
                path = await _add(store, op, known, scratch)
                return Outcome(value=str(path)), path
            case Derive():
                inputs = [_pick(known, i) for i in op.inputs]
                text = _aterm(op.name, inputs)
                path = await store.add_to_store(
                    f"{op.name}.drv", text.encode(), method=huggorm.ContentAddressMethod.TEXT, references=inputs
                )
                return Outcome(value=str(path)), path
            case Import():
                path = await _import(store, staging, op)
                return Outcome(value=str(path)), path
            case Ask():
                return Outcome(value=await _ask(store, op, known)), None
            case Realise():
                realisation = huggorm.Realisation(_drv_output(_pick(known, op.drv), op.output), _pick(known, op.out))
                await store.register_drv_output(realisation, check_sigs=False)
                return Outcome(), None
            case Copy():
                path = await staging.add_to_store(op.name, op.data)
                await staging.copy_closure(store, [path], check_sigs=False)
                return Outcome(value=str(path)), path
            case Root():
                await store.add_temp_root(_pick(known, op.target))
                return Outcome(), None
            case Collect():
                results = await store.collect_garbage(_gc_options(op, known))
                return Outcome(value=(tuple(sorted(results.paths())), results.bytes_freed())), None
    except NixError as error:
        # The client names the store it asked, and the two arms listen on two sockets.
        return Outcome(error=(type(error).__name__, str(error).replace(uri, STORE_URI))), None


def _on_purpose(op: Op, control: Outcome, pynixd: Outcome) -> bool:
    """A divergence pynixd makes on purpose, where both arms refuse and pynixd says why.

    The NIX-DEFECT of `drv_parser._check_monotonic`: before 2.36 the control merges a duplicate input
    source and refuses the output path; pynixd refuses the duplicate, as master does.

    The NIX-DEFECT of `register.register_drv_output`: on 2.34 the control fails an SQLite constraint on an
    output path that is not valid; pynixd names the path.

    The NIX-DEVIATION of `collector.Collection.refuses`: a path that cannot go is refused by both, and pynixd
    names why. Upstream says "since it is still alive"; Determinate v3.24.0 names a temporary root
    `{nix-process:<pid>}` (`gc.cc:290`), where pynixd and Determinate's `main` name its file.
    """
    if control.error is None or pynixd.error is None:
        return False
    if isinstance(op, Derive):
        return (
            not CANONICAL_ATERM
            and "error parsing derivation" in pynixd.error[1]
            and ": duplicate store path '" in pynixd.error[1]
        )
    if isinstance(op, Realise):
        return (
            not KEYED_BY_PATH
            and "NOT NULL constraint failed: Realisations.outputPath" in control.error[1]
            and "' is not valid" in pynixd.error[1]
        )
    if isinstance(op, Collect):
        return _same_refused_path(control.error[1], pynixd.error[1])
    return False


_REFUSED = re.compile(r"Cannot delete path '([^']+)' (since it is still alive|because it's)")


def _same_refused_path(control: str, pynixd: str) -> bool:
    expected, actual = _REFUSED.search(control), _REFUSED.search(pynixd)
    return expected is not None and actual is not None and expected[1] == actual[1] and actual[2] == "because it's"


async def run(arms: dict[str, AsyncStore], ops: list[Op], scratch: Path, staging: AsyncStore) -> Divergence | None:
    """Apply `ops` to both stores, keyed by URI, control first; the first operation whose outcomes differ, or None.

    `staging` is a store of neither arm, where an `Import` makes its NAR.
    """
    (control_uri, control), (pynixd_uri, pynixd) = arms.items()
    known = [ABSENT]
    for step, op in enumerate(ops):
        expected, added = await _apply(control, control_uri, op, known, scratch, staging)
        actual, _ = await _apply(pynixd, pynixd_uri, op, known, scratch, staging)
        match op:
            case Add():
                kind = f"add {op.method}"
            case Ask():
                kind = str(op.query)
            case Derive():
                kind = "derive"
            case Import():
                kind = "import"
            case Realise():
                kind = "realise"
            case Copy():
                kind = "copy"
            case Root():
                kind = "root"
            case Collect():
                kind = f"gc {op.action}"
        event(f"{kind}: {'error ' + expected.error[0] if expected.error else 'answered'}")
        if expected != actual and not _on_purpose(op, expected, actual):
            return Divergence(step=step, op=op, control=expected, pynixd=actual)
        if added is not None:
            known.append(added)
    return None
