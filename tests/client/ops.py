"""Store operations as data, the strategies that generate them, and one run of a list of them on both arms.

An operation names the paths it uses by index into the paths known so far.
Index 0 is a path no example ever adds, so a query or a reference can name an
absent path, and hypothesis shrinks a failing list to the fewest operations
with the smallest indices.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

import anyio
import huggorm  # type: ignore[import-not-found]  # the type gate has no huggorm; see default.nix
from huggorm.errors import NixError  # type: ignore[import-not-found]  # the type gate has no huggorm; see default.nix
from hypothesis import event, strategies as st

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


class Query(StrEnum):
    VALID = "is_valid_path"
    INFO = "query_path_info"
    VALID_PATHS = "query_valid_paths"
    REFERRERS = "query_referrers"
    CLOSURE = "compute_fs_closure"
    REVERSE_CLOSURE = "compute_fs_closure(flip_direction=True)"
    HASH_PART = "query_path_from_hash_part"


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


type Op = Add | Ask
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
operations = st.lists(st.one_of(adds, asks), min_size=1, max_size=12)


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


async def _add(store: AsyncStore, add: Add, known: list[StorePath], scratch: Path) -> StorePath:
    references = [_pick(known, i) for i in add.references]
    if add.method is not huggorm.ContentAddressMethod.NAR:
        return await store.add_to_store(add.name, add.data, method=add.method, references=references)
    # `add_to_store` sends a flat dump, and `RemoteStore::addToStoreFromDump` (`remote-store.cc:429`)
    # refuses a flat dump with a NAR hash on every remote store (nixidae/nixidae#1289). `add_path_to_store`
    # sends a NAR.
    await anyio.Path(scratch).write_bytes(add.data)
    return await store.add_path_to_store(add.name, str(scratch), method=add.method, references=references)


async def _apply(
    store: AsyncStore, uri: str, op: Op, known: list[StorePath], scratch: Path
) -> tuple[Outcome, StorePath | None]:
    try:
        match op:
            case Add():
                path = await _add(store, op, known, scratch)
                return Outcome(value=str(path)), path
            case Ask():
                return Outcome(value=await _ask(store, op, known)), None
    except NixError as error:
        # The client names the store it asked, and the two arms listen on two sockets.
        return Outcome(error=(type(error).__name__, str(error).replace(uri, STORE_URI))), None


async def run(arms: dict[str, AsyncStore], ops: list[Op], scratch: Path) -> Divergence | None:
    """Apply `ops` to both stores, keyed by URI, control first; the first operation whose outcomes differ, or None."""
    (control_uri, control), (pynixd_uri, pynixd) = arms.items()
    known = [ABSENT]
    for step, op in enumerate(ops):
        expected, added = await _apply(control, control_uri, op, known, scratch)
        actual, _ = await _apply(pynixd, pynixd_uri, op, known, scratch)
        kind = f"add {op.method}" if isinstance(op, Add) else str(op.query)
        event(f"{kind}: {'error ' + expected.error[0] if expected.error else 'answered'}")
        if expected != actual:
            return Divergence(step=step, op=op, control=expected, pynixd=actual)
        if added is not None:
            known.append(added)
    return None
