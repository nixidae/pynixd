"""Declarative binary serialization for Nix daemon protocol types.

Pydantic models inherit from ``WireModel`` to auto-generate
to_writer()/from_reader() from type annotations.

Class variables (ClassVar[int]) are skipped — they're protocol constants
like op codes that are written/read by the operation layer, not the message body.
"""

from __future__ import annotations

import asyncio
import functools
import types
from collections.abc import Callable, Iterable  # noqa: TC003
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, ClassVar, NamedTuple, Self, get_args, get_origin, get_type_hints

from pydantic import BaseModel, ConfigDict
from pydantic import Field as PydanticField
from pydantic_core import PydanticUndefined

from .context import ReadContext, WriteContext
from .logging import deserialization_scope
from .wire_integer import WireUInt64
from .wire_scalar import is_wire_scalar


class WirePlan(NamedTuple):
    """One resolved codec per (model, version, features)."""

    read_steps: tuple
    write_steps: tuple
    defaults: tuple


# ── Helpers ──


def _nested_context(ctx: ReadContext) -> ReadContext:
    """The context a nested `WireModel` reads under.

    **`error_factory`, `logger` and `log_sink` cross over.**
    `WireResponse.logs` is a `WireLogs`, so every response of the protocol
    reads its stderr stream through here, and `WireLogs.from_reader` is the
    one reader that acts on the log settings. A nested read built a bare
    context, so none of them reached it, and a daemon error came back as
    `DaemonProtocolError` although the caller asked for its own class.

    `log_sink` crosses so that the stream reaches the client while it is
    read. A response kept every message until the answer instead, and a
    substituting `QueryValidPaths` kept one per chunk of the closure: 10.4
    GB, and garbage collections that stalled the loop until the liveness
    probe killed pynixd. Issue #507. A streamed message is not kept unless
    the caller asks, so `proxy.py` writing the response back sends it once.

    `buffer_logs` crosses, except with nothing streaming: an unbuffered
    stream with no sink is a lost one.

    `raise_on_error` does not cross: it would turn a daemon error into a
    silent success. `DaemonStore.call` declares `raise_on_error=False` as
    its default, and every operation of pynixd goes through it. Honouring
    that flag here made `_try_substitute_upstream` read a failed
    `EnsurePath` as a hit, and `nix build` then waited 112 s for a
    substitution that never came. Issue #46.
    """
    if ctx.raise_on_error and (ctx.buffer_logs or ctx.log_sink is not None):
        # Already settled by an enclosing nested read. Every response nests
        # its `WireLogs` one level down, and everything below that reads under
        # the same settings, so this returns the context unchanged for all
        # but the first nesting. `dataclasses.replace` ran 4,177,587 times in
        # one system build, 8.0 s of its 137 s; this keeps all but the topmost
        # off that path. The rebuild below is the first nesting only.
        return ctx
    return ReadContext(
        reader=ctx.reader,
        version=ctx.version,
        log_sink=ctx.log_sink,
        buffer_logs=ctx.buffer_logs or ctx.log_sink is None,
        raise_on_error=True,
        error_factory=ctx.error_factory,
        logger=ctx.logger,
        features=ctx.features,
    )


@functools.lru_cache(maxsize=256)
def _find_reader(ann: type, version: int = 0, features: frozenset[str] = frozenset()) -> Any:
    """Look up a reader for a wire type.

    **Each reader takes the whole `ReadContext`, and not the `NixReader`
    alone.** A nested `WireModel` needs the settings of the read that
    contains it, and `WireLogs` is nested in every response, so a reader that
    took the reader alone had no way to reach them. See `_read_nested`.

    The cache keys on the annotation, the version and the feature set. The
    context is an argument of the reader for that reason: it holds the
    reader, which is one connection and not one type.
    """
    from .wire_string import WireString  # lazy: break circular import

    # Primitives
    if ann is int:
        return lambda ctx: ctx.reader.read_uint64()
    if isinstance(ann, type) and issubclass(ann, WireUInt64):

        async def _read_uint64_model(ctx):
            return ann(await ctx.reader.read_uint64())

        return _read_uint64_model
    # IntEnum — read uint64, convert to enum member
    if isinstance(ann, type) and issubclass(ann, IntEnum):

        async def _read_enum(ctx):
            return ann(await ctx.reader.read_uint64())

        return _read_enum
    if ann is str:
        return lambda ctx: ctx.reader.read_string(str)
    if ann is bool:
        return lambda ctx: ctx.reader.read_bool()
    if ann is bytes:
        return lambda ctx: ctx.reader.read_bytes()

    origin = get_origin(ann)
    args = get_args(ann)

    # Optional[T] — strip None, delegate to inner
    if origin is types.UnionType:
        non_none = tuple(a for a in args if a is not type(None))
        if len(non_none) == 1:
            inner = _find_reader(non_none[0], version, features)
            if is_wire_scalar(non_none[0]):
                # Nix writes an absent scalar as the empty string, which
                # `_write_optional_scalar` below answers for `None`. Without this the
                # value comes back as the empty scalar rather than as `None`,
                # so a caller that tests `is None` never takes that branch.
                # The bytes do not move; only the Python value does. Issue nixidae/nixidae#233.
                async def _read_optional_scalar(ctx: Any) -> Any:
                    value = await inner(ctx)
                    # `not value` and not `value == ""`. A scalar that keeps
                    # its own type does not compare equal to a string, so the
                    # equality silently stopped matching and an absent
                    # `deriver` came back as `StorePath("")` instead of
                    # `None`. Every scalar is falsy when its wire string is
                    # empty, so emptiness is the test.
                    return value or None

                return _read_optional_scalar
            return inner

    # -- list generics --
    if origin is list:
        elem = _find_reader(args[0], version, features)

        async def _read_list(ctx):
            n = await ctx.reader.read_uint64()
            return [await elem(ctx) for _ in range(n)]

        return _read_list

    # -- set generics --
    if origin is set:
        elem = _find_reader(args[0], version, features)

        async def _read_set(ctx):
            n = await ctx.reader.read_uint64()
            return {await elem(ctx) for _ in range(n)}

        return _read_set

    # -- dict generics --
    if origin is dict:
        k_reader = _find_reader(args[0], version, features)
        v_reader = _find_reader(args[1], version, features)

        async def _read_dict(ctx):
            n = await ctx.reader.read_uint64()
            d = {}
            for _ in range(n):
                key = await k_reader(ctx)
                val = await v_reader(ctx)
                d[key] = val
            return d

        return _read_dict

    # WireString — read one string, construct directly (no model_construct)
    if isinstance(ann, type) and issubclass(ann, WireString):
        n_fields = len(ann.model_fields)
        if n_fields == 1:
            _field_name: str = next(iter(ann.model_fields.keys()))

            async def _read_string(ctx):
                raw = await ctx.reader.read_string(str)
                obj = ann.__new__(ann)
                object.__setattr__(obj, "__pydantic_extra__", None)
                object.__setattr__(obj, "__pydantic_private__", None)
                object.__setattr__(obj, _field_name, raw)
                object.__setattr__(obj, "__pydantic_fields_set__", {_field_name})
                return obj
        else:

            async def _read_string(ctx):
                raw = await ctx.reader.read_string(str)
                data = ann.from_str(raw)
                if not isinstance(data, dict):
                    raise TypeError(f"from_str returned {type(data).__name__}, expected dict")
                obj = ann.__new__(ann)
                field_names = set(data.keys())
                object.__setattr__(obj, "__pydantic_extra__", None)
                object.__setattr__(obj, "__pydantic_private__", None)
                object.__setattr__(obj, "__pydantic_fields_set__", field_names)
                for k, v in data.items():
                    object.__setattr__(obj, k, v)
                return obj

        return _read_string

    # One Python value, one wire string. Tested by its methods, not its
    # base class, so a scalar that stops being a `str` still reads here.
    if is_wire_scalar(ann):

        async def _read_scalar(ctx):
            return ann.from_wire(await ctx.reader.read_string(str))

        return _read_scalar

    # WireModel subclass
    if isinstance(ann, type) and issubclass(ann, WireModel):

        async def _read_nested(ctx):
            return await ann.from_reader(_nested_context(ctx))

        return _read_nested

    raise TypeError(f"No reader for {ann}")


@functools.lru_cache(maxsize=256)
def _find_writer(ann: type, version: int = 0, features: frozenset[str] = frozenset()) -> Any:
    """Look up an async writer for a wire type, mirroring `_find_reader`.

    Every writer is `async`, so a container can `await` its element writer
    whatever the element is. Each takes the value and the `WriteContext`. The
    cache keys on the annotation, the version and the feature set, because a
    version- or feature-gated annotation resolves to a different writer.
    """
    from .wire_string import WireString  # lazy: break circular import

    # Primitives
    if ann is int:

        async def _write_u64(val: Any, ctx: WriteContext) -> None:
            ctx.writer.write_uint64(val)

        return _write_u64
    if isinstance(ann, type) and issubclass(ann, WireUInt64):

        async def _write_uint64_model(val: Any, ctx: WriteContext) -> None:
            ctx.writer.write_uint64(val)

        return _write_uint64_model
    if ann is str:

        async def _write_str(val: Any, ctx: WriteContext) -> None:
            ctx.writer.write_string(val)

        return _write_str
    if ann is bool:

        async def _write_bool(val: Any, ctx: WriteContext) -> None:
            ctx.writer.write_bool(val)

        return _write_bool
    if ann is bytes:

        async def _write_bytes(val: Any, ctx: WriteContext) -> None:
            ctx.writer.write_bytes(val)

        return _write_bytes

    origin = get_origin(ann)
    args = get_args(ann)

    # Optional[T] — delegate to inner type (val should never be None on the wire)
    if origin is types.UnionType:
        non_none = tuple(a for a in args if a is not type(None))
        if len(non_none) == 1:
            inner = _find_writer(non_none[0], version, features)
            if is_wire_scalar(non_none[0]):

                async def _write_optional_scalar(val: Any, ctx: WriteContext) -> None:
                    # Nix represents an absent scalar as the empty string, and
                    # not as the textual representation of the Python value.
                    # This is the write half of the rule that `_find_reader`
                    # reads back, and it holds for every scalar rather than for
                    # `StorePath` alone. Issue nixidae/nixidae#233.
                    if val is None:
                        ctx.writer.write_string("")
                        return
                    await inner(val, ctx)

                return _write_optional_scalar
            return inner

    # -- list generics --
    if origin is list:
        element = _find_writer(args[0], version, features)

        async def _write_list(val: Any, ctx: WriteContext) -> None:
            ctx.writer.write_uint64(len(val))
            for item in val:
                await element(item, ctx)

        return _write_list

    # -- set generics --
    if origin is set:
        element = _find_writer(args[0], version, features)

        async def _write_set(val: Any, ctx: WriteContext) -> None:
            ctx.writer.write_uint64(len(val))
            for item in val:
                await element(item, ctx)

        return _write_set

    # -- dict generics --
    if origin is dict:
        key_writer = _find_writer(args[0], version, features)
        value_writer = _find_writer(args[1], version, features)

        async def _write_dict(val: Any, ctx: WriteContext) -> None:
            ctx.writer.write_uint64(len(val))
            for key, value in val.items():
                await key_writer(key, ctx)
                await value_writer(value, ctx)

        return _write_dict

    # WireString — write str(self) as a single wire string
    if isinstance(ann, type) and issubclass(ann, WireString):

        async def _write_string_model(val: Any, ctx: WriteContext) -> None:
            ctx.writer.write_string(str(val))

        return _write_string_model

    # One Python value, one wire string. See `_find_reader`.
    if is_wire_scalar(ann):

        async def _write_scalar(val: Any, ctx: WriteContext) -> None:
            ctx.writer.write_string(val.to_wire())

        return _write_scalar

    # WireModel subclass
    if isinstance(ann, type) and issubclass(ann, WireModel):

        async def _write_nested(val: Any, ctx: WriteContext) -> None:
            result = val.to_writer(ctx)
            if asyncio.iscoroutine(result):
                await result

        return _write_nested

    # IntEnum — write its int value
    if isinstance(ann, type) and issubclass(ann, IntEnum):

        async def _write_enum(val: Any, ctx: WriteContext) -> None:
            ctx.writer.write_uint64(val.value)

        return _write_enum

    raise TypeError(f"No writer for {ann}")


# ── Version-constrained fields ──


@dataclass(frozen=True)
class VersionMeta:
    """Protocol version and feature constraint for a wire field."""

    min_version: int | None = None
    max_version: int | None = None
    serialize: bool | None = None
    deserialize: bool | None = None
    wire_depends_on: Callable | None = None
    needs_features: frozenset[str] | None = None
    unless_features: frozenset[str] | None = None
    text_errors: str = "strict"


def WireField(  # noqa: N802
    default: Any = PydanticUndefined,
    *,
    default_factory: Any = None,
    min_version: int | None = None,
    max_version: int | None = None,
    serialize: bool | None = None,
    deserialize: bool | None = None,
    wire_depends_on: Callable | None = None,
    needs_features: Iterable[str] | None = None,
    unless_features: Iterable[str] | None = None,
    text_errors: str = "strict",
    **kwargs: Any,
) -> Any:
    """A Pydantic Field with Nix protocol version and feature requirements.

    Usage::

        times_built: int = WireField(default=0, min_version=proto(1, 29))
        legacy_field: str = WireField(default="", max_version=proto(1, 27))

    **The version number stopped at 1.38, and a new capability is a feature.**
    `worker-protocol.hh:105` of Nix says so. Both sides send a set of names in
    the handshake, and the negotiated set is the intersection, so a version
    number alone no longer says what shape a field has.

    Nix writes that choice as an if/else over the negotiated set, and these
    two arguments are its two halves. `worker-protocol.cc:268` is the
    example: `BuildResult.builtOutputs` is a map of `UnkeyedRealisation` when
    `realisation-with-path-not-hash` is on, and a map of JSON strings when it
    is off::

        built_outputs_new: ... = WireField(needs_features=[FEATURE_REALISATION_WITH_PATH])
        built_outputs_old: ... = WireField(unless_features=[FEATURE_REALISATION_WITH_PATH])

    `needs_features` keeps the field when the negotiated set holds **every**
    name. `unless_features` keeps it when the set holds **none** of them. A
    field with neither is there whatever the peers agreed, which is every
    field of the Nix 2.34 shape. Issue #14.

    `text_errors` is the error handler for the bytes of a `str` field, and it
    is `"strict"` unless the field says otherwise. `"surrogateescape"` decodes
    every byte and re-encodes it back to the same byte, which is what a field
    takes when Nix treats it as opaque bytes that it never validates: log and
    error text. A builder prints whatever it prints, and the daemon forwards
    it without looking, so strict decoding fails the whole build on a gzip
    stream in the log. Issue nixidae/nixidae#662. Anything but `"strict"` or
    `"surrogateescape"` is refused here: a lossy handler would round-trip the
    bytes into something else, silently.
    """
    if text_errors not in ("strict", "surrogateescape"):
        raise ValueError(f"text_errors={text_errors!r} must be 'strict' or 'surrogateescape'")
    if default is not PydanticUndefined:
        kwargs.setdefault("default", default)
    if default_factory is not None:
        kwargs["default_factory"] = default_factory

    field_info = PydanticField(**kwargs)
    field_info.metadata.append(
        VersionMeta(
            min_version,
            max_version,
            serialize,
            deserialize,
            wire_depends_on,
            None if needs_features is None else frozenset(needs_features),
            None if unless_features is None else frozenset(unless_features),
            text_errors,
        )
    )
    return field_info


# ── Field collection ──


@functools.lru_cache(maxsize=256)
def _wire_fields(
    cls: type[BaseModel],
    version: int = 0,
    features: frozenset[str] = frozenset(),
) -> list[tuple[str, type, Callable | None, bool, bool, str]]:
    """Return (name, raw_annotation, wire_depends_on, serialize, deserialize, text_errors) tuples.

    ClassVar fields default to serialize=False, deserialize=False unless
    ``WireField(serialize=..., deserialize=...)`` overrides them explicitly.

    Fields with ``min_version`` or ``max_version`` constraints are filtered
    against the provided ``version``.  ``version=0`` means no filtering.

    Fields with ``needs_features`` or ``unless_features`` are filtered against
    *features*, which is the set the two peers negotiated. That filter runs
    whatever the version is, because an empty set is a real answer: it is what
    Nix 2.34 offers, and what a peer below 1.38 can say at all.
    """
    hints = get_type_hints(cls, include_extras=True)
    result = []
    for name in cls.model_fields:
        field = cls.model_fields[name]
        version_meta: VersionMeta | None = next((m for m in field.metadata if isinstance(m, VersionMeta)), None)

        # Version-gating (0 means no filtering)
        if version_meta is not None and version:
            if version_meta.min_version is not None and version < version_meta.min_version:
                continue
            if version_meta.max_version is not None and version > version_meta.max_version:
                continue

        # Feature-gating. No `if features:` guard: the empty set decides as
        # much as any other set, and a field behind `unless_features` is
        # exactly the one that the empty set keeps.
        if version_meta is not None:
            if version_meta.needs_features is not None and not version_meta.needs_features <= features:
                continue
            if version_meta.unless_features is not None and version_meta.unless_features & features:
                continue

        ann = hints.get(name)
        if ann is None:
            continue

        is_classvar = get_origin(ann) is ClassVar

        # Resolve serialize / deserialize flags
        if version_meta is not None:
            _serialize = version_meta.serialize if version_meta.serialize is not None else (not is_classvar)
            _deserialize = version_meta.deserialize if version_meta.deserialize is not None else (not is_classvar)
        else:
            _serialize = not is_classvar
            _deserialize = not is_classvar

        wire_depends_on = version_meta.wire_depends_on if version_meta else None
        text_errors = version_meta.text_errors if version_meta else "strict"
        result.append((name, ann, wire_depends_on, _serialize, _deserialize, text_errors))
    return result


@functools.lru_cache(maxsize=256)
def _compiled_or_none(cls: type, version: int = 0, features: frozenset[str] = frozenset()) -> Any:
    """The compiled codec for (model, version, features), or `None`.

    A model with a custom codec refuses, and the refusal is cached like the
    codec: every operation looks this up, and a refusal that recompiled its
    error each time would cost what the cache exists to remove. Only the
    deliberate refusal is caught: any other error is a compiler bug, and a
    silent fallback would hide it. The import is lazy because the compiler
    imports this module.
    """
    from .experimental_compiled import NotCompilableError, compile_codec

    try:
        return compile_codec(cls, version, features)
    except NotCompilableError:
        return None


def _lossless_reader(errors: str) -> Any:
    """Read one wire string without validating its bytes as text.

    The length prefix frames the bytes whatever they hold, so the decode
    cannot fail: `errors` round-trips each byte through the `str` and back.
    """

    async def _read_lossless(ctx: Any) -> str:
        return (await ctx.reader.read_bytes()).decode("utf-8", errors=errors)

    return _read_lossless


def _lossless_optional_reader(errors: str) -> Any:
    """The `str | None` half of `_lossless_reader`.

    The empty-wire-string-answers-`None` rule of `_find_reader`, spelled for
    a codec that never raises on the bytes. Issue nixidae/nixidae#233.
    """

    async def _read_lossless_optional(ctx: Any) -> Any:
        return (await ctx.reader.read_bytes()).decode("utf-8", errors=errors) or None

    return _read_lossless_optional


def _lossless_writer(errors: str) -> Any:
    """Write one `str` back to the bytes it decoded from."""

    async def _write_lossless(val: Any, ctx: Any) -> None:
        ctx.writer.write_bytes(val.encode("utf-8", errors=errors))

    return _write_lossless


def _lossless_optional_writer(errors: str) -> Any:
    """The `str | None` half of `_lossless_writer`.

    The absent-scalar-travels-as-empty rule of `_find_writer`: `None` takes
    the empty string, which encodes under every handler.
    """

    async def _write_lossless_optional(val: Any, ctx: Any) -> None:
        if val is None:
            ctx.writer.write_string("")
        else:
            ctx.writer.write_bytes(val.encode("utf-8", errors=errors))

    return _write_lossless_optional


def _lossless_shape(ann: Any) -> str | None:
    """`plain` for `str`, `optional` for `str | None`, else `None`.

    Only these two shapes take `text_errors`: anything else with the flag is
    a declaration bug, and `_wire_plan` refuses it rather than guessing.
    """
    if ann is str:
        return "plain"
    if get_origin(ann) is types.UnionType:
        non_none = tuple(a for a in get_args(ann) if a is not type(None))
        if len(non_none) == 1 and non_none[0] is str:
            return "optional"
    return None


@functools.lru_cache(maxsize=256)
def _wire_plan(
    cls: type[BaseModel],
    version: int = 0,
    features: frozenset[str] = frozenset(),
) -> WirePlan:
    """One resolved codec per (model, version, features): readers, writers, defaults.

    A session negotiates one version and one feature set, and then decodes
    thousands of operations under them. `_wire_fields` already caches the
    field list, but every operation still paid a lookup per field
    (`_find_reader`, `_find_writer`), a pass over `model_fields` for the
    defaults, and the gating walk that the cache made redundant. This
    resolves all of it once: the per-operation path below is one cache
    lookup and straight-line reads and writes.

    The dynamic half stays dynamic. `wire_depends_on` reads the values of
    the operation, so it still runs per operation, and a default factory
    still runs per operation -- the factory is shared, its product is not.
    """
    read_steps = []
    write_steps = []
    for name, ann, wire_depends_on, serialize, deserialize, text_errors in _wire_fields(cls, version, features):
        shape = _lossless_shape(ann) if text_errors != "strict" else None
        if text_errors != "strict" and shape is None:
            raise TypeError(f"{cls.__name__}.{name}: text_errors={text_errors!r} needs a `str` field")
        if shape == "plain":
            read_steps.append((name, _lossless_reader(text_errors), wire_depends_on, deserialize))
            write_steps.append((name, _lossless_writer(text_errors), wire_depends_on, serialize))
        elif shape == "optional":
            read_steps.append((name, _lossless_optional_reader(text_errors), wire_depends_on, deserialize))
            write_steps.append((name, _lossless_optional_writer(text_errors), wire_depends_on, serialize))
        else:
            read_steps.append((name, _find_reader(ann, version, features), wire_depends_on, deserialize))
            write_steps.append((name, _find_writer(ann, version, features), wire_depends_on, serialize))

    defaults = []
    for name, field in cls.model_fields.items():
        if field.default is not PydanticUndefined:
            defaults.append((name, False, field.default))
        elif field.default_factory is not None:
            defaults.append((name, True, field.default_factory))
    return WirePlan(tuple(read_steps), tuple(write_steps), tuple(defaults))


# ── Base class ──


class WireModel(BaseModel):
    """Pydantic base class with auto-generated Nix daemon protocol serde.

    Usage:
        class FooResponse(WireModel):
            valid: int   # uint64 on the wire
            path: str    # length-prefixed UTF-8
    """

    # **The name of a field builds one of these, and the alias does too.**
    # The wire uses the alias, and Python code uses the name. With the alias
    # alone, `Realisation(out_path=...)` put nothing in the field and raised
    # nothing, so the answer carried `null` where a store path belongs. The
    # rule belongs here, so the next field that gets an alias cannot repeat it.
    model_config = ConfigDict(arbitrary_types_allowed=True, validate_by_alias=True, validate_by_name=True)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    def __hash__(self) -> int:
        return hash(tuple(getattr(self, f) for f in self.__class__.model_fields))

    async def to_writer(self, ctx: WriteContext) -> None:
        """Write all non-ClassVar fields in declaration order."""
        codec = _compiled_or_none(type(self), version=ctx.version, features=ctx.features)
        if codec is not None:
            await codec.write(self, ctx)
            return
        await self._write_body(ctx)

    async def _write_body(self, ctx: WriteContext) -> None:
        """Write the body fields through the resolved plan, no prelude."""
        plan = _wire_plan(type(self), version=ctx.version, features=ctx.features)
        for name, writer, wire_depends_on, serialize in plan.write_steps:
            if not serialize:
                continue
            if wire_depends_on is not None and not wire_depends_on(self):
                continue

            await writer(getattr(self, name), ctx)

    @classmethod
    async def from_reader(cls, ctx: ReadContext):
        """Read all non-ClassVar fields in declaration order."""
        codec = _compiled_or_none(cls, version=ctx.version, features=ctx.features)
        if codec is not None:
            return await codec.read(ctx)
        with deserialization_scope(ctx, cls):
            plan = _wire_plan(cls, version=ctx.version, features=ctx.features)
            obj = cls.__new__(cls)
            # Initialize Pydantic internals (bypassed __init__)
            object.__setattr__(obj, "__pydantic_fields_set__", set())
            object.__setattr__(obj, "__pydantic_extra__", None)
            object.__setattr__(obj, "__pydantic_private__", None)
            # Set defaults for version-gated and conditional fields
            for name, is_factory, value in plan.defaults:
                object.__setattr__(obj, name, value() if is_factory else value)  # pyright: ignore[reportCallIssue]

            for name, reader, wire_depends_on, deserialize in plan.read_steps:
                if not deserialize:
                    continue
                if wire_depends_on is not None and not wire_depends_on(obj):
                    continue

                object.__setattr__(obj, name, await reader(ctx))
                obj.__pydantic_fields_set__.add(name)

            return obj

    def to_json(self, **kwargs) -> str:
        """Serialize to JSON string.

        Uses Pydantic's ``model_dump_json``.
        """
        return self.model_dump_json(**kwargs)

    @classmethod
    def from_json(cls, json_data: str | bytes, **kwargs) -> Self:
        """Deserialize from JSON string.

        Uses Pydantic's ``model_validate_json``.
        """
        return cls.model_validate_json(json_data, **kwargs)
