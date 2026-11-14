"""WireRequest / WireResponse — base classes for Nix daemon operations.

These live in their own module to avoid circular imports: they depend on
both ``wire_message`` (WireModel, WireField) and ``logs`` (WireLogs).
"""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any, ClassVar, Self

from .constants import FEATURE_OPEN_TELEMETRY, MINIMUM_PROTOCOL, proto_str
from .exceptions import UnsupportedProtocolVersion
from .logs import WireLogs
from .wire_message import WireField, WireModel, _compiled_or_none, _wire_plan

if TYPE_CHECKING:
    from collections.abc import Callable

    from .context import ReadContext, WriteContext

WIRE_REGISTRY: dict[int, type[WireRequest]] = {}


@functools.cache
def _fast_defaults(cls: type[WireResponse]) -> tuple[dict[str, Any], tuple[tuple[str, Callable[[], Any]], ...]]:
    """What `WireResponse.fast` fills for a field the caller leaves out: the static defaults, and the factories."""
    plan = _wire_plan(cls)  # type: ignore[arg-type]  # zuban-bug: zuban checks type[BaseModel] against Hashable through BaseModel's __eq__-derived __hash__ = None; mypy accepts the class object
    defaults = [(name, is_factory, value) for name, is_factory, value in plan.defaults if name != "logs"]
    static = {name: value for name, is_factory, value in defaults if not is_factory}
    return static, tuple((name, value) for name, is_factory, value in defaults if is_factory)


class WireRequest(WireModel):
    """Base class for Nix daemon protocol requests.

    Subclasses must override ``op`` and ``response_type``::

        class SomeRequest(WireRequest):
            op: ClassVar[int] = 42
            response_type: ClassVar[type[SomeResponse]] = SomeResponse
            path: StorePath
    """

    op: ClassVar[int]
    name: ClassVar[str]
    response_type: ClassVar[type]
    min_protocol: ClassVar[int] = MINIMUM_PROTOCOL
    forward: ClassVar[bool] = True
    is_extension: ClassVar[bool] = False
    is_query: ClassVar[bool] = False

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if "name" not in cls.__dict__:
            cls.name = cls.__name__.removesuffix("Request")
        if "op" in cls.__dict__:
            WIRE_REGISTRY[cls.op] = cls

    async def to_writer(self, ctx: WriteContext) -> None:
        """Write op code then body, resolving the codec once."""
        codec = _compiled_or_none(type(self), version=ctx.version, features=ctx.features)
        if codec is not None:
            # The compiled request writes the prelude itself.
            await codec.write(self, ctx)
            return
        await write_request_prelude(self, ctx)
        await self._write_body(ctx)

    @classmethod
    async def from_reader(cls, ctx: ReadContext) -> Self:
        """Read body only — op was consumed by dispatch."""
        return await super().from_reader(ctx)


async def write_request_prelude(value: WireRequest, ctx: WriteContext) -> None:
    """The op code, and the trace context after it under `open-telemetry` (`startOp`, `worker-protocol-connection.cc:261`
    of Determinate v3.24.0). Both codecs of a request write it here."""
    if ctx.version and ctx.version < value.min_protocol:
        raise UnsupportedProtocolVersion(
            f"{value.name} requires daemon protocol >= {proto_str(value.min_protocol)}, got {proto_str(ctx.version)}",
        )
    ctx.writer.write_uint64(value.op)
    if FEATURE_OPEN_TELEMETRY in ctx.features:
        ctx.writer.write_string(ctx.traceparent)


class WireResponse(WireModel):
    """Base class for Nix daemon protocol responses.

    The ``logs`` field is a ``WireLogs`` (stderr stream).  Because it is a
    ``WireModel`` the generic serde engine handles it automatically —
    ``to_writer`` writes the full stderr stream before the body fields,
    ``from_reader`` reads the stream before the body fields.

    Subclasses define wire-body fields as normal::

        class SomeResponse(WireResponse):
            valid: bool
    """

    logs: WireLogs = WireField(default_factory=WireLogs)

    @classmethod
    def fast(cls, **body: Any) -> Self:
        """Build a response the daemon answers with, without validation.

        Only for values that are already the declared types: the daemon
        decoded them from the wire, parsed them from a store, or holds them
        as constants. There is nothing to coerce and nothing to refuse, so
        a full pydantic validation only re-checks what an earlier boundary
        already proved. Anything built from untrusted input -- a remote
        `.narinfo`, a client string -- goes through the validated
        constructor instead.

        The plan resolves at version 0 with no features, and only its
        defaults are kept: field defaults are static values, so no version
        resolves different ones, and the encode still gates fields under the
        negotiated version. This sets the body, fills those defaults, and
        gives the response fresh empty logs.

        The logs are fresh per response and not shared: `query_missing` and
        `set_options` append to the logs of the response they build, and a
        shared log would carry one operation's warnings into another's
        answer. The log is built by hand and not by `model_construct`: an
        empty log needs no validation, and `model_construct` cost half of
        this constructor. Both objects take their whole `__dict__` in one
        assignment, as `model_construct` does: one `object.__setattr__` per
        field made this 1.9 us and not 1.3 (nixidae/nixidae#1345).
        """
        if not body.keys() <= cls.model_fields.keys():
            unknown = sorted(body.keys() - cls.model_fields.keys())
            raise TypeError(f"{cls.__name__} has no field {unknown[0]!r}")
        static, factories = _fast_defaults(cls)
        values = dict(static)
        for name, factory in factories:
            if name not in body:
                values[name] = factory()
        values.update(body)
        logs = WireLogs.__new__(WireLogs)
        object.__setattr__(logs, "__dict__", {"messages": []})
        object.__setattr__(logs, "__pydantic_fields_set__", {"messages"})
        object.__setattr__(logs, "__pydantic_extra__", None)
        object.__setattr__(logs, "__pydantic_private__", None)
        values["logs"] = logs
        obj = cls.__new__(cls)
        object.__setattr__(obj, "__dict__", values)
        object.__setattr__(obj, "__pydantic_fields_set__", set(body) | {"logs"})
        object.__setattr__(obj, "__pydantic_extra__", None)
        object.__setattr__(obj, "__pydantic_private__", None)
        return obj

    @property
    def is_not_found(self) -> bool:
        """True when extension fallback should continue to another store."""
        return False
