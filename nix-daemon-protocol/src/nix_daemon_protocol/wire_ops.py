"""WireRequest / WireResponse — base classes for Nix daemon operations.

These live in their own module to avoid circular imports: they depend on
both ``wire_message`` (WireModel, WireField) and ``logs`` (WireLogs).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Self

from .constants import MINIMUM_REMOTE_PROTOCOL, proto_str
from .exceptions import UnsupportedProtocolVersion
from .logs import WireLogs
from .wire_message import WireField, WireModel, _compiled_or_none, _wire_plan

if TYPE_CHECKING:
    from .context import ReadContext, WriteContext

WIRE_REGISTRY: dict[int, type[WireRequest]] = {}


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
    min_protocol: ClassVar[int] = MINIMUM_REMOTE_PROTOCOL
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
        if ctx.version and ctx.version < self.min_protocol:
            raise UnsupportedProtocolVersion(
                f"{self.name} requires daemon protocol >= {proto_str(self.min_protocol)}, got {proto_str(ctx.version)}",
            )
        ctx.writer.write_uint64(self.op)
        await self._write_body(ctx)

    @classmethod
    async def from_reader(cls, ctx: ReadContext) -> Self:
        """Read body only — op was consumed by dispatch."""
        return await super().from_reader(ctx)


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
        this constructor.
        """
        fields = cls.model_fields
        for name in body:
            if name not in fields:
                raise TypeError(f"{cls.__name__} has no field {name!r}")
        plan = _wire_plan(cls, 0, frozenset())
        obj = cls.__new__(cls)
        object.__setattr__(obj, "__pydantic_fields_set__", set(body) | {"logs"})
        object.__setattr__(obj, "__pydantic_extra__", None)
        object.__setattr__(obj, "__pydantic_private__", None)
        logs = WireLogs.__new__(WireLogs)
        object.__setattr__(logs, "__pydantic_fields_set__", {"messages"})
        object.__setattr__(logs, "__pydantic_extra__", None)
        object.__setattr__(logs, "__pydantic_private__", None)
        object.__setattr__(logs, "messages", [])
        object.__setattr__(obj, "logs", logs)
        for name, is_factory, value in plan.defaults:
            if name == "logs" or name in body:
                continue
            object.__setattr__(obj, name, value() if is_factory else value)
        for name, value in body.items():
            object.__setattr__(obj, name, value)
        return obj

    @property
    def is_not_found(self) -> bool:
        """True when extension fallback should continue to another store."""
        return False
