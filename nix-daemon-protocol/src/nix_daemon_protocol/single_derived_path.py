"""SingleDerivedPath — a store path, or one output of a derived path, as `worker-protocol.cc:244` frames it.

Each `Built` layer is tag 1, the layer below, then its output name; the
`Opaque` root is tag 0 and its store path. A path built two levels deep is
therefore `1 1 0 <path> <output> <output>` on the wire.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .exceptions import DaemonProtocolError
from .store_path import StorePath
from .wire_message import WireField, WireModel

if TYPE_CHECKING:
    from .context import ReadContext, WriteContext

OPAQUE, BUILT = 0, 1


class SingleDerivedPath(WireModel):
    """The `Opaque` path at the root, and the output each `Built` layer names, innermost first."""

    path: StorePath = WireField(default_factory=StorePath)
    outputs: tuple[str, ...] = WireField(default=())

    @property
    def is_opaque(self) -> bool:
        return not self.outputs

    def __str__(self) -> str:
        """`SingleDerivedPath::to_string`: the path, then `^output` per layer."""
        return "".join((str(self.path), *(f"^{output}" for output in self.outputs)))

    @classmethod
    async def from_reader(cls, ctx: ReadContext) -> SingleDerivedPath:
        tag = await ctx.reader.read_uint64()
        if tag == OPAQUE:
            return cls(path=StorePath.from_wire(await ctx.reader.read_string()))
        if tag == BUILT:
            inner = await cls.from_reader(ctx)
            output = await ctx.reader.read_string()
            return cls(path=inner.path, outputs=(*inner.outputs, output))
        raise DaemonProtocolError(f"Invalid tag {tag} for single derived path")

    async def to_writer(self, ctx: WriteContext) -> None:
        for _ in self.outputs:
            ctx.writer.write_uint64(BUILT)
        ctx.writer.write_uint64(OPAQUE)
        ctx.writer.write_string(self.path.to_wire())
        for output in self.outputs:
            ctx.writer.write_string(output)
