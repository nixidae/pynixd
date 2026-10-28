"""A Nix store path: one class, holding the base name.

**Nix holds the base name and nothing else.**
`src/libstore/include/nix/store/path.hh`::

    class StorePath
    {
        std::string baseName;

The store directory is not part of the value. It belongs to the store, at
`StoreDirConfig::storeDir`, and `printStorePath` is what puts it in front.

This module used to hold a second, different `StorePath`: a `WireScalar`, so
a `str`, holding the whole path -- while `pynixd.store_path` held the one
below. The two were not `==`, did not hash alike, and a dict keyed by one
never answered the other. Issue #3 holds the measurement. There is one class
now, and `pynixd.store_path` re-exports it.

**It is not a `str`.** Measured before the change: no call site depended on
the `str`-ness. Every str method called on a store path in the repository was
in `tests/functional`, and all of them were on `stdout.strip()` from a
subprocess. A `str` subclass also makes the wrong thing easy -- `path[:10]`
slices text that means nothing -- and it hides the question this class exists
to answer, which is which form a path is in.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Self

from pydantic_core import core_schema

from .store_dir import store_prefix


class StorePath:
    """A Nix store path, held as its base name.

    The store directory is stripped on construction and put back by
    `__str__`, so a value never carries it twice and never carries it in one
    place and not another.
    """

    __slots__ = ("_path", "extrainfo")

    _path: str
    extrainfo: Any

    def __init__(self, path: str | StorePath = "", extrainfo: Any = None) -> None:
        if isinstance(path, StorePath):
            self._path = path._path
            self.extrainfo = extrainfo or path.extrainfo
        else:
            self._path = self._strip_prefix(str(path))
            self.extrainfo = extrainfo

    @staticmethod
    def _strip_prefix(path: str) -> str:
        """Remove the store directory, and refuse a path of another store.

        The refusal is the point. This kept an absolute path of another store
        whole, and `__str__` then put the store directory in front of it a
        second time. The result named no file, and nothing reported the
        mistake. Issue nixidae/nixidae#212 holds the measurement.
        """
        prefix = store_prefix()
        if path.startswith(prefix):
            return path[len(prefix) :]
        if path.startswith("/"):
            from .store_dir import store_dir

            raise ValueError(f"{path!r} is not a path of the store at {store_dir()!r}")
        return path

    # ── the wire ───────────────────────────────────────────────────

    @classmethod
    def from_wire(cls, value: str) -> Self:
        return cls(value)

    def to_wire(self) -> str:
        """The whole path, which is what the daemon wire carries.

        `CommonProto::Serialise<StorePath>::write` of Nix calls
        `store.printStorePath`, and that is this. narinfo's `References:`
        calls `to_string()` instead, which is `name` below -- a different
        codec, not a differently annotated field.
        """
        return str(self)

    def to_json_value(self) -> str:
        """What JSON carries: the base name.

        `adl_serializer<nix::StorePath>::to_json`, `src/libstore/path.cc:95`
        of Nix, writes `storePath.to_string()`, and `from_json` builds one
        straight back from that string. So a `Realisation`, which travels as
        JSON, carries `abc-foo` where the binary wire carries
        `/nix/store/abc-foo`.

        **This is the whole of issue #4 in one pair of methods.** One value,
        two codecs, and the form belongs to the codec rather than to the
        value or to the field that holds it.
        """
        return self.name

    @classmethod
    def __get_pydantic_core_schema__(cls, _source: Any, _handler: Any) -> core_schema.CoreSchema:
        """Read either form, write the JSON one.

        The instance arm comes first so a `StorePath` passes through; a string
        goes to the constructor, which takes the base name or the whole path.
        """
        return core_schema.union_schema(
            [
                core_schema.is_instance_schema(cls),
                core_schema.no_info_after_validator_function(
                    cls.from_wire,
                    core_schema.str_schema(),
                ),
            ],
            serialization=core_schema.plain_serializer_function_ser_schema(cls.to_json_value),
        )

    # ── accessors ──────────────────────────────────────────────────

    @property
    def path(self) -> str:
        """The whole path. The spelling most of pynixd already uses."""
        return str(self)

    def base(self) -> str:
        """The base name, without the store directory."""
        return self._path

    @property
    def name(self) -> str:
        """The base name: what Nix's `to_string()` answers."""
        return Path(self._path).name

    def hash_part(self) -> str:
        """The 32-character hash in front of the name."""
        return self.name.split("-", 1)[0]

    def base_name(self) -> str:
        """The readable part, after the hash."""
        parts = self.name.split("-", 1)
        return parts[1] if len(parts) > 1 else ""

    def is_derivation(self) -> bool:
        return self._path.endswith(".drv")

    def to_path(self) -> Path:
        """The whole path, as a `pathlib.Path`."""
        return Path(str(self))

    def with_store_prefix(self) -> StorePath:
        """Return self: `__str__` already puts the store directory first."""
        return self

    # ── str-adjacent helpers, on the base name ─────────────────────

    def endswith(self, suffix: str) -> bool:
        return self._path.endswith(suffix)

    def startswith(self, prefix: str) -> bool:
        return self._path.startswith(prefix)

    # ── dunders ────────────────────────────────────────────────────

    def __str__(self) -> str:
        if not self._path:
            return ""
        return store_prefix() + self._path

    def __repr__(self) -> str:
        inner = repr(self._path)
        if self.extrainfo:
            return f"StorePath({inner}, info={self.extrainfo!r})"
        return f"StorePath({inner})"

    def __eq__(self, other: object) -> bool:
        """Only another `StorePath`.

        Not a `str`. A path and its text are different things, and the whole
        reason this class exists is that there is more than one text for one
        path. `str(path) == text` says which text was meant.

        `extrainfo` is a debugging note and is not part of the value, so two
        paths that differ only there are equal and hash alike.
        """
        if isinstance(other, StorePath):
            return self._path == other._path
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._path)

    def __bool__(self) -> bool:
        return bool(self._path)

    def __len__(self) -> int:
        return len(self._path)

    def __lt__(self, other: object) -> bool:
        if isinstance(other, StorePath):
            return self._path < other._path
        return NotImplemented

    def __le__(self, other: object) -> bool:
        if isinstance(other, StorePath):
            return self._path <= other._path
        return NotImplemented

    def __json__(self) -> str:
        return str(self)
