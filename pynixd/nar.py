"""NAR (Nix Archive) format parser and serializer.

Provides structured access to NAR archives for reading, editing, and writing.
See https://nix.dev/manual/nix/2.34/protocols/nix-archive/
"""

from __future__ import annotations

import io
import struct
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from .wire import NarDepth, _nar_pad

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import BinaryIO


# ═════════════════════════════════════════════════════════════════════════════
# Data model
# ═════════════════════════════════════════════════════════════════════════════


@dataclass
class NarDirectoryEntry:
    """A single entry within a NAR directory."""

    name: str
    node: NarNode


@dataclass
class NarDirectory:
    """A directory node in a NAR archive.

    Entries must be ordered by their names (Nix requirement).
    """

    entries: list[NarDirectoryEntry] = field(default_factory=list)


@dataclass
class NarRegular:
    """A regular file node in a NAR archive."""

    contents: bytes = b""
    executable: bool = False


@dataclass
class NarSymlink:
    """A symbolic link node in a NAR archive."""

    target: str = ""


NarNode = NarDirectory | NarRegular | NarSymlink


# ═════════════════════════════════════════════════════════════════════════════
# Low-level: padded string encoding
# ═════════════════════════════════════════════════════════════════════════════

# _nar_pad is imported from wire.py for 8-byte alignment calculation.


def _write_padded_string(buf: io.BytesIO, s: str) -> None:
    """Write a length-prefixed, null-padded ASCII string."""
    encoded = s.encode("ascii")
    length = len(encoded)
    buf.write(struct.pack("<Q", length))
    buf.write(encoded)
    buf.write(b"\x00" * _nar_pad(length))


def _read_padded_string(data: bytes, offset: int) -> tuple[str, int]:
    """Read a length-prefixed, null-padded ASCII string.

    Returns (string, new_offset).
    """
    length = struct.unpack_from("<Q", data, offset)[0]
    offset += 8
    body = data[offset : offset + length].decode("ascii")
    offset += length + _nar_pad(length)
    return body, offset


# ═════════════════════════════════════════════════════════════════════════════
# Writing
# ═════════════════════════════════════════════════════════════════════════════


def _write_node(buf: io.BytesIO, node: NarNode) -> None:
    _write_padded_string(buf, "(")
    _write_padded_string(buf, "type")

    if isinstance(node, NarDirectory):
        _write_padded_string(buf, "directory")
        for entry in node.entries:
            _write_padded_string(buf, "entry")
            _write_padded_string(buf, "(")
            _write_padded_string(buf, "name")
            _write_padded_string(buf, entry.name)
            _write_padded_string(buf, "node")
            _write_node(buf, entry.node)
            _write_padded_string(buf, ")")
        # Directory terminator: a single ")" padded string
        _write_padded_string(buf, ")")

    elif isinstance(node, NarRegular):
        _write_padded_string(buf, "regular")
        if node.executable:
            _write_padded_string(buf, "executable")
            _write_padded_string(buf, "")
        _write_padded_string(buf, "contents")
        length = len(node.contents)
        buf.write(struct.pack("<Q", length))
        buf.write(node.contents)
        buf.write(b"\x00" * _nar_pad(length))
        _write_padded_string(buf, ")")

    elif isinstance(node, NarSymlink):
        _write_padded_string(buf, "symlink")
        _write_padded_string(buf, "target")
        _write_padded_string(buf, node.target)
        _write_padded_string(buf, ")")
    else:
        raise TypeError(f"Unknown NAR node type: {type(node).__name__}")


def write_nar(node: NarNode) -> bytes:
    """Serialize a NAR node to raw NAR archive bytes."""
    buf = io.BytesIO()
    _write_padded_string(buf, "nix-archive-1")
    _write_node(buf, node)
    return buf.getvalue()


def write_nar_to_path(node: NarNode, path: str | Path) -> None:
    """Serialize a NAR node to a file."""
    Path(path).write_bytes(write_nar(node))


# ═════════════════════════════════════════════════════════════════════════════
# Reading
# ═════════════════════════════════════════════════════════════════════════════


def _expect_padded_string(data: bytes, offset: int, expected: str) -> int:
    """Read a padded string and assert it equals *expected*.

    Returns the new offset.
    """
    body, offset = _read_padded_string(data, offset)
    if body != expected:
        msg = f"Expected {expected!r} at offset {offset - len(body) - 8}, got {body!r}"
        raise ValueError(msg)
    return offset


def _read_node(data: bytes, offset: int) -> tuple[NarNode, int]:
    """Read a single NAR node.

    Returns (node, new_offset).
    """
    offset = _expect_padded_string(data, offset, "(")
    offset = _expect_padded_string(data, offset, "type")

    type_val, offset = _read_padded_string(data, offset)

    if type_val == "directory":
        entries: list[NarDirectoryEntry] = []
        while True:
            kind, offset = _read_padded_string(data, offset)
            if kind == ")":
                # Directory terminator — no closing ')' at node level
                return NarDirectory(entries=entries), offset
            if kind != "entry":
                raise ValueError(f"Expected 'entry' or ')' in directory, got {kind!r}")

            offset = _expect_padded_string(data, offset, "(")
            offset = _expect_padded_string(data, offset, "name")
            name, offset = _read_padded_string(data, offset)
            offset = _expect_padded_string(data, offset, "node")
            child, offset = _read_node(data, offset)
            offset = _expect_padded_string(data, offset, ")")
            entries.append(NarDirectoryEntry(name=name, node=child))

    elif type_val == "regular":
        executable = False
        # Read attributes until we hit "contents"
        while True:
            key, offset = _read_padded_string(data, offset)
            if key == "contents":
                break
            if key == "executable":
                # Executable attribute value is always empty string
                _, offset = _read_padded_string(data, offset)
                executable = True
            else:
                raise ValueError(f"Unknown regular file attribute: {key!r}")

        # File data
        length = struct.unpack_from("<Q", data, offset)[0]
        offset += 8
        contents = data[offset : offset + length]
        offset += length + _nar_pad(length)

        offset = _expect_padded_string(data, offset, ")")
        return NarRegular(contents=contents, executable=executable), offset

    elif type_val == "symlink":
        offset = _expect_padded_string(data, offset, "target")
        target, offset = _read_padded_string(data, offset)
        offset = _expect_padded_string(data, offset, ")")
        return NarSymlink(target=target), offset

    else:
        raise ValueError(f"Unknown NAR node type: {type_val!r}")


def parse_nar(data: bytes) -> NarNode:
    """Parse raw NAR archive bytes into a structured representation."""
    if len(data) < 8:
        raise ValueError("NAR data too short")
    magic, offset = _read_padded_string(data, 0)
    if magic != "nix-archive-1":
        raise ValueError(f"Invalid NAR magic: {magic!r}")
    node, offset = _read_node(data, offset)
    if offset != len(data):
        raise ValueError(f"Trailing bytes after NAR archive: {len(data) - offset} bytes")
    return node


def parse_nar_from_path(path: str | Path) -> NarNode:
    """Parse a NAR archive from a file."""
    return parse_nar(Path(path).read_bytes())


# ═════════════════════════════════════════════════════════════════════════════
# Helpers
# ═════════════════════════════════════════════════════════════════════════════


def _walk(node: NarNode, prefix: str = "") -> Iterator[tuple[str, NarNode]]:
    """Yield (path, node) pairs for every node in the archive."""
    yield prefix, node
    if isinstance(node, NarDirectory):
        for entry in node.entries:
            child_prefix = f"{prefix}/{entry.name}" if prefix else entry.name
            yield from _walk(entry.node, child_prefix)


def nar_entries(node: NarNode) -> Iterator[tuple[str, NarNode]]:
    """Iterate over all entries in a NAR archive.

    Yields (path, node) pairs where *path* is the relative path within the
    archive and *node* is the NAR node at that path.
    """
    return _walk(node)


def find_nar_entry(node: NarNode, path: str) -> NarNode | None:
    """Find a NAR node by its path within the archive.

    Returns *None* if the path does not exist.
    """
    for p, n in _walk(node):
        if p == path:
            return n
    return None


# ═════════════════════════════════════════════════════════════════════════════
# Streaming NAR support (parenthesis tracking)
# ═════════════════════════════════════════════════════════════════════════════


class NarForwarder:
    """Stateful NAR forwarder that tracks parenthesis depth.

    Feed raw bytes in, get forwarding chunks out.  Stops automatically
    when the NAR archive is complete (``self.complete`` becomes ``True``).

    Usage example::

        forwarder = NarForwarder()
        while True:
            chunk = source.read(65536)
            if not chunk:
                break
            for forwarded in forwarder.feed(chunk):
                destination.write(forwarded)
            if forwarder.complete:
                break
    """

    def __init__(self) -> None:
        self._buf = bytearray()
        self._depth = NarDepth()
        self._body_len = 0
        self._in_body = False
        self.complete = False

    def _consume_token(self) -> bytes | None:
        """Try to consume one complete token from the internal buffer.

        Returns the raw token bytes or *None* if not enough data is buffered.
        """
        if not self._in_body:
            if len(self._buf) < 8:
                return None
            self._body_len = struct.unpack("<Q", self._buf[:8])[0]
            self._in_body = True

        pad = _nar_pad(self._body_len)
        need = 8 + self._body_len + pad
        if len(self._buf) < need:
            return None

        token = bytes(self._buf[:need])
        del self._buf[:need]
        self._in_body = False
        return token

    def feed(self, data: bytes) -> Iterator[bytes]:
        """Consume *data* and yield forwarding chunks.

        Each yielded chunk is a complete NAR token (length header + body +
        padding) that can be written to the destination unchanged.  When the
        archive is complete ``self.complete`` is set and no more tokens will
        be yielded.
        """
        if self.complete:
            return

        self._buf.extend(data)

        while True:
            token = self._consume_token()
            if token is None:
                return
            yield token

            self.complete = self._depth.step(token[8 : 8 + self._body_len])
            if self.complete:
                return


def forward_nar(src: BinaryIO, dst: BinaryIO) -> int:
    """Copy a NAR archive from *src* to *dst*, returning total bytes copied.

    Reads until the self-terminating NAR structure is complete, so the
    caller does not need to know the NAR size in advance.
    """
    forwarder = NarForwarder()
    total = 0
    while True:
        chunk = src.read(65536)
        if not chunk:
            break
        for token in forwarder.feed(chunk):
            dst.write(token)
            total += len(token)
        if forwarder.complete:
            break
    return total


def nar_size(data: bytes) -> int:
    """Return the byte size of the NAR archive at the start of *data*.

    Raises *ValueError* if *data* is too short or does not contain a
    complete NAR.
    """
    forwarder = NarForwarder()
    offset = 0
    for token in forwarder.feed(data):
        offset += len(token)
    if forwarder.complete:
        return offset
    raise ValueError("Incomplete NAR archive in buffer")


# ═════════════════════════════════════════════════════════════════════════════
# pathlib-like API
# ═════════════════════════════════════════════════════════════════════════════


class NarPath:
    """A pathlib-like interface for navigating and editing NAR archives.

    Usage::

        # Build a directory NAR
        root = NarPath()
        root = (root / "bin" / "hello").write_text("hi", executable=True)
        data = root.to_nar()

        # Build a single-file NAR
        root = NarPath().write_text("hello")
        data = root.to_nar()
    """

    def __init__(self, root: NarNode | None = None, parts: tuple[str, ...] = ()) -> None:
        self._root = root if root is not None else NarDirectory()
        self._parts = parts

    @classmethod
    def from_nar(cls, data: bytes) -> NarPath:
        """Create a NarPath from raw NAR bytes."""
        return cls(parse_nar(data), ())

    @classmethod
    def from_node(cls, node: NarNode) -> NarPath:
        """Create a NarPath from an existing NarNode."""
        return cls(node, ())

    def to_nar(self) -> bytes:
        """Serialize the NAR archive rooted at this path."""
        return write_nar(self._root)

    @property
    def name(self) -> str:
        return self._parts[-1] if self._parts else ""

    @property
    def parent(self) -> NarPath:
        return NarPath(self._root, self._parts[:-1])

    def __truediv__(self, other: str) -> NarPath:
        if not isinstance(other, str):
            return NotImplemented
        new_parts = list(self._parts)
        for segment in other.split("/"):
            if segment in ("", "."):
                continue
            if segment == "..":
                if new_parts:
                    new_parts.pop()
            else:
                new_parts.append(segment)
        return NarPath(self._root, tuple(new_parts))

    def __str__(self) -> str:
        if not self._parts:
            return "/"
        return "/" + "/".join(self._parts)

    def __repr__(self) -> str:
        return f"NarPath({str(self)!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, NarPath):
            return NotImplemented
        return self._parts == other._parts

    def __hash__(self) -> int:
        return hash(self._parts)

    def _resolve(self) -> NarNode:
        node = self._root
        for part in self._parts:
            if not isinstance(node, NarDirectory):
                raise ValueError(f"Not a directory: {self}")  # noqa: TRY004
            for entry in node.entries:
                if entry.name == part:
                    node = entry.node
                    break
            else:
                raise ValueError(f"Path not found: {self}")
        return node

    def exists(self) -> bool:
        try:
            self._resolve()
        except ValueError:
            return False
        else:
            return True

    def is_file(self) -> bool:
        try:
            return isinstance(self._resolve(), NarRegular)
        except ValueError:
            return False

    def is_dir(self) -> bool:
        try:
            return isinstance(self._resolve(), NarDirectory)
        except ValueError:
            return False

    def is_symlink(self) -> bool:
        try:
            return isinstance(self._resolve(), NarSymlink)
        except ValueError:
            return False

    def iterdir(self) -> Iterator[NarPath]:
        node = self._resolve()
        if not isinstance(node, NarDirectory):
            raise ValueError(f"Not a directory: {self}")  # noqa: TRY004
        for entry in node.entries:
            yield NarPath(self._root, (*self._parts, entry.name))

    def read_bytes(self) -> bytes:
        node = self._resolve()
        if not isinstance(node, NarRegular):
            raise ValueError(f"Not a regular file: {self}")  # noqa: TRY004
        return node.contents

    def read_text(self, encoding: str = "utf-8") -> str:
        return self.read_bytes().decode(encoding)

    def _replace_in_tree(self, new_node: NarNode) -> NarNode:
        """Rebuild the tree, replacing the node at self._parts."""
        if not self._parts:
            return new_node

        def rebuild(node: NarNode, parts: tuple[str, ...]) -> NarNode:
            if not isinstance(node, NarDirectory):
                raise TypeError(f"Expected NarDirectory, got {type(node).__name__}")
            name = parts[0]
            new_entries = []
            found = False
            for entry in node.entries:
                if entry.name == name:
                    found = True
                    if len(parts) == 1:
                        new_entries.append(NarDirectoryEntry(name=name, node=new_node))
                    else:
                        new_entries.append(NarDirectoryEntry(name=name, node=rebuild(entry.node, parts[1:])))
                else:
                    new_entries.append(entry)
            if not found:
                raise ValueError(f"Path not found: {self}")
            return NarDirectory(entries=new_entries)

        return rebuild(self._root, self._parts)

    def _insert_child(self, name: str, node: NarNode) -> NarNode:
        """Insert a child into this directory, returning new root."""
        target = self._resolve()
        if not isinstance(target, NarDirectory):
            raise ValueError(f"Not a directory: {self}")  # noqa: TRY004
        for entry in target.entries:
            if entry.name == name:
                raise ValueError(f"File exists: {name}")
        new_entries = [*target.entries, NarDirectoryEntry(name=name, node=node)]
        return self._replace_in_tree(NarDirectory(entries=new_entries))

    def write_bytes(self, data: bytes, *, executable: bool = False) -> NarPath:
        """Write file contents, returning a NarPath to the root of the new tree.

        When called on the root path (``/``), replaces the root directory
        with a file — useful for creating single-file NARs.
        """
        new_node = NarRegular(contents=data, executable=executable)
        if self.exists():
            if self.is_dir() and self._parts:
                raise ValueError(f"Is a directory: {self}")
            new_root = self._replace_in_tree(new_node)
        else:
            parent = self.parent
            if not parent.exists():
                raise ValueError(f"No such directory: {parent}")
            new_root = parent._insert_child(self.name, new_node)
        return NarPath(new_root, ())

    def write_text(self, text: str, *, encoding: str = "utf-8", executable: bool = False) -> NarPath:
        return self.write_bytes(text.encode(encoding), executable=executable)

    def mkdir(self, *, parents: bool = False) -> NarPath:
        """Create a directory, returning a NarPath to the root of the new tree."""
        if self.exists():
            if self.is_dir():
                return NarPath(self._root, ())
            raise ValueError(f"File exists: {self}")
        if not self._parts:
            raise ValueError("Cannot mkdir root")
        parent = self.parent
        if not parent.exists():
            if not parents:
                raise ValueError(f"No such directory: {parent}")
            root = parent.mkdir(parents=True)
            parent = NarPath(root._root, parent._parts)
        new_root = parent._insert_child(self.name, NarDirectory())
        return NarPath(new_root, ())

    def unlink(self) -> NarPath:
        """Remove this path from its parent, returning a NarPath to the root of the new tree."""
        if not self.exists():
            raise ValueError(f"No such file or directory: {self}")
        if not self._parts:
            raise ValueError("Cannot unlink root")
        parent = self.parent
        target = parent._resolve()
        if not isinstance(target, NarDirectory):
            raise ValueError(f"Parent is not a directory: {parent}")  # noqa: TRY004
        new_entries = [e for e in target.entries if e.name != self.name]
        new_root = parent._replace_in_tree(NarDirectory(entries=new_entries))
        return NarPath(new_root, ())

    def chmod(self, *, executable: bool = True) -> NarPath:
        """Toggle executable flag, returning a NarPath to the root of the new tree."""
        node = self._resolve()
        if not isinstance(node, NarRegular):
            raise ValueError(f"Not a regular file: {self}")  # noqa: TRY004
        new_node = NarRegular(contents=node.contents, executable=executable)
        new_root = self._replace_in_tree(new_node)
        return NarPath(new_root, ())
