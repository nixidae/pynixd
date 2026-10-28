"""Unit tests for pynixd.nar — NAR format parser and serializer."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pynixd.nar import (
    NarDirectory,
    NarDirectoryEntry,
    NarForwarder,
    NarPath,
    NarRegular,
    NarSymlink,
    find_nar_entry,
    forward_nar,
    nar_entries,
    nar_size,
    parse_nar,
    parse_nar_from_path,
    write_nar,
    write_nar_to_path,
)

if TYPE_CHECKING:
    from pathlib import Path

# ═════════════════════════════════════════════════════════════════════════════
# 1. Roundtrip tests
# ═════════════════════════════════════════════════════════════════════════════


class TestNarRoundtrip:
    """Serialize → deserialize → compare for every node type."""

    async def test_empty_file(self) -> None:
        node = NarRegular(contents=b"")
        data = write_nar(node)
        result = parse_nar(data)
        assert isinstance(result, NarRegular)
        assert result.contents == b""
        assert result.executable is False

    async def test_small_file(self) -> None:
        node = NarRegular(contents=b"hello world")
        data = write_nar(node)
        result = parse_nar(data)
        assert isinstance(result, NarRegular)
        assert result.contents == b"hello world"
        assert result.executable is False

    async def test_executable_file(self) -> None:
        node = NarRegular(contents=b"#!/bin/sh\necho hi", executable=True)
        data = write_nar(node)
        result = parse_nar(data)
        assert isinstance(result, NarRegular)
        assert result.contents == b"#!/bin/sh\necho hi"
        assert result.executable is True

    async def test_file_with_padding(self) -> None:
        """File sizes that are not multiples of 8 need padding."""
        for size in [1, 7, 8, 9, 15, 16, 17, 63, 64, 65]:
            contents = b"x" * size
            node = NarRegular(contents=contents)
            data = write_nar(node)
            result = parse_nar(data)
            assert isinstance(result, NarRegular)
            assert result.contents == contents

    async def test_symlink(self) -> None:
        node = NarSymlink(target="/nix/store/abc-foo/bin/hello")
        data = write_nar(node)
        result = parse_nar(data)
        assert isinstance(result, NarSymlink)
        assert result.target == "/nix/store/abc-foo/bin/hello"

    async def test_empty_directory(self) -> None:
        node = NarDirectory(entries=[])
        data = write_nar(node)
        result = parse_nar(data)
        assert isinstance(result, NarDirectory)
        assert result.entries == []

    async def test_directory_with_files(self) -> None:
        node = NarDirectory(
            entries=[
                NarDirectoryEntry(name="a.txt", node=NarRegular(contents=b"aaa")),
                NarDirectoryEntry(name="b.txt", node=NarRegular(contents=b"bbb")),
            ]
        )
        data = write_nar(node)
        result = parse_nar(data)
        assert isinstance(result, NarDirectory)
        assert len(result.entries) == 2
        assert result.entries[0].name == "a.txt"
        assert isinstance(result.entries[0].node, NarRegular)
        assert result.entries[0].node.contents == b"aaa"
        assert result.entries[1].name == "b.txt"
        assert isinstance(result.entries[1].node, NarRegular)
        assert result.entries[1].node.contents == b"bbb"

    async def test_nested_directory(self) -> None:
        node = NarDirectory(
            entries=[
                NarDirectoryEntry(
                    name="outer",
                    node=NarDirectory(
                        entries=[
                            NarDirectoryEntry(
                                name="inner",
                                node=NarDirectory(
                                    entries=[
                                        NarDirectoryEntry(
                                            name="deep.txt",
                                            node=NarRegular(contents=b"deep"),
                                        ),
                                    ]
                                ),
                            ),
                        ]
                    ),
                ),
                NarDirectoryEntry(name="top.txt", node=NarRegular(contents=b"top")),
            ]
        )
        data = write_nar(node)
        result = parse_nar(data)
        assert isinstance(result, NarDirectory)
        assert len(result.entries) == 2
        assert result.entries[0].name == "outer"
        outer = result.entries[0].node
        assert isinstance(outer, NarDirectory)
        assert len(outer.entries) == 1
        assert outer.entries[0].name == "inner"
        inner = outer.entries[0].node
        assert isinstance(inner, NarDirectory)
        assert len(inner.entries) == 1
        assert inner.entries[0].name == "deep.txt"
        assert isinstance(inner.entries[0].node, NarRegular)
        assert inner.entries[0].node.contents == b"deep"
        assert result.entries[1].name == "top.txt"
        assert isinstance(result.entries[1].node, NarRegular)
        assert result.entries[1].node.contents == b"top"

    async def test_mixed_directory(self) -> None:
        node = NarDirectory(
            entries=[
                NarDirectoryEntry(
                    name="bin",
                    node=NarDirectory(
                        entries=[
                            NarDirectoryEntry(
                                name="hello",
                                node=NarRegular(
                                    contents=b"#!/bin/sh\necho hello",
                                    executable=True,
                                ),
                            ),
                        ]
                    ),
                ),
                NarDirectoryEntry(
                    name="lib",
                    node=NarDirectory(
                        entries=[
                            NarDirectoryEntry(
                                name="link",
                                node=NarSymlink(target="/nix/store/abc-foo/lib"),
                            ),
                        ]
                    ),
                ),
                NarDirectoryEntry(name="README", node=NarRegular(contents=b"hi")),
            ]
        )
        data = write_nar(node)
        result = parse_nar(data)
        assert isinstance(result, NarDirectory)
        assert len(result.entries) == 3


# ═════════════════════════════════════════════════════════════════════════════
# 2. Error handling
# ═════════════════════════════════════════════════════════════════════════════


class TestNarErrors:
    """Invalid NAR data should raise ValueError."""

    async def test_bad_magic(self) -> None:
        data = b"\x0enot-a-nar-\x00\x00\x00\x00\x00\x00"
        with pytest.raises(ValueError, match="Invalid NAR magic"):
            parse_nar(data)

    async def test_too_short(self) -> None:
        with pytest.raises(ValueError, match="too short"):
            parse_nar(b"")

    async def test_bad_node_type(self) -> None:
        """Inject an unknown type value after the magic."""
        import io

        from pynixd.nar import _write_padded_string

        buf = io.BytesIO()
        _write_padded_string(buf, "nix-archive-1")
        _write_padded_string(buf, "(")
        _write_padded_string(buf, "type")
        _write_padded_string(buf, "unknown")
        data = buf.getvalue()

        with pytest.raises(ValueError, match="Unknown NAR node type"):
            parse_nar(data)

    async def test_trailing_bytes(self) -> None:
        node = NarRegular(contents=b"x")
        data = write_nar(node) + b"extra"
        with pytest.raises(ValueError, match="Trailing bytes"):
            parse_nar(data)


# ═════════════════════════════════════════════════════════════════════════════
# 3. Helpers
# ═════════════════════════════════════════════════════════════════════════════


class TestNarHelpers:
    """Test walk / find helpers."""

    async def test_nar_entries(self) -> None:
        node = NarDirectory(
            entries=[
                NarDirectoryEntry(
                    name="a",
                    node=NarDirectory(
                        entries=[
                            NarDirectoryEntry(name="b.txt", node=NarRegular(contents=b"b")),
                        ]
                    ),
                ),
                NarDirectoryEntry(name="c.txt", node=NarRegular(contents=b"c")),
            ]
        )
        paths = {p for p, _ in nar_entries(node)}
        assert paths == {"", "a", "a/b.txt", "c.txt"}

    async def test_find_nar_entry(self) -> None:
        node = NarDirectory(
            entries=[
                NarDirectoryEntry(name="x", node=NarRegular(contents=b"xxx")),
            ]
        )
        found = find_nar_entry(node, "x")
        assert isinstance(found, NarRegular)
        assert found.contents == b"xxx"

    async def test_find_nar_entry_missing(self) -> None:
        node = NarDirectory(entries=[])
        assert find_nar_entry(node, "missing") is None


# ═════════════════════════════════════════════════════════════════════════════
# 4. File I/O
# ═════════════════════════════════════════════════════════════════════════════


class TestNarFileIO:
    """Roundtrip through the filesystem."""

    async def test_write_and_read_path(self, tmp_path: Path) -> None:
        path = tmp_path / "test.nar"
        node = NarDirectory(
            entries=[
                NarDirectoryEntry(name="file.txt", node=NarRegular(contents=b"from file")),
            ]
        )
        write_nar_to_path(node, path)
        result = parse_nar_from_path(path)
        assert isinstance(result, NarDirectory)
        assert result.entries[0].name == "file.txt"
        assert isinstance(result.entries[0].node, NarRegular)
        assert result.entries[0].node.contents == b"from file"


# ═════════════════════════════════════════════════════════════════════════════
# 5. Streaming NAR support
# ═════════════════════════════════════════════════════════════════════════════


class TestNarForwarder:
    """Test the streaming NAR forwarder with various chunk sizes."""

    async def test_forward_whole(self) -> None:
        """Feed the entire NAR in one chunk."""
        node = NarDirectory(
            entries=[
                NarDirectoryEntry(name="a.txt", node=NarRegular(contents=b"aaa")),
            ]
        )
        data = write_nar(node)
        forwarder = NarForwarder()
        tokens = list(forwarder.feed(data))
        assert forwarder.complete
        assert b"".join(tokens) == data

    async def test_forward_byte_by_byte(self) -> None:
        """Feed one byte at a time — the hardest case."""
        node = NarDirectory(
            entries=[
                NarDirectoryEntry(name="a.txt", node=NarRegular(contents=b"aaa")),
            ]
        )
        data = write_nar(node)
        forwarder = NarForwarder()
        tokens: list[bytes] = []
        for byte in data:
            tokens.extend(forwarder.feed(bytes([byte])))
        assert forwarder.complete
        assert b"".join(tokens) == data

    async def test_forward_random_chunks(self) -> None:
        """Feed in random-sized chunks."""
        import random

        node = NarDirectory(
            entries=[
                NarDirectoryEntry(name="x", node=NarRegular(contents=b"x" * 1000)),
                NarDirectoryEntry(
                    name="y",
                    node=NarDirectory(
                        entries=[
                            NarDirectoryEntry(name="z", node=NarSymlink(target="/a")),
                        ]
                    ),
                ),
            ]
        )
        data = write_nar(node)
        rng = random.Random(42)
        offset = 0
        forwarder = NarForwarder()
        tokens: list[bytes] = []
        while offset < len(data):
            size = rng.randint(1, 64)
            chunk = data[offset : offset + size]
            tokens.extend(forwarder.feed(chunk))
            offset += len(chunk)
        assert forwarder.complete
        assert b"".join(tokens) == data

    async def test_forward_executable(self) -> None:
        """Forwarding preserves executable bit via token content."""
        node = NarRegular(contents=b"#!/bin/sh", executable=True)
        data = write_nar(node)
        forwarder = NarForwarder()
        tokens = list(forwarder.feed(data))
        assert forwarder.complete
        assert b"".join(tokens) == data

    async def test_forward_empty_directory(self) -> None:
        """An empty directory has just the terminator ')' token."""
        node = NarDirectory(entries=[])
        data = write_nar(node)
        forwarder = NarForwarder()
        tokens = list(forwarder.feed(data))
        assert forwarder.complete
        assert b"".join(tokens) == data


class TestForwardNar:
    """Test the convenience forward_nar function."""

    async def test_forward_nar(self) -> None:
        import io

        node = NarDirectory(
            entries=[
                NarDirectoryEntry(name="file.txt", node=NarRegular(contents=b"hello")),
            ]
        )
        data = write_nar(node)
        src = io.BytesIO(data)
        dst = io.BytesIO()
        total = forward_nar(src, dst)
        assert total == len(data)
        assert dst.getvalue() == data


class TestNarSize:
    """Test nar_size() for extracting NAR length from a larger buffer."""

    async def test_nar_size_exact(self) -> None:
        node = NarRegular(contents=b"test")
        data = write_nar(node)
        assert nar_size(data) == len(data)

    async def test_nar_size_with_trailing(self) -> None:
        node = NarRegular(contents=b"test")
        data = write_nar(node) + b"EXTRA_TRAILING_BYTES"
        assert nar_size(data) == len(write_nar(node))

    async def test_nar_size_nested(self) -> None:
        node = NarDirectory(
            entries=[
                NarDirectoryEntry(name="a", node=NarDirectory(entries=[])),
                NarDirectoryEntry(name="b", node=NarRegular(contents=b"bbb")),
            ]
        )
        data = write_nar(node)
        assert nar_size(data) == len(data)

    async def test_nar_size_incomplete(self) -> None:
        with pytest.raises(ValueError, match="Incomplete"):
            nar_size(b"nix-arc")  # too short

    async def test_nar_size_partial(self) -> None:
        """A buffer that starts with a valid NAR magic but is truncated."""
        node = NarRegular(contents=b"x")
        data = write_nar(node)
        with pytest.raises(ValueError, match="Incomplete"):
            nar_size(data[: len(data) // 2])


class TestNarPath:
    """Test the pathlib-like NarPath API."""

    async def test_construction_from_nar(self) -> None:
        data = write_nar(NarDirectory(entries=[NarDirectoryEntry(name="a", node=NarRegular(contents=b"hello"))]))
        root = NarPath.from_nar(data)
        assert root.is_dir()
        assert (root / "a").is_file()
        assert (root / "a").read_bytes() == b"hello"

    async def test_construction_from_node(self) -> None:
        root = NarPath.from_node(NarDirectory())
        assert root.is_dir()

    async def test_navigation(self) -> None:
        root = NarPath.from_node(NarDirectory())
        a = root / "a"
        ab = a / "b"
        assert ab._parts == ("a", "b")
        assert ab.parent == a
        assert ab.name == "b"
        assert str(ab) == "/a/b"
        assert repr(ab) == "NarPath('/a/b')"

    async def test_navigation_with_dot_and_dotdot(self) -> None:
        root = NarPath.from_node(NarDirectory())
        a = root / "a" / "b" / ".." / "c"
        assert a._parts == ("a", "c")

    async def test_navigation_with_slash(self) -> None:
        root = NarPath.from_node(NarDirectory())
        p = root / "a/b/c"
        assert p._parts == ("a", "b", "c")

    async def test_type_checks(self) -> None:
        root = NarPath.from_node(
            NarDirectory(
                entries=[
                    NarDirectoryEntry(name="file", node=NarRegular(contents=b"x")),
                    NarDirectoryEntry(name="dir", node=NarDirectory()),
                    NarDirectoryEntry(name="link", node=NarSymlink(target="file")),
                ]
            )
        )
        assert (root / "file").is_file()
        assert (root / "file").exists()
        assert not (root / "file").is_dir()
        assert not (root / "file").is_symlink()
        assert (root / "dir").is_dir()
        assert (root / "link").is_symlink()
        assert not (root / "missing").exists()
        assert not (root / "missing").is_file()
        assert not (root / "missing").is_dir()

    async def test_iterdir(self) -> None:
        root = NarPath.from_node(
            NarDirectory(
                entries=[
                    NarDirectoryEntry(name="a", node=NarRegular(contents=b"1")),
                    NarDirectoryEntry(name="b", node=NarRegular(contents=b"2")),
                ]
            )
        )
        names = {p.name for p in root.iterdir()}
        assert names == {"a", "b"}

    async def test_iterdir_not_a_directory(self) -> None:
        root = NarPath.from_node(NarRegular(contents=b"x"))
        with pytest.raises(ValueError, match="Not a directory"):
            list(root.iterdir())

    async def test_read_bytes(self) -> None:
        root = NarPath.from_node(
            NarDirectory(entries=[NarDirectoryEntry(name="file", node=NarRegular(contents=b"hello"))])
        )
        assert (root / "file").read_bytes() == b"hello"

    async def test_read_text(self) -> None:
        root = NarPath.from_node(
            NarDirectory(entries=[NarDirectoryEntry(name="file", node=NarRegular(contents=b"hello"))])
        )
        assert (root / "file").read_text() == "hello"

    async def test_read_not_a_file(self) -> None:
        root = NarPath.from_node(NarDirectory())
        with pytest.raises(ValueError, match="Not a regular file"):
            root.read_bytes()

    async def test_write_bytes_new_file(self) -> None:
        root = NarPath.from_node(NarDirectory())
        root = (root / "file").write_bytes(b"hello")
        assert (root / "file").read_bytes() == b"hello"
        assert not (root / "file").is_dir()

    async def test_write_bytes_replace(self) -> None:
        root = NarPath.from_node(
            NarDirectory(entries=[NarDirectoryEntry(name="file", node=NarRegular(contents=b"old"))])
        )
        root = (root / "file").write_bytes(b"new")
        assert (root / "file").read_bytes() == b"new"

    async def test_write_bytes_executable(self) -> None:
        root = NarPath.from_node(NarDirectory())
        root = (root / "script").write_bytes(b"#!/bin/sh", executable=True)
        node = (root / "script")._resolve()
        assert isinstance(node, NarRegular)
        assert node.executable

    async def test_write_text(self) -> None:
        root = NarPath.from_node(NarDirectory())
        root = (root / "file").write_text("hello")
        assert (root / "file").read_text() == "hello"

    async def test_write_bytes_missing_parent(self) -> None:
        root = NarPath.from_node(NarDirectory())
        with pytest.raises(ValueError, match="No such directory"):
            (root / "a" / "file").write_bytes(b"data")

    async def test_write_bytes_on_directory(self) -> None:
        root = NarPath.from_node(NarDirectory(entries=[NarDirectoryEntry(name="a", node=NarDirectory())]))
        with pytest.raises(ValueError, match="Is a directory"):
            (root / "a").write_bytes(b"data")

    async def test_mkdir(self) -> None:
        root = NarPath.from_node(NarDirectory())
        root = (root / "a").mkdir()
        assert (root / "a").is_dir()

    async def test_mkdir_parents(self) -> None:
        root = NarPath.from_node(NarDirectory())
        root = (root / "a" / "b").mkdir(parents=True)
        assert (root / "a").is_dir()
        assert (root / "a" / "b").is_dir()

    async def test_mkdir_existing(self) -> None:
        root = NarPath.from_node(NarDirectory(entries=[NarDirectoryEntry(name="a", node=NarDirectory())]))
        root = (root / "a").mkdir()
        assert (root / "a").is_dir()

    async def test_mkdir_existing_file(self) -> None:
        root = NarPath.from_node(NarDirectory(entries=[NarDirectoryEntry(name="a", node=NarRegular(contents=b"x"))]))
        with pytest.raises(ValueError, match="File exists"):
            (root / "a").mkdir()

    async def test_unlink(self) -> None:
        root = NarPath.from_node(NarDirectory(entries=[NarDirectoryEntry(name="a", node=NarRegular(contents=b"x"))]))
        root = (root / "a").unlink()
        assert not (root / "a").exists()
        assert root.is_dir()

    async def test_unlink_root(self) -> None:
        root = NarPath.from_node(NarDirectory())
        with pytest.raises(ValueError, match="Cannot unlink root"):
            root.unlink()

    async def test_unlink_missing(self) -> None:
        root = NarPath.from_node(NarDirectory())
        with pytest.raises(ValueError, match="No such file"):
            (root / "a").unlink()

    async def test_chmod(self) -> None:
        root = NarPath.from_node(
            NarDirectory(entries=[NarDirectoryEntry(name="script", node=NarRegular(contents=b"x", executable=False))])
        )
        root = (root / "script").chmod(executable=True)
        node = (root / "script")._resolve()
        assert isinstance(node, NarRegular)
        assert node.executable

    async def test_chmod_not_a_file(self) -> None:
        root = NarPath.from_node(NarDirectory())
        with pytest.raises(ValueError, match="Not a regular file"):
            root.chmod()

    async def test_immutability(self) -> None:
        root = NarPath.from_node(NarDirectory())
        child = root / "a"
        new_root = child.mkdir()
        assert not child.exists()
        assert (new_root / "a").exists()

    async def test_to_nar_roundtrip(self) -> None:
        root = NarPath.from_node(NarDirectory())
        root = (root / "a").mkdir()
        root = (root / "a" / "file").write_text("hello")
        root = (root / "a" / "script").write_bytes(b"#!/bin/sh", executable=True)
        root = (root / "b").write_bytes(b"world")
        data = root.to_nar()
        restored = NarPath.from_nar(data)
        assert (restored / "a" / "file").read_text() == "hello"
        assert (restored / "a" / "script").read_bytes() == b"#!/bin/sh"
        script = (restored / "a" / "script")._resolve()
        assert isinstance(script, NarRegular)
        assert script.executable
        assert (restored / "b").read_bytes() == b"world"

    async def test_complex_build(self) -> None:
        root = NarPath.from_node(NarDirectory())
        root = (root / "nix" / "store" / "abc").mkdir(parents=True)
        root = (root / "nix" / "store" / "abc" / "bin").mkdir(parents=True)
        root = (root / "nix" / "store" / "abc" / "lib").mkdir(parents=True)
        root = (root / "nix" / "store" / "abc" / "bin" / "hello").write_text("hi", executable=True)
        root = (root / "nix" / "store" / "abc" / "lib" / "libhello.so").write_bytes(b"\x7fELF")
        root = (root / "nix" / "store" / "abc" / "README").write_text("Read me")
        assert (root / "nix" / "store" / "abc" / "bin" / "hello").read_text() == "hi"
        assert (root / "nix" / "store" / "abc" / "lib" / "libhello.so").read_bytes() == b"\x7fELF"
        assert (root / "nix" / "store" / "abc" / "README").read_text() == "Read me"
        restored = NarPath.from_nar(root.to_nar())
        assert (restored / "nix" / "store" / "abc" / "bin" / "hello").read_text() == "hi"
        assert (restored / "nix" / "store" / "abc" / "lib" / "libhello.so").read_bytes() == b"\x7fELF"
