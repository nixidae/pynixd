"""What an upload to an S3 binary cache writes: the NAR object, its parts, and the narinfo.

The layout is Nix's (`binary-cache-store.cc:addToStoreCommon`), so stock Nix reads the bucket. One
difference: the NAR object is named by the NAR hash, not the file hash. The NAR hash is known before
the upload starts, so the compressed NAR streams to the bucket in parts and needs no spool to learn
its own name. Two paths with one NAR share one object.
"""

from __future__ import annotations

import hashlib
import lzma
import math
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Final, Protocol
from xml.etree import ElementTree

import zstandard as zstd

from pynixd_store.signing import nar_hash_for_a_fingerprint
from pynixd_store.utils import nix32_encode

if TYPE_CHECKING:
    from nix_daemon_protocol.valid_path_info import ValidPathInfo

    from ..config import S3Compression as Compression

EXTENSION: Final[dict[Compression, str]] = {"zstd": ".zst", "xz": ".xz", "none": ""}
"""`nar/<hash>.nar` and this, as `addToStoreCommon` names the file (`binary-cache-store.cc:197`)."""

MIN_PART: Final = 5 * 1024 * 1024
"""S3's and R2's smallest part but the last."""

MAX_PARTS: Final = 10_000
"""S3's and R2's most parts in one upload."""

NARINFO_TYPE: Final = "text/x-nix-narinfo"
NAR_TYPE: Final = "application/x-nix-nar"
CACHE_INFO_TYPE: Final = "text/x-nix-cache-info"

_S3_XMLNS: Final = "http://s3.amazonaws.com/doc/2006-03-01/"


class Uploaded(StrEnum):
    """What an upload of one path found or did."""

    UPLOADED = "uploaded"
    """The narinfo PUT answered 2xx after its NAR's did."""
    PRESENT = "present"
    """The bucket had the narinfo before the upload started."""


class _Compressor(Protocol):
    def compress(self, data: bytes, /) -> bytes: ...

    def flush(self) -> bytes: ...


class _Identity:
    def compress(self, data: bytes, /) -> bytes:
        return data

    def flush(self) -> bytes:
        return b""


def compressor(compression: Compression) -> _Compressor:
    """A streaming compressor for *compression*, as Nix's `makeCompressionSink` picks one."""
    if compression == "zstd":
        return zstd.ZstdCompressor(level=3).compressobj()
    if compression == "xz":
        return lzma.LZMACompressor(lzma.FORMAT_XZ)
    return _Identity()


def nar_object(info: ValidPathInfo, compression: Compression) -> str:
    """The NAR object's key under the cache prefix: `nar/<NAR hash>.nar` and the extension."""
    digest = nar_hash_for_a_fingerprint(info.info.nar_hash).removeprefix("sha256:")
    return f"nar/{digest}.nar{EXTENSION[compression]}"


def part_size(nar_size: int, chunk_size: int) -> int:
    """The size of every part but the last.

    R2 refuses an upload whose parts before the last differ in size, so one size holds for the whole
    upload. It must fit the compressed NAR in `MAX_PARTS` before the size is known: zstd and xz grow
    data that does not compress by well under 1 %, so the bound is the NAR size and a margin.
    """
    bound = nar_size + nar_size // 64 + MIN_PART
    return max(chunk_size, MIN_PART, math.ceil(bound / MAX_PARTS))


@dataclass
class Parts:
    """Compressed bytes cut into parts of exactly *size* bytes, the rest held for the last."""

    size: int
    _held: bytearray = field(default_factory=bytearray)

    def feed(self, data: bytes) -> list[bytes]:
        """The parts *data* completes."""
        self._held += data
        full = []
        while len(self._held) >= self.size:
            full.append(bytes(self._held[: self.size]))
            del self._held[: self.size]
        return full

    def rest(self) -> bytes:
        """What remains after the last full part; empty when the data ended on a part boundary."""
        rest = bytes(self._held)
        self._held.clear()
        return rest


class FileDigest:
    """The `FileHash` and `FileSize` of the compressed NAR, counted as it streams."""

    def __init__(self) -> None:
        self._sha256 = hashlib.sha256()
        self.size = 0

    def update(self, data: bytes) -> None:
        self._sha256.update(data)
        self.size += len(data)

    @property
    def nix32(self) -> str:
        return nix32_encode(self._sha256.digest())


def narinfo_text(info: ValidPathInfo, *, url: str, compression: Compression, file_hash: str, file_size: int) -> str:
    """`NarInfo::to_string` (`nar-info.cc:108`): its fields, order and hash forms.

    `References:` is written when empty too, as Nix writes it. Every `Sig:` of *info* stays.
    """
    lines = [
        f"StorePath: {info.path}",
        f"URL: {url}",
        f"Compression: {compression}",
        f"FileHash: sha256:{file_hash}",
        f"FileSize: {file_size}",
        f"NarHash: {nar_hash_for_a_fingerprint(info.info.nar_hash)}",
        f"NarSize: {info.info.nar_size}",
        "References: " + " ".join(sorted(ref.name for ref in info.info.references)),
    ]
    if info.info.deriver:
        lines.append(f"Deriver: {info.info.deriver.name}")
    lines.extend(f"Sig: {sig}" for sig in sorted(info.info.sigs))
    if info.info.ca:
        lines.append(f"CA: {info.info.ca}")
    return "\n".join(lines) + "\n"


def cache_info_text(store_dir: str) -> str:
    """What `BinaryCacheStore::init` writes into a cache that has no `nix-cache-info` (`binary-cache-store.cc:50`)."""
    return f"StoreDir: {store_dir}\n"


def complete_body(etags: list[str]) -> bytes:
    """The `CompleteMultipartUpload` request: each part's number and ETag, in order."""
    root = ElementTree.Element("CompleteMultipartUpload", xmlns=_S3_XMLNS)
    for number, etag in enumerate(etags, start=1):
        part = ElementTree.SubElement(root, "Part")
        ElementTree.SubElement(part, "PartNumber").text = str(number)
        ElementTree.SubElement(part, "ETag").text = etag
    body: bytes = ElementTree.tostring(root, encoding="utf-8", xml_declaration=True)
    return body


def _local(tag: str) -> str:
    return tag.rpartition("}")[2]


def _child_text(root: ElementTree.Element, name: str) -> str | None:
    return next((child.text or "" for child in root if _local(child.tag) == name), None)


def upload_id(body: bytes) -> str:
    """The `UploadId` of a `CreateMultipartUpload` answer; `ValueError` when it has none."""
    found = _child_text(ElementTree.fromstring(body), "UploadId")
    if not found:
        raise ValueError(f"CreateMultipartUpload answered no UploadId: {body[:200]!r}")
    return found


def error_of(body: bytes) -> str | None:
    """`Code: Message` when *body* is an S3 `<Error>` document, else `None`.

    `CompleteMultipartUpload` can answer 200 and still fail: the error is in the body.
    """
    if not body.strip():
        return None
    try:
        root = ElementTree.fromstring(body)
    except ElementTree.ParseError:
        return None
    if _local(root.tag) != "Error":
        return None
    return f"{_child_text(root, 'Code')}: {_child_text(root, 'Message')}"
