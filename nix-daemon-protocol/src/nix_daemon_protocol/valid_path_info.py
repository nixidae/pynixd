"""ValidPathInfo — StorePath + UnkeyedValidPathInfo as nested fields."""

from __future__ import annotations

from typing import Any

from .content_address import ContentAddress
from .context import WriteContext
from .io import BytesWriter
from .nar_hash import NARHash
from .path_info import UnkeyedValidPathInfo
from .signature import Signature
from .store_dir import store_prefix
from .store_path import StorePath
from .wire_message import WireModel
from .wire_time import Time


class ValidPathInfo(WireModel):
    """Wire mirror of ValidPathInfo.

    Wire order: ``path`` then all ``info`` fields inline.
    ``WireModel`` serializes nested ``WireModel`` fields inline,
    so this produces the correct flat wire format.
    """

    path: StorePath
    info: UnkeyedValidPathInfo

    async def bytes_wire(self, version: int = 0, features: frozenset[str] = frozenset()) -> bytes:
        """Serialize this ValidPathInfo to bytes (in-memory BytesWriter is sync)."""
        buf = BytesWriter()
        ctx = WriteContext(writer=buf, version=version, features=features)
        await self.to_writer(ctx)
        return buf.bytes()

    @classmethod
    def from_narinfo(cls, content: str, store_dir: str | None = None) -> ValidPathInfo:
        data: dict[str, Any] = {
            "references": set(),
            "sigs": set(),
        }
        for line in content.splitlines():
            line = line.strip()
            if not line or ":" not in line or line.startswith("#"):
                continue
            key, val = line.split(":", 1)
            key = key.strip()
            val = val.strip()

            if key == "StorePath":
                # `nar-info.cc:50`: the cache's own store directory, which *store_dir* names.
                data["path"] = StorePath(path=val) if store_dir is None else StorePath.in_store(val, store_dir)
            elif key == "NarHash":
                data["nar_hash"] = NARHash(hash=val.removeprefix("sha256:"))
            elif key == "NarSize":
                data["nar_size"] = int(val)
            elif key == "References":
                for ref in val.split():
                    if ref:
                        ref_path = ref if ref.startswith(store_prefix()) else f"{store_prefix()}{ref}"
                        data["references"].add(StorePath(path=ref_path))
            elif key == "Deriver":
                if val and val != "unknown-deriver":
                    deriver = val if val.startswith(store_prefix()) else f"{store_prefix()}{val}"
                    data["deriver"] = StorePath(path=deriver)
                else:
                    data["deriver"] = None
            elif key == "Sig":
                data["sigs"].add(Signature(**Signature.from_str(val)))
            elif key == "CA":
                data["ca"] = ContentAddress(value=val)
            elif key == "Provenance":
                # `nar-info.cc:94` of Determinate v3.24.0: one JSON value on the line.
                data["provenance"] = val

        info = UnkeyedValidPathInfo(
            deriver=data.get("deriver"),
            nar_hash=data.get("nar_hash", NARHash(hash="")),
            references=data.get("references", set()),
            registration_time=Time(ts=data.get("registration_time", 0)),
            nar_size=data.get("nar_size", 0),
            ultimate=bool(data.get("ultimate", False)),
            sigs=data.get("sigs", set()),
            ca=data.get("ca", ContentAddress(value="")),
            provenance=data.get("provenance", ""),
        )
        return cls(path=data.get("path", StorePath(path="")), info=info)

    def to_narinfo(self) -> str:
        nar_hash = str(self.info.nar_hash)
        if not nar_hash.startswith("sha256:"):
            nar_hash = f"sha256:{nar_hash}"

        path_hash = self.path.hash_part()
        lines = [
            f"StorePath: {self.path}",
            f"URL: nar/{path_hash}.nar",
            "Compression: none",
            f"NarHash: {nar_hash}",
            f"NarSize: {self.info.nar_size}",
        ]

        if self.info.references:
            refs = " ".join(sorted(ref.name for ref in self.info.references))
            lines.append(f"References: {refs}")

        if self.info.deriver:
            lines.append(f"Deriver: {self.info.deriver.name}")

        lines.extend(f"Sig: {sig.to_str()}" for sig in sorted(self.info.sigs, key=str))

        if self.info.ca:
            lines.append(f"CA: {self.info.ca}")

        if self.info.provenance:
            lines.append(f"Provenance: {self.info.provenance}")

        return "\n".join(lines) + "\n"
