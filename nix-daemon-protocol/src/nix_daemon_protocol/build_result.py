"""BuildResult and related types — build outcome on the Nix daemon wire."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import IntEnum

from .constants import FEATURE_REALISATION_WITH_PATH, proto
from .opt_microseconds import OptMicroseconds
from .realisation import Realisation
from .store_dir import store_prefix
from .store_path import StorePath
from .unkeyed_realisation import UnkeyedRealisation
from .wire_message import WireField, WireModel

# The highest status byte a Nix client can decode. `buildResultStatusTable` in
# `src/libstore/common-protocol.cc` holds 15 entries, and the reader rejects any
# index at or above that size. `BuildResult.wire_status` is what keeps this
# project inside it.
MAX_WIRE_STATUS = 14


class BuildResultStatus(IntEnum):
    """Build result status codes from nix daemon protocol."""

    BUILT = 0
    SUBSTITUTED = 1
    ALREADY_VALID = 2
    RESOLVES_TO_ALREADY_VALID = 13

    PERMANENT_FAILURE = 3
    INPUT_REJECTED = 4
    OUTPUT_REJECTED = 5
    TRANSIENT_FAILURE = 6
    CACHED_FAILURE = 7
    TIMED_OUT = 8
    MISC_FAILURE = 9
    DEPENDENCY_FAILED = 10
    LOG_LIMIT_EXCEEDED = 11
    NOT_DETERMINISTIC = 12
    NO_SUBSTITUTERS = 14

    HASH_MISMATCH = 101
    UNKNOWN = 102

    @property
    def is_success(self) -> bool:
        return self in {
            self.BUILT,
            self.SUBSTITUTED,
            self.ALREADY_VALID,
            self.RESOLVES_TO_ALREADY_VALID,
        }

    @property
    def is_failure(self) -> bool:
        return not self.is_success


class BuildMode(IntEnum):
    """Build mode flags from nix daemon protocol."""

    NORMAL = 0
    REPAIR = 1
    CHECK = 2


@dataclass
class BuiltOutput:
    """A built output - either plain path or content-addressed (CA) format."""

    out_path: StorePath = field(default_factory=lambda: StorePath(path=""))
    ca: str = ""
    hash: str = ""
    hash_algo: str = ""
    nar_hash: str = ""
    nar_size: int = 0
    reference: str = ""

    @classmethod
    def from_string(cls, s: str) -> BuiltOutput:
        if not s:
            return cls()
        try:
            data = json.loads(s)
            if isinstance(data, dict):
                return cls(
                    out_path=StorePath(path=data.get("outPath", "")),
                    ca=data.get("ca", ""),
                    hash=data.get("hash", ""),
                    hash_algo=data.get("hashAlgo", ""),
                    nar_hash=data.get("narHash", ""),
                    nar_size=data.get("narSize", 0),
                    reference=data.get("reference", ""),
                )
        except (json.JSONDecodeError, TypeError):
            pass
        return cls(out_path=StorePath(path=s))

    def to_string(self) -> str:
        if self.ca or self.hash or self.nar_hash:
            data: dict[str, str | int] = {"outPath": str(self.out_path)}
            if self.ca:
                data["ca"] = self.ca
            if self.hash:
                data["hash"] = self.hash
            if self.hash_algo:
                data["hashAlgo"] = self.hash_algo
            if self.nar_hash:
                data["narHash"] = self.nar_hash
            if self.nar_size:
                data["narSize"] = self.nar_size
            if self.reference:
                data["reference"] = self.reference
            return json.dumps(data)
        return str(self.out_path)


def _whole_path(path: object) -> StorePath:
    """A store path with the store directory in front of it.

    **The two shapes of a realisation spell a path differently.** The JSON of
    `Realisation` carries `<hash>-<name>`, which is `StorePath::to_string` of
    Nix, and `UnkeyedRealisation` carries a `StorePath` on the wire, which is
    the whole path. A bare name where the wire wants a whole path makes the
    peer answer "not an absolute path: '...'", and `ca:build-cache` read that.
    Issue #14.
    """
    text = str(path)
    if not text or text.startswith("/"):
        return StorePath(text)
    return StorePath(store_prefix() + text)


def _bare_path(path: object) -> StorePath:
    """A store path with the store directory taken off. See `_whole_path`."""
    text = str(path)
    prefix = store_prefix()
    return StorePath(text.removeprefix(prefix))


class BuildResult(WireModel):
    """Nix daemon protocol BuildResult.

    Fields present based on protocol version:
    - All versions: status, error_msg
    - >= 1.29: times_built, is_non_deterministic, start_time, stop_time
    - >= 1.37: cpu_user, cpu_system
    - >= 1.28: built_outputs (dict[str, str])
    """

    status: int = 0
    error_msg: str = WireField(default="", text_errors="surrogateescape")
    """What the daemon said about the failure, byte for byte.

    `BuildResult.errorMsg` is a `std::string` that the daemon forwards
    without validating, and pynixd quotes `LogError.msg` into it on the
    `queue.fail` path -- both already carry bytes no UTF-8 decoder accepts.
    Strict decoding would move the crash of issue nixidae/nixidae#662 from
    the log stream to the result that follows it.
    """

    # Protocol 1.29 fields
    times_built: int | None = WireField(default=None, min_version=proto(1, 29))
    is_non_deterministic: int | None = WireField(default=None, min_version=proto(1, 29))
    start_time: int | None = WireField(default=None, min_version=proto(1, 29))
    stop_time: int | None = WireField(default=None, min_version=proto(1, 29))

    # Protocol 1.37 fields
    cpu_user: OptMicroseconds = WireField(default_factory=OptMicroseconds, min_version=proto(1, 37))
    cpu_system: OptMicroseconds = WireField(default_factory=OptMicroseconds, min_version=proto(1, 37))

    # Protocol 1.28 fields
    #
    # **`builtOutputs` of Nix is two fields, and a feature picks which one.**
    # `worker-protocol.cc:268` writes it as an if/else. With
    # `realisation-with-path-not-hash` it is a map of output name to
    # `UnkeyedRealisation`; without it, and from 1.28, it is a map of
    # `"<drvHash>!<output>"` to a JSON `Realisation`. Both live at 1.38, so a
    # version alone cannot separate them. Issue #14.
    built_outputs: dict[str, Realisation] | None = WireField(
        default=None,
        min_version=proto(1, 28),
        unless_features=[FEATURE_REALISATION_WITH_PATH],
    )
    built_outputs_by_name: dict[str, UnkeyedRealisation] | None = WireField(
        default=None,
        needs_features=[FEATURE_REALISATION_WITH_PATH],
    )
    """The map that the feature shape carries, keyed by the output name alone.

    The key of `built_outputs` is a whole `DrvOutput`; this one names the
    output of the derivation that the answer is already about, so it needs no
    derivation in the key.

    **The two spell a path differently.** This one carries a whole store path,
    because the wire writes it as a `StorePath`. The JSON of `Realisation`
    carries the bare `<hash>-<name>`. `_whole_path` and `_bare_path` are the
    translation, and `ca:build-cache` measured what skipping it costs.
    """

    def realised_outputs(self) -> dict[str, Realisation]:
        """Every output this result realised, keyed by the **output name**.

        One accessor for the two wire shapes, so a caller needs no branch.
        `built_outputs` keys by a whole `DrvOutput`, which is
        `"<drvHash>!<output>"`, and `built_outputs_by_name` keys by the output
        name alone. This takes the part after the `!` in the first case.

        **A realisation of the feature shape carries no id.** The request
        already named the derivation, so `worker-protocol.cc:268` writes the
        output name as the key and an `UnkeyedRealisation` as the value. The
        `Realisation` this builds therefore has an empty `id`, and a caller
        that needs the id must build it from the derivation it already holds.
        No caller in this repository does: each one reads `out_path` and the
        key. Issue #14.
        """
        if self.built_outputs_by_name:
            return {
                name: Realisation(out_path=_bare_path(value.out_path), signatures=sorted(value.signatures))
                for name, value in self.built_outputs_by_name.items()
            }
        if self.built_outputs:
            return {key.rpartition("!")[2] or key: value for key, value in self.built_outputs.items()}
        return {}

    def wire_status(self) -> int:
        """The status byte to send, which is not always the one held.

        A hash mismatch is a kind of output rejection, which is the mapping Nix
        chose and the one a client expects. Everything else out of range
        becomes a plain failure: the detail belongs in `error_msg`, which has
        no such limit, and a status a client cannot read carries no detail at
        all.
        """
        if self.status == BuildResultStatus.HASH_MISMATCH:
            return int(BuildResultStatus.OUTPUT_REJECTED)
        if 0 <= self.status <= MAX_WIRE_STATUS:
            return self.status
        return int(BuildResultStatus.MISC_FAILURE)

    def for_the_wire(self, features: frozenset[str] | None = None) -> BuildResult:
        """This result, with a status a client can decode and a map it can read.

        Call this before sending a result to a client. It is a method and not a
        `to_writer` override on purpose: `experimental_compiled` refuses to
        compile a model that overrides its codec (`_can_compile`), and
        `BuildResult` is the model its own tests compile as the example. A
        correctness fix that quietly turned off the fast path for the hottest
        model on the wire would be a poor trade, so the knowledge of what is
        wire-safe lives here and the caller decides when to apply it.

        **A proxy reads a result on one connection and writes it on another,
        and the two negotiate their features apart.** A backend that offers
        `realisation-with-path-not-hash` fills `built_outputs_by_name` and
        leaves `built_outputs` at `None`; a client that offers nothing then
        reads `built_outputs`, and a `None` where a map belongs raises in the
        writer. Give *features* the set of the peer this result is going to,
        and this fills the field that peer will read.

        **The fill from the feature shape to the old one is lossy.** The old
        key is a whole `DrvOutput`, which carries the hash of the derivation,
        and the feature shape carries no hash anywhere. This keys by the
        output name and leaves the id empty, so the output path survives and
        the id does not. Building the real id means reading the derivation
        and hashing it, which this model cannot do. Issue #14.
        """
        update: dict[str, object] = {}

        wire_status = self.wire_status()
        if wire_status != self.status:
            update["status"] = wire_status

        if features is not None:
            wants_the_feature = FEATURE_REALISATION_WITH_PATH in features
            if wants_the_feature and self.built_outputs_by_name is None:
                update["built_outputs_by_name"] = {
                    name: UnkeyedRealisation(out_path=_whole_path(value.out_path), signatures=set(value.signatures))
                    for name, value in self.realised_outputs().items()
                    if value.out_path is not None
                }
            elif not wants_the_feature and self.built_outputs is None:
                update["built_outputs"] = dict(self.realised_outputs())

        if not update:
            return self
        return self.model_copy(update=update)
