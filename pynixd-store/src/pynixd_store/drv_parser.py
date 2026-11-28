"""
Parser for Nix .drv files (ATerm format).

Parses the ATerm representation into a structured Derivation.
Handles Derive(...) and DrvWithVersion("xp-dyn-drv",...) formats.

ATerm .drv format (Traditional Derive):
    Derive(
        [("out","/nix/store/...","",""),...],       -- outputs
        [("/nix/store/...drv",["out"]),...],        -- inputDrvs (simple)
        ["/nix/store/..."],                          -- inputSrcs
        "x86_64-linux",                              -- platform
        "/nix/store/.../bin/bash",                   -- builder
        ["-e","/nix/store/.../builder.sh"],          -- args
        [("key","value"),...]                        -- env
    )

ATerm .drv format (Dynamic DrvWithVersion):
    DrvWithVersion("xp-dyn-drv",
        [("out","/nix/store/...","",""),...],       -- outputs (same as above)
        [("/nix/store/...drv",(["out"],[...nested...])),...], -- inputDrvs (dynamic)
        ...
    )

Output fields: (name, path, hash_algo, hash_value)

Compatibility with serde DerivationOutput:
  DrvOutput: hash_algo, hash_value, output_name, path  (parser - raw ATerm fields)
  DerivationOutput: name, path, method, hash_digest  (wire protocol)
  Mapping: name->name, path->path, hash_algo->method, hash_value->hash_digest
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict

import anyio
import msgspec

from nix_daemon_protocol.basic_derivation import BasicDerivation, bool_attr, required_features_of
from nix_daemon_protocol.derivation_output import DerivationOutput, OutputKind
from nix_daemon_protocol.store_dir import store_dir

from .content_address import (
    HASH_SIZES,
    ContentAddress,
    check_name,
    content_addressed_path,
    fixed_output_address,
    output_method,
)
from .errors import PynixdError
from .hintfmt import magenta
from .store_path import DrvOutput, StorePath
from .utils import compress_hash, nix32_encode

if TYPE_CHECKING:
    from collections.abc import Callable, Collection

    from nix_daemon_protocol.aliases import OutputMap, StorePathSet

    from .store_layout import StoreLayout

BUILDER_RPC = "builder-rpc-v0"
"""`drvFeatureBuilderRpcV0`: the builder submits its outputs over its daemon (`derivations.hh:24`)."""

DRV_TEXT_ERRORS = "surrogateescape"
"""A derivation is bytes to Nix (`derivations.cc:224-275`, `:533-561`).

An env value with byte 0xff builds under nix-daemon; a strict decode
failed it in pynixd. Each byte round-trips through `str`. nixidae/nixidae#1010."""


def drv_text(raw: bytes) -> str:
    """Derivation bytes as text, losslessly."""
    return raw.decode("utf-8", DRV_TEXT_ERRORS)


def drv_bytes(text: str) -> bytes:
    """Derivation text back to the bytes it came from."""
    return text.encode("utf-8", DRV_TEXT_ERRORS)


# An input drv node for unparse: (output_names, {output_name: child node}).
type _DrvInputNode = tuple[list[str], dict[str, "_DrvInputNode"]]


def _child_map_to_drv_node(node: ChildMapNode, prefix_outputs: list[str] | None = None) -> _DrvInputNode:
    """Convert a ChildMapNode to _DrvInputNode for serialization.

    Combines ``prefix_outputs`` (flat outputs from ``input_drvs``) with
    the node's own outputs and recursively converts children.
    """
    # `DerivedPathMap<StringSet>` holds a set: sorted, each name once.
    all_outputs = sorted({*(prefix_outputs or []), *node.outputs})
    children = {name: _child_map_to_drv_node(child) for name, child in node.children.items()}
    return (all_outputs, children)


@dataclass
class ChildMapNode:
    """A node in the recursive DerivedPathMapNode tree.

    Maps to the ATerm format: ``([flat_outs],[(output_name, child_node), ...])``

    Each node has:
    - outputs: flat output names at this level (leaf outputs).
    - children: nested {output_name: ChildMapNode} map for deeper levels.
    """

    outputs: list[str] = field(default_factory=list)
    children: dict[str, ChildMapNode] = field(default_factory=dict)

    def is_leaf(self) -> bool:
        """True if this node has no further nesting."""
        return not self.children

    def direct_outputs(self) -> list[str]:
        """Return the output names referenced at this level.

        This includes both flat outputs and child keys, since both
        represent output references at this level of nesting.
        """
        result: list[str] = list(self.outputs)
        result.extend(self.children)
        return result

    def to_dict(self) -> dict[str, Any]:
        """Serialize to JSON-compatible dict."""
        result: dict[str, Any] = {"outputs": self.outputs}
        if self.children:
            result["dynamicOutputs"] = {name: child.to_dict() for name, child in self.children.items()}
        return result


class NixDerivationOutputShow(TypedDict, total=False):
    """Output entry in `nix derivation show` JSON."""

    path: str
    hashAlgo: str
    hash: str


class NixInputDrvShow(TypedDict):
    """Input derivation entry in `nix derivation show` JSON."""

    dynamicOutputs: dict[str, NixInputDrvShow]
    outputs: list[str]


class NixDerivationShow(TypedDict):
    """Complete derivation object in `nix derivation show` JSON."""

    args: list[str]
    builder: str
    env: dict[str, str]
    inputDrvs: dict[str, NixInputDrvShow]
    inputSrcs: list[str]
    name: str
    outputs: dict[str, NixDerivationOutputShow]
    system: str


def _aterm_escape(s: str) -> str:
    s = s.replace("\\", "\\\\")
    s = s.replace('"', '\\"')
    s = s.replace("\n", "\\n")
    s = s.replace("\r", "\\r")
    return s.replace("\t", "\\t")


@dataclass
class Derivation:
    """A parsed .drv file."""

    outputs: list[DrvOutput] = field(default_factory=list)

    input_drvs: dict[StorePath, list[str]] = field(default_factory=dict)

    input_srcs: StorePathSet = field(default_factory=set)

    platform: str = ""
    builder: str = ""
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)

    is_dynamic: bool = False
    """True if DrvWithVersion("xp-dyn-drv",...) format (dynamic derivations)."""

    dynamic_input_drvs: dict[StorePath, ChildMapNode] = field(
        default_factory=dict,
    )
    """{drv_path: ChildMapNode} for DrvWithVersion dynamic inputs."""

    @property
    def required_system_features(self) -> set[str]:
        return required_features_of(self.env)

    @property
    def allow_substitutes(self) -> bool:
        """`allowSubstitutes`, true unless the derivation says otherwise."""
        return bool_attr(self.env, "allowSubstitutes", default=True)

    def output_paths(self) -> dict[str, StorePath]:
        """Return {output_name: output_path} for all outputs."""
        return {o.name: StorePath(o.path) for o in self.outputs}

    def selected_output_paths(self, wanted: Collection[str]) -> dict[str, StorePath]:
        """The path of each output that *wanted* names, by name.

        `{"*"}` means every output, which is what a derived path with no
        output list means. An output with no path is in the answer, with an
        empty path: the name is what asks the store for a realisation.
        """
        paths = self.output_paths()
        if wanted == {"*"}:
            return paths
        return {name: path for name, path in paths.items() if name in wanted}

    def output_kinds(self) -> list[OutputKind]:
        """Return the OutputKind for each output.

        Avoids callers needing to construct DerivationOutput objects.
        """
        result: list[OutputKind] = []
        for o in self.outputs:
            dop = DerivationOutput(
                path=o.path,
                method=o.hash_algo,
                hash_digest=o.hash_value,
            )
            result.append(dop.kind)
        return result

    def fixed_out_address(self) -> ContentAddress | None:
        """`getDerivationCA` (`misc.cc:91`): the address of the output `out`, when it is fixed."""
        for o in self.outputs:
            output = DerivationOutput(path=o.path, method=o.hash_algo, hash_digest=o.hash_value)
            if o.output_name == "out" and output.kind is OutputKind.CA_FIXED:
                return fixed_output_address(o.hash_algo, o.hash_value)
        return None

    @property
    def is_fixed_output(self) -> bool:
        """True when every output takes its path from a hash the derivation names.

        `DerivationType::isFixed` at `derivation-options.hh`. Such a
        derivation has one hash for each output, and that hash does not
        depend on the derivation at all. `hashDerivationModulo` at
        `derivations.cc:902` branches on this.
        """
        return all(kind is OutputKind.CA_FIXED for kind in self.output_kinds())

    @property
    def is_impure(self) -> bool:
        """True when no existing output of this derivation counts as built.

        `DerivationGoal` at `derivation-goal.cc:87` skips `checkPathValidity`
        for an impure derivation, and the comment there gives the reason: "We
        don't yet have any safe way to cache an impure derivation at this
        step." So Nix builds it every time, and the output of the last build
        is not an answer.

        pynixd read the realisation of the last build and answered with it, so
        a second `nix build` of an impure derivation gave the first output.
        `impure-derivations.sh:36` of the functional suite states the rule: the
        builder writes a counter, and the second build must write `1`.
        """
        return OutputKind.IMPURE in self.output_kinds()

    @property
    def needs_realisations(self) -> bool:
        """True when the store holds a realisation for each output of this.

        Nix registers a realisation for every output of every derivation while
        `ca-derivations` is on, at `derivation-builder.cc:1994` and again at
        `derivation-goal.cc:236`. It asks the setting, and pynixd cannot: a
        daemon with the feature off answers "experimental Nix feature
        'ca-derivations' is disabled" to `RegisterDrvOutput`, and pynixd then
        discards a good connection as dirty.

        So this asks the derivation instead. An output with no path is the one
        case that needs the feature, and no such derivation exists while the
        feature is off. A floating content-addressed output names no path, and
        a deferred output names none either. An input-addressed output and a
        fixed-output one both name theirs.

        **An impure derivation is the exception, and it names no path.** Nix
        guards the registration with `if (!drv->type().isImpure())` at
        `derivation-goal.cc:226`, because every build of an impure derivation
        makes a new output and one id cannot hold two. pynixd registered one
        anyway, and the daemon answered "Trying to register a realisation of
        '...', but we already have another one locally". The refusal made the
        connection dirty, so the pool discarded it and the temporary roots it
        held stayed in the file. `main:impure-derivations` reaches line 50 of
        its script with this corrected, and it stopped at line 36 before.

        **The question is about the original derivation, and not the resolved
        one.** pynixd fills in a deferred output before it sends the
        derivation, so the resolved one names every path and answers False.
        The client holds the original, and `queryPartialDerivationOutputMap`
        at `store-api.cc:406` reads a realisation for each output that the
        original leaves open. `ca:build` builds `dependentNonCA`, which is
        that derivation.
        """
        if self.is_impure:
            return False
        return any(not output.path for output in self.outputs)

    def should_resolve(self, *, ca_derivations: bool) -> bool:
        """True when Nix writes a resolved derivation for this one, and builds that.

        `Derivation::shouldResolve` at `derivations.cc:1129`. A derivation with
        no input derivation has nothing to resolve. After that the type of the
        derivation decides: an input-addressed one resolves only when its
        output is deferred, a floating content-addressed one and an impure one
        always resolve, and a fixed-output one resolves only while the daemon
        has `ca-derivations` on (`derivations.cc:1134`). An input that is the
        output of a dynamic derivation also makes it resolve.

        **An input-addressed output belongs to the hash of the derivation that
        names it.** The resolved derivation is a different derivation, so the
        right path for that output is a different path, and the daemon says so
        at `derivations.cc:1324`: "derivation has incorrect output ..., should
        be ...". pynixd stored the resolved form of every derivation that had
        an input, and the daemon refused each input-addressed one. `main:gc` of
        the functional suite is where that showed: the refused connection went
        out of the pool as dirty, and the temporary root that it held stayed in
        the file.

        pynixd still resolves every derivation for the wire, because
        `BuildDerivation` carries a `BasicDerivation` and that model holds no
        input derivation. This decides one thing only: whether the request
        names the path of the resolved derivation or the path of the original
        one.
        """
        if not self.input_drvs and not self.dynamic_input_drvs:
            return False
        if self.dynamic_input_drvs:
            return True
        if all(kind is OutputKind.INPUT_ADDRESSED for kind in self.output_kinds()):
            return False
        if self.is_fixed_output:
            return ca_derivations
        return True

    def to_json(self, drv_path: StorePath | str) -> dict[str, NixDerivationShow]:
        """Serialize to the same JSON format as `nix derivation show`.

        Args:
            drv_path: The store path of this .drv file (used as top-level key).
        """
        drv_path_str = str(drv_path)
        # Outputs: {name: {path, hash?, hashAlgo?}}
        outputs: dict[str, NixDerivationOutputShow] = {}
        for o in self.outputs:
            entry: NixDerivationOutputShow = {}
            if o.path:
                entry["path"] = o.path
            if o.hash_algo:
                entry["hashAlgo"] = o.hash_algo
            if o.hash_value:
                entry["hash"] = o.hash_value
            outputs[o.name] = entry

        input_drvs: dict[str, NixInputDrvShow] = {}
        # Merge simple and dynamic entries
        all_drv_paths = set(self.input_drvs) | set(self.dynamic_input_drvs)
        for dp in sorted(all_drv_paths):
            entry_outputs = self.input_drvs.get(dp, [])
            dynamic_out = self.dynamic_input_drvs[dp].to_dict() if dp in self.dynamic_input_drvs else {}
            input_drvs[str(dp)] = {
                "dynamicOutputs": dynamic_out,
                "outputs": entry_outputs,
            }

        # name is derived from the store path: /nix/store/<hash>-<name>.drv
        basename = drv_path_str.rsplit("/", 1)[-1]  # <hash>-<name>.drv
        name = basename.split("-", 1)[1] if "-" in basename else basename
        name = name.removesuffix(".drv")

        inner: NixDerivationShow = {
            "args": self.args,
            "builder": self.builder,
            "env": self.env,
            "inputDrvs": input_drvs,
            "inputSrcs": sorted(str(p) for p in self.input_srcs),
            "name": name,
            "outputs": outputs,
            "system": self.platform,
        }
        return {drv_path_str: inner}

    def serialize(self) -> str:
        """Serialize to ATerm format (delegates to :meth:`unparse`).

        Returns a string in either ``Derive(...)`` or
        ``DrvWithVersion("xp-dyn-drv", ...)`` format.
        """
        return self.unparse(maskOutputs=False, actualInputs=None)

    @staticmethod
    def _make_store_path(
        type_str: str,
        hash_str: str,
        name: str,
        store_dir: str = "/nix/store",
    ) -> str:
        """Build a store path in the canonical Nix fashion.

        Computes ``sha256(type + ":" + hash + ":" + storeDir + ":" + name)``,
        compresses to 20 bytes, and formats as ``<hash>-<name>``.
        """
        digest = hashlib.sha256(f"{type_str}:{hash_str}:{store_dir}:{name}".encode()).digest()
        compressed = compress_hash(digest, 20)
        return f"{store_dir}/{nix32_encode(compressed)}-{name}"

    def compute_storepath(self, store_dir: Path | str = Path("/nix/store")) -> Path:
        """Compute the store path of this .drv file itself.

        Nix uses ``Store::makeFixedOutputPathFromCA`` with ``TextInfo``,
        which calls ``makeStorePath(type, hash, name)`` where::

            type = "text" + ":" + ref1 + ":" + ref2 + ...
            hash = "sha256:" + hex(content_hash)
            name = "<drv_name>.drv"

            s = type + ":" + hash + ":" + storeDir + ":" + name
            digest = sha256(s)
            storePath = compress(digest, 20).toBase32 + "-" + name
        """
        store_dir = Path(store_dir)
        raw = drv_bytes(self.unparse(maskOutputs=False))
        content_hash = hashlib.sha256(raw).hexdigest()

        drv_name = self.env.get("name", "unknown")
        name = f"{drv_name}.drv"

        # Build type string: "text" + ":" + ref1 + ":" + ref2 + ...
        type_str = "text"
        for p in sorted(self.input_drvs, key=str):
            type_str += ":" + str(p)
        for p in sorted(self.input_srcs, key=str):
            type_str += ":" + str(p)

        hash_str = f"sha256:{content_hash}"
        return Path(self._make_store_path(type_str, hash_str, name, str(store_dir)))

    @staticmethod
    def _print_unquoted_string(s: str) -> str:
        """Wrap in quotes without escaping (like C++ printUnquotedString)."""
        return f'"{s}"'

    @staticmethod
    def _print_unquoted_strings(items: list[str]) -> str:
        """ATerm list of unquoted strings (like C++ printUnquotedStrings)."""
        return "[" + ",".join(f'"{item}"' for item in items) + "]"

    def _has_dynamic_drv_dep(self) -> bool:
        """Check if this derivation depends on outputs of dynamic derivations.

        `hasDynamicDrvDep` (`derivations.cc:632`) asks for a node with
        children: an input written `([],[])` holds none. nixidae/nixidae#1141.
        """
        return any(node.children for node in self.dynamic_input_drvs.values())

    @staticmethod
    def _format_output_hash_algo(o: DrvOutput) -> str:
        """Return the ATerm hash_algo string for a DrvOutput.

        ``DrvOutput.hash_algo`` preserves the method prefix (e.g.
        ``"r:sha256"``) so we pass it through as-is.

        Returns the ATerm field value (e.g. ``"r:sha256"``, ``"sha256"``,
        or ``""``).
        """
        return o.hash_algo

    def _unparse_derived_path_node(self, node: _DrvInputNode) -> str:
        """Serialize a derived path map node (like C++ unparseDerivedPathMapNode).

        Produces the part after the path/name within an input drv entry.
        No children: ``,[\"out\"]``
        Children: ``,([\"out\"],[(\"a\",[...]),(\"b\",[...])])``

        The leading comma separates from the preceding path string.
        `derivations.cc:602` picks the form by whether the node has
        children, at every depth: a leaf under a dynamic input is a bare
        list too. nixidae/nixidae#1141.
        """
        output_names, child_map = node
        if not child_map:
            return "," + self._print_unquoted_strings(output_names)
        inner = self._print_unquoted_strings(output_names)
        child_parts = []
        for out_name, child_node in sorted(child_map.items()):
            child_parts.append(
                "(" + self._print_unquoted_string(out_name) + self._unparse_derived_path_node(child_node) + ")"
            )
        inner += ",[" + ",".join(child_parts) + "]"
        return ",(" + inner + ")"

    def unparse(self, maskOutputs: bool = False, actualInputs: dict[str, _DrvInputNode] | None = None) -> str:  # noqa: N803
        """Serialize to ATerm format, matching C++ ``Derivation::unparse``.

        Uses ``Derive(...)`` format when possible (backwards compatible),
        and ``DrvWithVersion(\"xp-dyn-drv\", ...)`` format when the
        derivation has dynamic input drv dependencies.

        Args:
            maskOutputs: If True, output paths are masked (empty string)
                in both the output list and environment variables. This
                is used for ``hashDerivationModulo``.
            actualInputs: If provided, replaces the input drvs entirely.
                Keys are already-string identifiers (e.g., hash modulo
                values). Values are ``(output_names, child_map)`` pairs
                matching the ``_DrvInputNode`` type.

        Returns:
            ATerm string representation of the derivation.
        """
        parts: list[str] = []

        # **The derivation chooses the format, and the replacement inputs do
        # not.** `Derivation::unparse` at `derivations.cc:641` asks
        # `hasDynamicDrvDep(*this)`, which reads the input derivations of the
        # derivation itself.
        #
        # This read *actualInputs* instead, and `hashDerivationModulo` builds
        # one that carries no child at all: it takes the direct outputs of each
        # input and drops the tree. So a dynamic derivation unparsed as
        # `Derive(` here and as `DrvWithVersion("xp-dyn-drv",` in Nix, and the
        # two hashes could never agree.
        if self._has_dynamic_drv_dep():
            parts.append("DrvWithVersion(")
            parts.append(self._print_unquoted_string("xp-dyn-drv"))
            parts.append(",")
        else:
            parts.append("Derive(")

        # --- Outputs ---
        parts.append("[")
        first = True
        for o in sorted(self.outputs, key=lambda x: x.name):
            if first:
                first = False
            else:
                parts.append(",")
            parts.append("(")
            parts.append(self._print_unquoted_string(o.name))
            parts.append(",")
            parts.append(self._print_unquoted_string("" if maskOutputs else o.path))
            parts.append(",")
            parts.append(self._print_unquoted_string(self._format_output_hash_algo(o)))
            parts.append(",")
            parts.append(self._print_unquoted_string(o.hash_value))
            parts.append(")")
        parts.append("],")

        # --- Input derivations ---
        parts.append("[")
        first = True
        if actualInputs is not None:
            for key, node in sorted(actualInputs.items()):
                if first:
                    first = False
                else:
                    parts.append(",")
                parts.append("(")
                parts.append(self._print_unquoted_string(key))
                parts.append(self._unparse_derived_path_node(node))
                parts.append(")")
        else:
            # Merge simple and dynamic input drvs
            all_paths = set(self.input_drvs) | set(self.dynamic_input_drvs)
            for drv_path in sorted(all_paths, key=str):
                if first:
                    first = False
                else:
                    parts.append(",")
                parts.append("(")
                parts.append(self._print_unquoted_string(str(drv_path)))
                # Build node from both simple and dynamic entries
                outputs = self.input_drvs.get(drv_path, [])
                if drv_path in self.dynamic_input_drvs:
                    node = _child_map_to_drv_node(
                        self.dynamic_input_drvs[drv_path],
                        prefix_outputs=outputs,
                    )
                else:
                    node = (outputs, {})
                parts.append(self._unparse_derived_path_node(node))
                parts.append(")")
        parts.append("],")

        # --- Input sources ---
        srcs = sorted(str(p) for p in self.input_srcs)
        parts.append(self._print_unquoted_strings(srcs))
        parts.append(",")

        # --- Platform (unquoted - simple identifier) ---
        parts.append(self._print_unquoted_string(self.platform))
        parts.append(",")

        # --- Builder (needs full ATerm escaping) ---
        parts.append(f'"{_aterm_escape(self.builder)}"')
        parts.append(",")

        # --- Arguments (needs full ATerm escaping) ---
        parts.append("[")
        first = True
        for a in self.args:
            if first:
                first = False
            else:
                parts.append(",")
            parts.append(f'"{_aterm_escape(a)}"')
        parts.append("],")

        # --- Environment ---
        parts.append("[")
        first = True
        output_names = {o.name for o in self.outputs}
        for k, v in sorted(self.env.items()):
            if first:
                first = False
            else:
                parts.append(",")
            parts.append("(")
            parts.append(f'"{_aterm_escape(k)}"')
            parts.append(",")
            if maskOutputs and k in output_names:
                parts.append('""')
            else:
                parts.append(f'"{_aterm_escape(v)}"')
            parts.append(")")
        parts.append("])")

        return "".join(parts)

    def hash_derivation_modulo(
        self,
        mask_outputs: bool = True,
        input_drv_hashes: dict[str, list[str]] | None = None,
    ) -> dict[str, str]:
        """Compute ``hashDerivationModulo``, matching C++ behaviour.

        For **fixed-output** derivations (every output has a non-empty
        hash_value and hash_algo), each output gets its own hash computed
        as SHA256 of ``"fixed:out:<method>:<hash>:<path>"``.

        For **all other** derivations (floating, deferred,
        input-addressed), the derivation is serialised via
        :meth:`unparse` with ``maskOutputs`` and the supplied
        ``input_drv_hashes``, and a single SHA256 hash is assigned to
        every output.

        Args:
            mask_outputs: Forwarded to :meth:`unparse`.  When ``True``,
                output paths are masked in the ATerm, which is required
                for ``hashDerivationModulo``.
            input_drv_hashes: ``{hex_hash: [output_name, ...]}`` — the
                hashes of each referenced output from input derivations.
                When the derivation has no input drvs this can be
                ``None`` (or empty).

        Returns:
            ``{output_name: hex_hash}`` — one SHA256 hex hash per
            derivation output.
        """
        # Fixed-output derivations: each output gets its own hash.
        #
        # **An impure output is not a fixed one.** `hashDerivationModulo` asks
        # `type().isFixed()` at `derivations.cc:902`, and `DerivationType::Impure`
        # answers no. The test here read the two raw fields instead, and an
        # impure output carries `r:sha256` in one and the word `impure` in the
        # other, so it read as fixed. pynixd then gave every realisation of an
        # impure derivation an id that no Nix agrees with. `OutputKind` is the
        # one place that classifies an output, so this asks it.
        if self.is_fixed_output:
            result: dict[str, str] = {}
            for o in self.outputs:
                method_algo = self._format_output_hash_algo(o)
                content = f"fixed:out:{method_algo}:{o.hash_value}:{o.path}"
                result[o.name] = hashlib.sha256(content.encode()).hexdigest()
            return result

        # Floating / deferred / input-addressed: unparse + single hash
        actual: dict[str, _DrvInputNode] | None = None
        if input_drv_hashes is not None:
            actual = {h: (outs, {}) for h, outs in input_drv_hashes.items()}

        aterm = self.unparse(maskOutputs=mask_outputs, actualInputs=actual)
        h = hashlib.sha256(drv_bytes(aterm)).hexdigest()
        return {o.name: h for o in self.outputs}


_STRING_BODY = re.compile(r'(?:[^"\\\n\r\t]++|\\[nrt\\"])*+')
"""What `parseString` (`aterm.cc:123`) takes: a character that needs no escape, or one of its five escapes.

It stops in front of an unknown escape and of a raw LF, CR or TAB; `_string_error` says which.
"""
_ESCAPED = re.compile(r"\\(.)", re.DOTALL)
_ESCAPES = {"n": "\n", "r": "\r", "t": "\t", "\\": "\\", '"': '"'}
"""`escapes` (`aterm.cc:80`)."""

_HASH_LEN = 32
_NIX32_CHARS = frozenset("0123456789abcdfghijklmnpqrsvwxyz")
_ANSI = re.compile(r"\x1b\[[0-9;]*m")


_PIECE = 1 << 20
"""The most text one C call of the parser handles: about 6 ms of held GIL.

`parse_drv_bytes` parses a large `.drv` on a worker thread, and a thread
hands the GIL to the event loop only between calls. One regex over a 19.5 MB
value held it 0.13 s (nixidae/nixidae#1139).
"""


def _unescape(body: str) -> str:
    """The value of an ATerm string body, one `_PIECE` at a time.

    A cut never splits an escape: an odd run of backslashes in front of it
    means the last one escapes the character after the cut.
    """
    if "\\" not in body:
        return body
    pieces: list[str] = []
    start = 0
    while start < len(body):
        cut = min(start + _PIECE, len(body))
        run = 0
        while cut - run > start and body[cut - run - 1] == "\\":
            run += 1
        cut += run % 2
        pieces.append(_unescape_piece(body[start:cut]))
        start = cut
    return "".join(pieces)


def _unescape_piece(body: str) -> str:
    """The value of a run of whole escapes, by `str.replace` and not per escape.

    A 23 MB `manifest.json.drv` holds 3.5 million escapes in one `__json`.
    Handled one at a time in Python, they held the event loop 1.5 s per read
    (nixidae/nixidae#1139). `\\\\` goes to NUL first, so every backslash left
    starts one escape; no Nix string holds a NUL, and one that does takes the
    slow road.
    """
    if "\\" not in body:
        return body
    if "\0" in body:
        return _ESCAPED.sub(lambda m: _ESCAPES[m[1]], body)
    text = body.replace("\\\\", "\0").replace('\\"', '"').replace("\\n", "\n").replace("\\r", "\r")
    return text.replace("\\t", "\t").replace("\0", "\\")


def _string_error(text: str, pos: int) -> ValueError:
    """Why `parseString` (`aterm.cc:123`) refuses the string at *pos*, where `_STRING_BODY` stopped."""
    if pos >= len(text) or (text[pos] == "\\" and pos + 1 >= len(text)):
        return ValueError("unterminated string in derivation")
    if text[pos] == "\\":
        return ValueError(f"unknown escape '\\{magenta(text[pos + 1])}'")
    return ValueError("invalid character in string")


def parse_store_path_canonical(path: str) -> StorePath:
    """`StoreDirConfig::parseStorePathCanonical` (`store-dir-config.cc:30`) and `StorePath::StorePath` (`path.cc:49`).

    NixOS/nix#16287: a store path in a derivation has one spelling, so `/nix/store/x/../y` is refused.
    """
    directory = store_dir()
    if not path.startswith(directory) or path[len(directory) : len(directory) + 1] != "/":
        raise ValueError(f"path '{magenta(path)}' is not in the Nix store")
    base = path[len(directory) + 1 :]
    if len(base) < _HASH_LEN + 1:
        raise ValueError(f"'{magenta(base)}' is too short to be a valid store path basename")
    for char in base[:_HASH_LEN]:
        if char not in _NIX32_CHARS:
            raise ValueError(
                f"store path basename '{magenta(base)}' contains illegal base-32 character '{magenta(char)}'"
            )
    if base[_HASH_LEN] != "-":
        raise ValueError(f"'{magenta(base)}' can't name a store path because the hash part is not followed by a '-'")
    try:
        check_name(base[_HASH_LEN + 1 :])
    except ValueError as error:
        # `checkPathName` (`path.cc:40`) quotes the inner message uncoloured.
        raise ValueError(f"path '{magenta(base)}' is not a valid store path: {_ANSI.sub('', str(error))}") from None
    return StorePath(base)


def _check_output(drv_name: str | None, output: DrvOutput) -> None:
    """`parseOutput` (`aterm.cc:308`): the combinations of path, method and hash that name an output.

    NIX-DEVIATION (#27): Nix also requires an experimental feature here (`xpSettings.require` at
    `aterm.cc:322`, `:325`, `:361`, and `DrvWithVersion` at `:469`), from the settings of the process that
    parses. pynixd parses files a daemon already admitted, and holds no client's settings at this point, so it
    leaves the feature to the daemon that builds. Reverse it if a derivation that needs a disabled feature
    builds through pynixd and fails through nix-daemon. nixidae/nixidae#416.

    A fixed output's path is compared with its content address only when *drv_name* is known.
    """
    path, algo_text, hash_text = output.path, output.hash_algo, output.hash_value
    if not algo_text:
        if hash_text:
            raise ValueError("hash specified without hash algorithm")
        if path:
            parse_store_path_canonical(path)
        return
    _, algo = output_method(algo_text)
    if algo not in HASH_SIZES:
        raise ValueError(
            f"unknown hash algorithm '{magenta(algo)}', expect 'blake3', 'md5', 'sha1', 'sha256', or 'sha512'"
        )
    if hash_text == "impure":
        if path:
            raise ValueError("impure derivation output should not specify output path")
        return
    if not hash_text:
        if path:
            raise ValueError("content-addressing derivation output should not specify output path")
        return
    parse_store_path_canonical(path)
    address = fixed_output_address(algo_text, hash_text)
    if drv_name is None:
        return
    name = drv_name if output.output_name == "out" else f"{drv_name}-{output.output_name}"
    if content_addressed_path(address, name, [], store_dir()) != path:
        raise ValueError(
            f"derivation output '{magenta(output.output_name)}' has path '{magenta(path)}', "
            "which does not match its content address"
        )


def _check_structured_attrs(env: dict[str, str]) -> None:
    """`StructuredAttrs::parse` (`parsed-derivations.cc:12`): `__json`, when present, is JSON.

    Validated into `msgspec.Raw`, which builds no object: `json.loads` of a 23.8 MB `__json` held the GIL long
    enough to stall the event loop 0.31 s, against 0.015 s for the rest of the parse. msgspec, like nlohmann,
    refuses `NaN` and a lone surrogate escape; it takes invalid UTF-8, which the strict encode refuses. The text
    after the colon is msgspec's, not nlohmann's.
    """
    encoded = env.get("__json")
    if encoded is None:
        return
    try:
        msgspec.json.decode(encoded.encode(), type=msgspec.Raw)
    except UnicodeEncodeError:
        raise ValueError(f"cannot process {magenta('__json')} attribute: invalid UTF-8") from None
    except msgspec.DecodeError as error:
        raise ValueError(f"cannot process {magenta('__json')} attribute: {error}") from None


def _check_monotonic(last: str | None, key: str, describe: Callable[[], str]) -> None:
    """`checkMonotonic` of Nix master (`derivation/aterm.cc:253`): a set or map is sorted, with no duplicate.

    NIX-DEFECT (#23): 2.35's `parseStrings` (`derivations.cc:303`) and its maps insert into a `std::set` or
    `std::map`, so a duplicate or an unsorted entry vanishes, and the text that hashes is no longer the text on
    disk: one value has several encodings. pynixd refuses them on every version, as master does. Fixed upstream
    by NixOS/nix#16284, after 2.35.
    """
    if last is None:
        return
    if last == key:
        raise ValueError(f"duplicate {magenta(describe())}")
    if not last < key:
        raise ValueError(f"{magenta(describe())} does not appear in a sorted order")


class _Parser:
    """`derivation::parse` of Nix master (`aterm.cc:442`): one spelling per derivation, so no whitespace anywhere.

    NIX-DEFECT (#23): Nix 2.35's parser (`derivations.cc:224-275`) skips no whitespace either, but takes any
    `\\x` as `x`, a raw LF in a string, an escape in a store path, a non-canonical store path and trailing input,
    so one derivation has several encodings and the text that hashes is not the text on disk. pynixd refuses
    them on every version, as master does since NixOS/nix#16284 and NixOS/nix#16287. nixidae/nixidae#1671.
    """

    def __init__(self, text: str, drv_name: str | None = None) -> None:
        self._text = text
        self._pos = 0
        self._drv_name = drv_name

    def _peek(self) -> str:
        """`StringViewStream::peek`; the empty string at the end of input."""
        return self._text[self._pos : self._pos + 1]

    def _expect(self, char: str) -> None:
        if self._peek() != char:
            raise ValueError(f"expected '{magenta(char)}' character")
        self._pos += 1

    def _expect_string(self, text: str) -> None:
        if not self._text.startswith(text, self._pos):
            raise ValueError(f"expected string '{magenta(text)}'")
        self._pos += len(text)

    def _list[T](self, item: Callable[[], T]) -> list[T]:
        """`parseList` (`aterm.cc:205`)."""
        self._expect("[")
        result: list[T] = []
        if self._peek() == "]":
            self._pos += 1
            return result
        while True:
            result.append(item())
            char = self._peek()
            self._pos += 1
            if char == "]":
                return result
            if char != ",":
                raise ValueError("invalid list")

    def _map[K, V](
        self, parse_key: Callable[[], K], parse_value: Callable[[K], V], describe: Callable[[K], str]
    ) -> list[tuple[K, V]]:
        """`parseMap` (`aterm.cc:273`): `(key,value)` pairs, keys sorted and unique."""
        pairs: list[tuple[K, V]] = []

        def pair() -> None:
            self._expect("(")
            key = parse_key()
            self._expect(",")
            value = parse_value(key)
            _check_monotonic(str(pairs[-1][0]) if pairs else None, str(key), lambda: describe(key))
            pairs.append((key, value))
            self._expect(")")

        self._list(pair)
        return pairs

    def parse_string(self) -> str:
        """`parseString` (`aterm.cc:123`): the builder, its arguments and the environment.

        Only `\\n`, `\\r`, `\\t`, `\\\\` and `\\"` are escapes, and a character that has one must use it.
        """
        self._expect('"')
        text = self._text
        start = end = self._pos
        while True:
            # A window that ends inside an escape stops in front of its
            # backslash, and the next window starts there.
            body = _STRING_BODY.match(text, end, end + max(_PIECE, 2))
            scanned = body.end() if body is not None else end
            if scanned < len(text) and text[scanned] == '"':
                break
            if scanned == end:
                raise _string_error(text, scanned)
            end = scanned
        self._pos = scanned + 1
        return _unescape(text[start:scanned])

    def parse_unquoted_string(self) -> str:
        """`parseUnquotedString` (`aterm.cc:166`): output names, store paths and the platform, verbatim.

        NIX-DEFECT (#23): Nix checks nothing else here, so an output name or a platform may hold a NUL, a DEL or
        a raw LF (the FIXME at `external-formats.cc:52`), and `printUnquotedString` writes a `"` in one that the
        parser then cannot read back. pynixd copies it: a stricter parser refuses a `.drv` that nix-daemon builds.
        nixidae/nixidae#1671.
        """
        self._expect('"')
        end = self._text.find('"', self._pos)
        if end < 0:
            raise ValueError("unterminated string in derivation")
        content = self._text[self._pos : end]
        self._pos = end + 1
        if "\\" in content:
            raise ValueError("unexpected escape sequence in unquoted string")
        return content

    def parse_store_path(self) -> StorePath:
        """`parseStorePath` (`aterm.cc:197`)."""
        return parse_store_path_canonical(self.parse_unquoted_string())

    def parse_string_list(self) -> list[str]:
        """The builder's arguments."""
        return self._list(self.parse_string)

    def parse_string_set(self) -> list[str]:
        """`parseStrings` (`aterm.cc:286`): output names, verbatim, sorted and with no duplicate."""
        items: list[str] = []

        def item() -> str:
            content = self.parse_unquoted_string()
            _check_monotonic(items[-1] if items else None, content, lambda: f"set item '{content}'")
            items.append(content)
            return content

        self._list(item)
        return items

    def parse_store_path_set(self) -> list[StorePath]:
        """`parseStorePaths` (`aterm.cc:297`): store paths, sorted and with no duplicate."""
        items: list[StorePath] = []

        def item() -> StorePath:
            path = self.parse_store_path()
            _check_monotonic(str(items[-1]) if items else None, str(path), lambda: f"store path '{path}'")
            items.append(path)
            return path

        self._list(item)
        return items

    def _parse_output(self, output_name: str) -> DrvOutput:
        """The value of one output (`aterm.cc:488`): path, method and hash, checked by `parseOutput`."""
        path = self.parse_unquoted_string()
        self._expect(",")
        hash_algo = self.parse_string()
        self._expect(",")
        hash_value = self.parse_string()
        output = DrvOutput(hash_algo=hash_algo, hash_value=hash_value, output_name=output_name, path=path)
        _check_output(self._drv_name, output)
        return output

    def parse_outputs(self) -> list[DrvOutput]:
        """The outputs (`aterm.cc:484`): `(name,path,hashAlgo,hash)` per output, by name."""
        pairs = self._map(self.parse_unquoted_string, self._parse_output, lambda name: f"output name '{name}'")
        return [output for _, output in pairs]

    def _parse_input_drv_path(self) -> StorePath:
        """An input derivation's path (`aterm.cc:505`): canonical, and a derivation's."""
        path = self.parse_store_path()
        if not path.name.endswith(".drv"):
            raise ValueError(f"store path '{magenta(path.name)}' is not a valid derivation path")
        return path

    def _parse_node(self, *, dynamic: bool) -> ChildMapNode:
        """`parseDerivedPathMapNode` (`aterm.cc:400`): `[outs]`, or with dynamic derivations `([outs],[(name,node)])`.

        NIX-DEFECT (#23): a child's output name goes through `parseString` (`aterm.cc:426`), so it takes escapes,
        while `unparseDerivedPathMapNode` writes it with `printUnquotedString` and every other output name is
        verbatim. pynixd copies it. nixidae/nixidae#1671.
        """
        if not dynamic or self._peek() == "[":
            return ChildMapNode(outputs=self.parse_string_set())
        if self._peek() != "(":
            raise ValueError("invalid inputDrvs entry in derivation")
        self._expect("(")
        outputs = self.parse_string_set()
        self._expect(",")
        children = self._map(
            self.parse_string, lambda _name: self._parse_node(dynamic=True), lambda name: f"output name '{name}'"
        )
        self._expect(")")
        return ChildMapNode(outputs=outputs, children=dict(children))

    def parse_input_drvs(self, *, dynamic: bool) -> tuple[dict[StorePath, list[str]], dict[StorePath, ChildMapNode]]:
        """The input derivations (`aterm.cc:501`): those that name outputs only, and those with a child map."""
        simple: dict[StorePath, list[str]] = {}
        nested: dict[StorePath, ChildMapNode] = {}

        def value(path: StorePath) -> tuple[bool, ChildMapNode]:
            is_nested = dynamic and self._peek() == "("
            node = self._parse_node(dynamic=dynamic)
            # Nix never writes one, and the flat inputs set cannot hold it (`aterm.cc:511`).
            if not node.outputs and not node.children:
                raise ValueError(f"inputDrvs entry for '{magenta(path)}' specifies no outputs")
            return is_nested, node

        for path, (is_nested, node) in self._map(
            self._parse_input_drv_path, value, lambda path: f"input derivation '{magenta(path)}'"
        ):
            if is_nested:
                nested[path] = node
            else:
                simple[path] = node.outputs
        return simple, nested

    def parse_env(self) -> dict[str, str]:
        """The environment (`aterm.cc:534`), with `__json` parsed as `StructuredAttrs::tryExtract` does."""
        env = dict(
            self._map(self.parse_string, lambda _key: self.parse_string(), lambda key: f"environment variable '{key}'")
        )
        _check_structured_attrs(env)
        return env

    def parse_derivation(self) -> Derivation:
        """`derivation::parse` (`aterm.cc:442`)."""
        self._expect("D")
        match self._peek():
            case "e":
                self._expect_string("erive(")
                dynamic = False
            case "r":
                self._expect_string("rvWithVersion(")
                version = self.parse_string()
                if version != "xp-dyn-drv":
                    raise ValueError(f"unknown derivation ATerm format version '{magenta(version)}'")
                self._expect(",")
                dynamic = True
            case _:
                raise ValueError("derivation does not start with 'Derive' or 'DrvWithVersion'")
        outputs = self.parse_outputs()
        self._expect(",")
        input_drvs, dynamic_input_drvs = self.parse_input_drvs(dynamic=dynamic)
        self._expect(",")
        input_srcs = self.parse_store_path_set()
        self._expect(",")
        platform = self.parse_unquoted_string()
        self._expect(",")
        builder = self.parse_string()
        self._expect(",")
        args = self.parse_string_list()
        self._expect(",")
        env = self.parse_env()
        self._expect(")")
        if self._pos < len(self._text):
            raise ValueError(f"expected end of file, found '{magenta(self._text[self._pos :])}'")
        return Derivation(
            outputs=outputs,
            input_drvs=input_drvs,
            input_srcs=set(input_srcs),
            platform=platform,
            builder=builder,
            args=args,
            env=env,
            is_dynamic=dynamic,
            dynamic_input_drvs=dynamic_input_drvs,
        )


async def to_basic_derivation(
    parsed: Derivation,
    layout: StoreLayout,
    output_cache: OutputMap | None = None,
) -> BasicDerivation:
    """Convert a Derivation to a BasicDerivation (wire protocol format).

    Resolves inputDrvs into concrete output paths and merges them into
    input_srcs, matching what nix does when sending BuildDerivation over
    the wire.

    Args:
        parsed: The parsed .drv file
        layout: The store that holds the referenced .drv files
        output_cache: Optional {drv_path: {output_name: output_path}} cache
            from the DB to skip reading input .drv files from disk.
    """
    outputs = {
        o.name: DerivationOutput(
            path=o.path,
            method=o.hash_algo,
            hash_digest=o.hash_value,
        )
        for o in parsed.outputs
    }

    # Start with the explicit input sources
    input_srcs = {StorePath(path=str(path)) for path in parsed.input_srcs}

    # Resolve inputDrvs: for each input drv, look up its output paths
    # and add them to input_srcs (this is what nix does before sending
    # BuildDerivation over the wire)
    for drv_path, output_names in parsed.input_drvs.items():
        if output_cache and drv_path in output_cache:
            cached = output_cache[drv_path]
            for name in output_names:
                p = cached.get(name)
                if p:
                    input_srcs.add(StorePath(path=str(p)))
            continue

        try:
            input_parsed = await read_drv_file(drv_path, layout)
        except FileNotFoundError:
            input_parsed = None

        if input_parsed is None:
            input_srcs.add(StorePath(path=str(drv_path)))
            continue

        all_outputs = input_parsed.output_paths()
        for name in output_names:
            p = all_outputs.get(name)
            if p:
                input_srcs.add(StorePath(path=str(p)))

    return BasicDerivation(
        outputs=outputs,
        input_srcs=input_srcs,
        platform=parsed.platform,
        builder=parsed.builder,
        args=parsed.args,
        env=parsed.env,
        is_dynamic=parsed.is_dynamic,
    )


def from_basic_derivation(basic: BasicDerivation) -> Derivation:
    """The `Derivation` a daemon makes of a `BuildDerivation` request: no input derivation (`daemon.cc:596`)."""
    return Derivation(
        outputs=[
            DrvOutput(hash_algo=output.method, hash_value=output.hash_digest, output_name=name, path=output.path)
            for name, output in sorted(basic.outputs.items())
        ],
        input_srcs={StorePath(path=str(path)) for path in basic.input_srcs},
        platform=basic.platform,
        builder=basic.builder,
        args=list(basic.args),
        env=dict(basic.env),
    )


def parse_drv(content: str, name: str | None = None) -> Derivation:
    """`derivation::parse`: a `.drv` file's text as a `Derivation`, or `ValueError` with Nix's message.

    *name* is the derivation's name, which a fixed output's path is checked against. A file read from a store
    goes through `parse_drv_bytes`, which always has it.
    """
    return _Parser(content, name).parse_derivation()


def drv_name(drv_path: StorePath | str) -> str:
    """`Derivation::nameFromPath` (`derivations.cc`): the name of a `.drv` path, without hash and extension."""
    return StorePath(str(drv_path)).name[_HASH_LEN + 1 :].removesuffix(".drv")


_INLINE_PARSE_BYTES = 256 * 1024
"""Below this a parse takes under a millisecond, less than the thread hop."""


async def parse_drv_bytes(raw: bytes, name: str) -> Derivation:
    """`parse_drv` of a `.drv` file's bytes, off the event loop when it is large.

    A 23 MB `.drv` parses in 0.29 s. On a worker thread the loop waited at
    most 0.15 s for the GIL meanwhile, the length of the longest C call, and
    `_PIECE` bounds that call (nixidae/nixidae#1139).
    """
    if len(raw) < _INLINE_PARSE_BYTES:
        return parse_drv(drv_text(raw), name)
    return await anyio.to_thread.run_sync(_parse_bytes, raw, name)


def _parse_bytes(raw: bytes, name: str) -> Derivation:
    return parse_drv(drv_text(raw), name)


class DerivationFormatError(PynixdError):
    """A `.drv` on disk that does not parse, with Nix's message (`readDerivationCommon`)."""


async def read_drv_file(drv_store_path: StorePath | str, layout: StoreLayout) -> Derivation | None:
    """Read and parse a `.drv` file from the file system of the store.

    Args:
        drv_store_path: A whole store path, such as `/nix/store/xxx.drv`.
        layout: The store that holds it: which directory its paths name,
            and which directory holds the files.

    Returns:
        The parsed derivation, or `None` when the file is not there.
    """
    # The layout's two directories, and not `root / str(drv_store_path).lstrip("/")`.
    #
    # That line assumed every store path starts with `/nix/store/`, so removing
    # the first separator and joining the root gave `<root>/nix/store/xxx.drv`.
    # A store that answers `<root>/nix/store/xxx.drv` broke it: the join then
    # made `<root><root>/nix/store/xxx.drv`, the file was not there, and the
    # caller took the `None` branch. `to_basic_derivation` then sent the daemon
    # a derivation whose `inputSrcs` named the input `.drv` rather than the
    # output of that `.drv`. The daemon scans the build output for the paths in
    # `inputSrcs` alone, so it found no reference and registered the output
    # with none. `tests/functional/dependencies.sh` of Nix caught it.
    #
    # `real_store_dir` is where the files are. `store_dir` is what a store path
    # says. A chroot store makes the two differ, and only the first one names a
    # file. Issue nixidae/nixidae#212.
    name = str(drv_store_path)
    prefix = f"{layout.store_dir}/"
    if not name.startswith(prefix):
        raise ValueError(f"{name!r} is not a path of the store at {layout.store_dir}")
    path = anyio.Path(layout.real_store_dir / name[len(prefix) :])
    if not await path.exists():
        return None
    raw = await path.read_bytes()
    # `readDerivationCommon` (`store-api.cc:1191`): an empty file has its own message, the name must be a
    # derivation's (`nameFromPath`), and each format error names the derivation.
    base = name[len(prefix) :]
    try:
        if not raw:
            raise ValueError("file is empty (possible filesystem corruption)")
        if not base.endswith(".drv"):
            raise ValueError(f"store path '{magenta(base)}' is not a valid derivation path")
        return await parse_drv_bytes(raw, drv_name(base))
    except ValueError as error:
        raise DerivationFormatError(f"error parsing derivation '{magenta(name)}': {error}") from error
