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
from typing import TYPE_CHECKING, Any, TypedDict, overload

import anyio

from nix_daemon_protocol.basic_derivation import bool_attr, required_features_of

from .serde import BasicDerivation, DerivationOutput, OutputKind
from .store_path import DrvOutput, StorePath
from .utils import compress_hash, nix32_encode

if TYPE_CHECKING:
    from collections.abc import Callable, Collection

    from nix_daemon_protocol.aliases import OutputMap, StorePathSet

    from .store_layout import StoreLayout

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


# Recursive type for input drv nodes in unparse.
# (output_names, child_map, is_dynamic) where:
# - output_names: output names referenced from this input
# - child_map: {output_name: (nested_names, child_map, is_dynamic)} for dynamic deps
# - is_dynamic: True if this entry was in DrvWithVersion dynamic format,
#   even when child_map is empty (``([], [])`` vs simple ``[]``)
type _DrvInputNode = tuple[list[str], dict[str, "_DrvInputNode"], bool]


def _child_map_to_drv_node(node: ChildMapNode, prefix_outputs: list[str] | None = None) -> _DrvInputNode:
    """Convert a ChildMapNode to _DrvInputNode for serialization.

    Combines ``prefix_outputs`` (flat outputs from ``input_drvs``) with
    the node's own outputs and recursively converts children.
    """
    all_outputs = (prefix_outputs or []) + node.outputs
    children = {name: _child_map_to_drv_node(child) for name, child in node.children.items()}
    return (all_outputs, children, True)


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

        Corresponds to the C++ hasDynamicDrvDep helper.
        """
        return bool(self.dynamic_input_drvs)

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
        Simple: ``,[\"out\"]``
        Dynamic: ``,([\"out\"],[(\"a\",[...]),(\"b\",[...])])``

        The leading comma separates from the preceding path string.
        """
        output_names, child_map, is_dynamic = node
        if not child_map and not is_dynamic:
            return "," + self._print_unquoted_strings(output_names)
        # Dynamic entry wraps output names and child map
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
                values). Values are ``(output_names, child_map)`` tuples
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
                    node = (outputs, {}, False)
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
            actual = {h: (outs, {}, False) for h, outs in input_drv_hashes.items()}

        aterm = self.unparse(maskOutputs=mask_outputs, actualInputs=actual)
        h = hashlib.sha256(drv_bytes(aterm)).hexdigest()
        return {o.name: h for o in self.outputs}


_STRING_BODY = re.compile(r'(?:[^"\\]++|\\.)*+', re.DOTALL)
_ESCAPED = re.compile(r"\\(.)", re.DOTALL)
_ESCAPES = {"n": "\n", "r": "\r", "t": "\t"}
"""`parseString` at `derivations.cc:188-205` maps these three, and any other `\\x` to `x`."""


def _unescape(body: str) -> str:
    """The value of an ATerm string body, by `str.replace` and not per escape.

    A 23 MB `manifest.json.drv` holds 3.5 million escapes in one `__json`.
    Handled one at a time in Python, they held the event loop 1.5 s per read
    (nixidae/nixidae#1139). `\\\\` goes to NUL first, so every backslash left
    starts one escape; no Nix string holds a NUL, and one that does takes the
    slow road.
    """
    if "\\" not in body:
        return body
    if "\0" in body:
        return _ESCAPED.sub(lambda m: _ESCAPES.get(m[1], m[1]), body)
    text = body.replace("\\\\", "\0").replace('\\"', '"').replace("\\n", "\n").replace("\\r", "\r")
    text = text.replace("\\t", "\t")
    if "\\" in text:
        text = _ESCAPED.sub(r"\1", text)
    return text.replace("\0", "\\")


class _Parser:
    """Simple recursive-descent ATerm parser for .drv files."""

    def __init__(self, text: str) -> None:
        self._text = text
        self._pos = 0

    def _peek(self) -> str:
        if self._pos >= len(self._text):
            raise ValueError("Unexpected end of input")
        return self._text[self._pos]

    def _advance(self) -> str:
        ch = self._peek()
        self._pos += 1
        return ch

    def _expect(self, s: str) -> None:
        for ch in s:
            got = self._advance()
            if got != ch:
                raise ValueError(f"Expected {ch!r} at pos {self._pos - 1}, got {got!r}")

    def _skip_ws(self) -> None:
        while self._pos < len(self._text) and self._text[self._pos] in " \t\n\r":
            self._pos += 1

    def parse_string(self) -> str:
        """Parse a quoted ATerm string with escape handling."""
        self._expect('"')
        start = self._pos
        body = _STRING_BODY.match(self._text, start)
        end = body.end() if body is not None else start
        if end >= len(self._text) or self._text[end] != '"':
            raise ValueError(f"Unterminated string starting at pos {start}")
        self._pos = end + 1
        return _unescape(self._text[start:end])

    @overload
    def parse_string_list(self) -> list[str]: ...

    @overload
    def parse_string_list[T](self, tp: Callable[[str], T]) -> list[T]: ...

    def parse_string_list[T](self, tp: Callable[[str], T] | None = None) -> list[Any]:
        """Parse [str, str, ...]."""
        self._expect("[")
        result: list[Any] = []
        convert: Callable[[str], Any] = tp if tp is not None else str
        self._skip_ws()
        while self._peek() != "]":
            if result:
                self._expect(",")
            self._skip_ws()
            result.append(convert(self.parse_string()))
            self._skip_ws()
        self._expect("]")
        return result

    def parse_outputs(self) -> list[DrvOutput]:
        """Parse [("name","path","hashAlgo","hash"), ...] into DrvOutput list."""
        self._expect("[")
        result: list[DrvOutput] = []
        self._skip_ws()
        while self._peek() != "]":
            if result:
                self._expect(",")
            self._skip_ws()
            self._expect("(")
            name = self.parse_string()
            self._expect(",")
            path = self.parse_string()
            self._expect(",")
            hash_algo = self.parse_string()
            self._expect(",")
            hash_value = self.parse_string()
            self._expect(")")
            result.append(
                DrvOutput(
                    hash_algo=hash_algo,
                    hash_value=hash_value,
                    output_name=name,
                    path=path,
                ),
            )
            self._skip_ws()
        self._expect("]")
        return result

    def parse_input_drvs_simple(self) -> dict[StorePath, list[str]]:
        """Parse [("drvPath",["out",...]), ...] - traditional format."""
        self._expect("[")
        result: dict[StorePath, list[str]] = {}
        self._skip_ws()
        while self._peek() != "]":
            if result:
                self._expect(",")
            self._skip_ws()
            self._expect("(")
            drv_path = StorePath(self.parse_string())
            self._expect(",")
            outputs = self.parse_string_list()
            self._expect(")")
            result[drv_path] = outputs
            self._skip_ws()
        self._expect("]")
        return result

    def _parse_child_value(self) -> ChildMapNode:
        """Parse a child value in a DerivedPathMapNode.

        A child value can be either:
        - Leaf: ``[out1,out2]`` — flat output names
        - Nested: ``([flat_outs],[(name,child),...])`` — recursive node
        """
        if self._peek() == "[":
            # Leaf: flat list of output names
            outputs = self.parse_string_list()
            return ChildMapNode(outputs=outputs)
        if self._peek() == "(":
            # Nested: recursive DerivedPathMapNode
            return self._parse_child_map_node()
        raise ValueError(
            f"Expected '[' or '(' at pos {self._pos}, got {self._peek()!r}",
        )

    def _parse_child_map_node(self) -> ChildMapNode:
        """Parse a ``DerivedPathMapNode`` recursively.

        Wire format: ``([flat_outs],[(output_name, child_value), ...])``
        where child_value is either a flat list ``[out]`` or a nested
        ``DerivedPathMapNode`` ``([outs],[children])``.
        """
        self._advance()  # '('
        self._skip_ws()
        outputs = self.parse_string_list()
        self._expect(",")
        self._expect("[")  # Start of children list
        children: dict[str, ChildMapNode] = {}
        self._skip_ws()
        while self._peek() != "]":
            if children:
                self._expect(",")
            self._skip_ws()
            self._expect("(")
            child_name = self.parse_string()
            self._expect(",")
            # Parse child value (leaf or nested)
            child_node = self._parse_child_value()
            children[child_name] = child_node
            self._expect(")")
            self._skip_ws()
        self._expect("]")  # End of children list
        self._expect(")")  # End of this node
        return ChildMapNode(outputs=outputs, children=children)

    def parse_input_drvs_dynamic(
        self,
    ) -> tuple[dict[StorePath, list[str]], dict[StorePath, ChildMapNode]]:
        """Parse dynamic input drvs format (DrvWithVersion).

        Each input drv is either:
        - Simple: ``(drvPath,[out1,out2])`` — non-dynamic dependency
        - Dynamic: ``(drvPath,([flat_outs],[(name,child),...]))`` — recursive

        Returns:
            (simple_map, dynamic_map) where:
            - simple_map: {drv_path: [output_name, ...]} for non-dynamic inputs
            - dynamic_map: {drv_path: ChildMapNode} for dynamic inputs
        """
        self._expect("[")
        simple: dict[StorePath, list[str]] = {}
        dynamic: dict[StorePath, ChildMapNode] = {}
        self._skip_ws()
        while self._peek() != "]":
            if simple or dynamic:
                self._expect(",")
            self._skip_ws()
            self._expect("(")
            drv_path = StorePath(self.parse_string())
            self._expect(",")
            self._skip_ws()

            if self._peek() == "[":
                # Non-dynamic: simple list of output names
                outputs = self.parse_string_list()
                simple[drv_path] = outputs
            elif self._peek() == "(":
                # Dynamic: recursive ChildMapNode
                node = self._parse_child_map_node()
                dynamic[drv_path] = node
            else:
                raise ValueError(
                    f"Expected '[' or '(' at pos {self._pos}, got {self._peek()!r}",
                )
            self._expect(")")
            self._skip_ws()
        self._expect("]")
        return simple, dynamic

    def parse_env(self) -> dict[str, str]:
        """Parse [("key","value"), ...]."""
        self._expect("[")
        result: dict[str, str] = {}
        self._skip_ws()
        while self._peek() != "]":
            if result:
                self._expect(",")
            self._skip_ws()
            self._expect("(")
            key = self.parse_string()
            self._expect(",")
            value = self.parse_string()
            self._expect(")")
            result[key] = value
            self._skip_ws()
        self._expect("]")
        return result

    def parse_derivation(self) -> Derivation:
        """Parse the full Derive(...) or DrvWithVersion(...) term."""
        self._skip_ws()

        # Check for dynamic derivation format
        if self._text[self._pos :].startswith("DrvWithVersion("):
            return self._parse_dynamic_derivation()
        return self._parse_traditional_derivation()

    def _parse_traditional_derivation(self) -> Derivation:
        """Parse Derive(...) term."""
        self._expect("Derive(")
        self._skip_ws()

        outputs = self.parse_outputs()
        self._expect(",")
        self._skip_ws()

        input_drvs = self.parse_input_drvs_simple()
        self._expect(",")
        self._skip_ws()

        input_srcs_list = self.parse_string_list(StorePath)
        self._expect(",")
        self._skip_ws()

        platform = self.parse_string()
        self._expect(",")
        self._skip_ws()

        builder = self.parse_string()
        self._expect(",")
        self._skip_ws()

        args = self.parse_string_list()
        self._expect(",")
        self._skip_ws()

        env = self.parse_env()
        self._skip_ws()
        self._expect(")")

        return Derivation(
            outputs=outputs,
            input_drvs=input_drvs,
            input_srcs=set(input_srcs_list),
            platform=platform,
            builder=builder,
            args=args,
            env=env,
            is_dynamic=False,
        )

    def _parse_dynamic_derivation(self) -> Derivation:
        """Parse DrvWithVersion("xp-dyn-drv",...) term."""
        self._expect("DrvWithVersion(")
        self._skip_ws()

        version = self.parse_string()
        if version != "xp-dyn-drv":
            raise ValueError(f"Unknown derivation ATerm version: {version!r}")
        self._expect(",")
        self._skip_ws()

        outputs = self.parse_outputs()
        self._expect(",")
        self._skip_ws()

        input_drvs, dynamic_input_drvs = self.parse_input_drvs_dynamic()
        self._expect(",")
        self._skip_ws()

        input_srcs_list = self.parse_string_list(StorePath)
        self._expect(",")
        self._skip_ws()

        platform = self.parse_string()
        self._expect(",")
        self._skip_ws()

        builder = self.parse_string()
        self._expect(",")
        self._skip_ws()

        args = self.parse_string_list()
        self._expect(",")
        self._skip_ws()

        env = self.parse_env()
        self._skip_ws()
        self._expect(")")

        return Derivation(
            outputs=outputs,
            input_drvs=input_drvs,
            input_srcs=set(input_srcs_list),
            platform=platform,
            builder=builder,
            args=args,
            env=env,
            is_dynamic=True,
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


def parse_drv(content: str) -> Derivation:
    """Parse a .drv file's content into a Derivation."""
    return _Parser(content).parse_derivation()


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
    return parse_drv(drv_text(await path.read_bytes()))
