"""
DerivedPath: model for Nix derived path references.

Mirrors the Nix C++ DerivedPath type:
  DerivedPath = Opaque(path) | Built(drv_path=SingleDerivedPath, outputs=OutputsSpec)

Unlike C++, pynixd uses a single class with property-based dispatch instead of
a variant with separate struct types.  This is simpler for Python and matches
the daemon wire protocol (which only ever carries ``DerivedPath``).

Wire format (daemon protocol): always uses the ``!`` separator (legacy format).
Public API format: uses the ``^`` separator.

Parsing follows Nix's right-to-left rfind strategy so that nested references
like ``a.drv!out!lib`` are parsed as:

    _drv_path = a.drv
    _chain    = ("out",)
    _outputs  = Names({"lib"})

Meaning: build ``a.drv``, take output ``out`` (which yields a .drv), then
build that derivation and take output ``lib``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Self

from .store_path import StorePath

# ── OutputsSpec ─────────────────────────────────────────────────────


@dataclass(frozen=True)
class OutputsAll:
    """Represents ``*`` — all outputs of a derivation."""

    def to_string(self) -> str:
        """Return ``*`` to indicate all outputs."""
        return "*"


@dataclass(frozen=True)
class OutputsNames:
    """A non-empty named set of derivation outputs."""

    names: frozenset[str]

    def to_string(self) -> str:
        """Return a comma-separated string of output names, sorted."""
        return ",".join(sorted(self.names))


OutputsSpec = OutputsAll | OutputsNames


def check_output_name(name: str) -> None:
    """`checkName` at `path.cc:12-38`, with its messages: `ValueError` for a name Nix refuses."""
    if not name:
        raise ValueError("name must not be empty")
    if len(name) > _MAX_NAME_LEN:
        raise ValueError(f"name '{name}' must be no longer than {_MAX_NAME_LEN} characters")
    if name in {".", ".."}:
        raise ValueError(f"name '{name}' is not valid")
    for dots in (".", ".."):
        if name.startswith(dots + "-"):
            raise ValueError(f"name '{name}' is not valid: first dash-separated component must not be '{dots}'")
    for char in name:
        if not (char.isascii() and (char.isalnum() or char in "+-._?=")):
            raise ValueError(f"name '{name}' contains illegal character '{char}'")


_MAX_NAME_LEN = 211
"""`StorePath::MaxPathLen`."""


def _parse_outputs_spec(s: str) -> OutputsSpec:
    if s == "*":
        return OutputsAll()
    # An empty list stays empty: Nix's daemon reads `drv^` as no output, and
    # plans nothing for it (`build.sh:102`, measured in issue #26).
    names = frozenset(s.split(",")) if s else frozenset()
    for name in names:
        check_output_name(name)
    return OutputsNames(names)


# ── DerivedPath (regular class, not a str subclass) ──────────────────


class DerivedPath:
    """A Nix derived path — an expression that evaluates to 1+ store paths.

    ``DerivedPath(s)`` parses the wire-format string and constructs the
    appropriate internal representation.  A single class serves all
    variants; use the properties below to dispatch instead of
    ``isinstance`` checks.

    Unlike the C++ model, this is *not* a ``str`` subclass — use
    Use ``str(dp)`` for the wire format (``!``) or ``dp.to_string()`` for the
    CLI format (``^``).

    Attributes
    ----------
    _drv_path : StorePath
        The root derivation store path (or the path itself for opaque).
    _chain : tuple[str, ...]
        Intermediate output names, outer-to-inner, for nested (dynamic-
        derivation) chains.  Empty for non-nested paths.
    _outputs : OutputsSpec | None
        The top-level outputs specification.  ``None`` means the path is
        opaque — it does not reference a derivation at all.
    extrainfo : Any | None
        Optional metadata (e.g. why this path is required).
    """

    def __init__(self, s: str | StorePath) -> None:
        if isinstance(s, StorePath):
            s = str(s)
        drv_path, chain, outputs = _parse_components(s, "!")
        self._drv_path = drv_path
        self._chain = chain
        self._outputs = outputs
        self.extrainfo = getattr(drv_path, "extrainfo", None)

    @classmethod
    def _from_components(
        cls,
        drv_path: StorePath,
        chain: tuple[str, ...],
        outputs: OutputsSpec | None,
    ) -> Self:
        """Bypass ``__init__`` for internal construction (parse, outer, wrap)."""
        instance = object.__new__(cls)
        instance._drv_path = drv_path
        instance._chain = chain
        instance._outputs = outputs
        instance.extrainfo = getattr(drv_path, "extrainfo", None)
        return instance

    # -- public properties -------------------------------------------------

    @property
    def drv_path(self) -> str:
        """The root derivation store path as a plain string.

        For opaque paths this is the path itself.
        """
        return str(self._drv_path)

    @property
    def outputs(self) -> OutputsSpec | None:
        """The outputs specification, or ``None`` for opaque paths."""
        return self._outputs

    @property
    def output_names(self) -> set[str]:
        """Output names referenced by this path.

        An opaque path names no output, and a `.drv` name changes nothing.
        For built paths delegates to the ``OutputsSpec``.
        """
        if self._outputs is None:
            return set()
        if isinstance(self._outputs, OutputsAll):
            return {"*"}
        return set(self._outputs.names)

    @property
    def is_opaque(self) -> bool:
        """Whether this path is opaque (not derived from a build)."""
        return self._outputs is None

    @property
    def is_nested(self) -> bool:
        """Whether this path has intermediate chain levels.

        Nested paths require multiple build steps: building one level
        produces a ``.drv`` that must be built for the next, until the
        chain is exhausted.
        """
        return len(self._chain) > 0

    @property
    def chain(self) -> tuple[str, ...]:
        """Intermediate output names, outer-to-inner.

        Empty for non-nested paths.
        """
        return self._chain

    @property
    def derived(self) -> Self:
        """Structured representation — returns ``self`` for backward compat."""
        return self

    # -- chain walking (build-goal decomposition) -------------------------

    @property
    def outer(self) -> DerivedPath:
        """One level less nested: the path to build first.

        Moves the innermost chain element into ``outputs`` so that
        building this produces the next ``.drv`` in the chain.

        Returns ``self`` if the path is not nested.
        """
        if not self._chain:
            return self
        return DerivedPath._from_components(
            drv_path=self._drv_path,
            chain=self._chain[:-1],
            outputs=OutputsNames(frozenset({self._chain[-1]})),
        )

    def wrap(self, result: StorePath) -> DerivedPath:
        """After building ``outer``, call ``wrap`` to create the next level.

        ``result`` is the store path produced by building ``outer``
        (typically an intermediate ``.drv``).  The returned ``DerivedPath``
        has ``result`` as its root with the original top-level outputs.
        """
        return DerivedPath._from_components(
            drv_path=result,
            chain=(),
            outputs=self._outputs,
        )

    # -- serialization ----------------------------------------------------

    def to_string(self) -> str:
        """CLI/public API format using the ``^`` separator.

        For opaque paths this is just the store path string.
        """
        if self._outputs is None:
            return str(self._drv_path)
        parts = [str(self._drv_path)]
        parts.extend(self._chain)
        parts.append(self._outputs.to_string())
        return "^".join(parts)

    def __str__(self) -> str:
        """Wire-protocol format using the ``!`` separator."""
        if self._outputs is None:
            return str(self._drv_path)
        parts = [str(self._drv_path)]
        parts.extend(self._chain)
        parts.append(self._outputs.to_string())
        return "!".join(parts)

    def __format__(self, format_spec: str) -> str:
        """Format the wire-protocol string with the given format spec."""
        return format(str(self), format_spec)

    def base_store_path(self) -> StorePath:
        """The root store path for this derived path.

        For opaque paths this returns the path itself.
        """
        return self._drv_path

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DerivedPath):
            return NotImplemented
        return self._drv_path == other._drv_path and self._chain == other._chain and self._outputs == other._outputs

    def __hash__(self) -> int:
        return hash((self._drv_path, self._chain, self._outputs))

    def __repr__(self) -> str:
        return f"DerivedPath({str(self)!r})"


# ── Internal parsing helpers ------------------------------------------


def _parse_components(
    s: str,
    sep: str,
) -> tuple[StorePath, tuple[str, ...], OutputsSpec | None]:
    """Parse a derived path string into ``(drv_path, chain, outputs)``.

    The last ``sep``-separated segment is parsed as an ``OutputsSpec``.
    Everything to its left is walked right-to-left: each remaining
    separator splits off a chain entry (an intermediate output name),
    and the leftmost segment is the root ``StorePath``.
    """
    n = s.rfind(sep)
    if n == -1:
        # **A string with no separator is opaque, and a `.drv` name changes
        # nothing.** `DerivedPath::parseWith` at `derived-path.cc:150` answers
        # `DerivedPath::Opaque` for one, in both formats. The rule that reads a
        # bare `.drv` as "every output of it" belongs to the command line of
        # Nix, and to `StorePathWithOutputs::toDerivedPath`, which the wire
        # stopped using at protocol 1.30.
        #
        # This read a bare `.drv` as `OutputsAll`, so
        # `builtins.unsafeDiscardOutputDependency drvPath` built the
        # derivation. `nix-daemon` makes the path valid and answers the path
        # itself, and `build.sh:91` reads that answer.
        return (StorePath(s), (), None)

    outputs = _parse_outputs_spec(s[n + 1 :])
    remaining = s[:n]

    chain_parts: list[str] = []
    while True:
        m = remaining.rfind(sep)
        if m == -1:
            drv_path = StorePath(remaining)
            return (drv_path, tuple(reversed(chain_parts)), outputs)
        chain_parts.append(remaining[m + 1 :])
        remaining = remaining[:m]


# ── Public parsing API --------------------------------------------------


def parse_derived_path(s: str) -> DerivedPath:
    """Parse a public-format (``^``) derived path string."""
    drv_path, chain, outputs = _parse_components(s, "^")
    return DerivedPath._from_components(
        drv_path=drv_path,
        chain=chain,
        outputs=outputs,
    )


def parse_derived_path_legacy(s: str) -> DerivedPath:
    """Parse a wire-format (``!``) derived path string."""
    drv_path, chain, outputs = _parse_components(s, "!")
    return DerivedPath._from_components(
        drv_path=drv_path,
        chain=chain,
        outputs=outputs,
    )
