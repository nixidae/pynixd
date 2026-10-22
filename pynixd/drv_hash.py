"""`hashDerivationModulo` over the closure of a derivation.

`Derivation.hash_derivation_modulo` hashes one derivation. It needs the hash
of each output that the derivation reads from another derivation, so this
walks the input derivations and collects those hashes.

`hashDerivationModulo` at `derivations.cc:891` of Nix gives two rules, and
each one is easy to get wrong:

- **The walk does not mask the output paths, and the top of the walk does.**
  `pathDerivationModulo` calls `hashDerivationModulo(..., false)`, and the
  caller of the outermost derivation passes `true`.
- **The key of an input is the plain hexadecimal hash of that output.** Nix
  does not hash it a second time.

Measured against the 235 input-addressed derivations of one machine: the path
that `makeOutputPath` builds from each hash is the path that the derivation
itself states.

The hash names a derivation output on the wire, as `sha256:<hex>!<name>`.
Issue nixidae/nixidae#218 gives the answer that needs it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from .drv_parser import Derivation

log = structlog.get_logger(__name__)

type DerivationReader = Callable[[str], Awaitable[Derivation | None]]
type OutputHashes = dict[str, str]

# **The walk reads every derivation of the closure, so it keeps the answers.**
# `DaemonStore.read_derivation` asks the daemon for the path information and
# then for a whole NAR, so one walk over a large closure is thousands of
# requests. Nix keeps the same map, at `drvHashes` of `derivations.cc`.
#
# A store path is a content address of the text of the derivation, and the
# hash reads that text alone, so an entry can never go stale.
_CACHE: dict[str, OutputHashes] = {}
# Above this, the oldest entries go. A daemon runs for a long time and meets
# many derivations, and each entry holds one short string for each output.
_CACHE_LIMIT = 20000


def _remember(cache: dict[str, OutputHashes], drv_path: str, hashes: OutputHashes) -> None:
    if len(cache) >= _CACHE_LIMIT:
        for old in list(cache)[: _CACHE_LIMIT // 4]:
            del cache[old]
    cache[drv_path] = hashes


async def output_hashes(
    parsed: Derivation,
    read: DerivationReader,
    cache: dict[str, OutputHashes] | None = None,
) -> OutputHashes | None:
    """Return the hash of each output of *parsed*, as hexadecimal.

    Answers `None` when the closure gives no answer. A dynamic input
    derivation and a missing input derivation both do that, and neither one
    is a fault of the caller.

    *cache* holds the answer for each input derivation, and it lasts as long
    as the caller keeps it. The default is the one of this module, which lasts
    as long as the process. A test that wants no memory passes its own.
    """
    inputs = await _input_hashes(parsed, read, _CACHE if cache is None else cache)
    if inputs is None:
        return None
    return parsed.hash_derivation_modulo(mask_outputs=True, input_drv_hashes=inputs)


async def _closure_hashes(drv_path: str, read: DerivationReader, cache: dict[str, OutputHashes]) -> OutputHashes | None:
    """The hashes of one input derivation, with the output paths in place."""
    cached = cache.get(drv_path)
    if cached is not None:
        return cached
    parsed = await read(drv_path)
    if parsed is None:
        log.debug("drv_hash_input_missing", drv_path=drv_path)
        return None
    inputs = await _input_hashes(parsed, read, cache)
    if inputs is None:
        return None
    hashes = parsed.hash_derivation_modulo(mask_outputs=False, input_drv_hashes=inputs)
    _remember(cache, drv_path, hashes)
    return hashes


async def _input_hashes(
    parsed: Derivation,
    read: DerivationReader,
    cache: dict[str, OutputHashes],
) -> dict[str, list[str]] | None:
    """Collect `{hexadecimal hash: [output name, ...]}` for the input derivations.

    **A dynamic input takes part, and its tree does not.**
    `hashDerivationModulo` at `derivations.cc:931` walks `drv.inputDrvs.map`
    and reads `node.value`, which holds the direct outputs alone. It puts each
    one in a fresh map that carries no child, and `Derivation::unparse` writes
    that map. So an input that names a dynamic output only adds nothing to the
    text.

    This walk answered `None` for a dynamic derivation, so
    `EnsureDerivedPathGoal` could not re-key the realisation of one. The client
    asked `queryPartialDerivationOutputMap` for the derivation it instantiated,
    got no path, and `nix-build.cc:730` stopped the program on an assertion.
    `dyn-drv:dep-built-drv` is the test.

    The walk still reads each dynamic input, because Nix reads it as well:
    `pathDerivationModulo` runs for every entry of the map, and the derivation
    that it names has to be there.
    """
    # A set, sorted: Nix's `StringSet` (`derivations.cc:951`). Two fixed-output
    # inputs with one hash are one entry with one "out".
    inputs: dict[str, set[str]] = {}
    wanted: list[tuple[str, list[str]]] = [(str(path), names) for path, names in parsed.input_drvs.items()]
    wanted += [(str(path), node.outputs) for path, node in parsed.dynamic_input_drvs.items()]
    for input_drv, output_names in wanted:
        hashes = await _closure_hashes(input_drv, read, cache)
        if hashes is None:
            return None
        for output_name in output_names:
            digest = hashes.get(output_name)
            if digest is None:
                log.debug("drv_hash_output_missing", drv_path=input_drv, output=output_name)
                return None
            inputs.setdefault(digest, set()).add(output_name)
    return {digest: sorted(names) for digest, names in inputs.items()}
