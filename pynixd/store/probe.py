"""Test builds that prove what a store builds: one per system, then one per feature on each system that built.

A `DaemonStore` sends them to its daemon. A local store with no daemon builds
them in pynixd's own sandbox (`LocalDBStore.probe_own_builder`). nixidae/nixidae#1541.
"""

from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable, Iterable
from typing import Any, Final, Protocol

import anyio
import structlog

from nix_daemon_protocol.protocol import ResultType
from nix_daemon_protocol.store_dir import store_prefix
from pynixd_store.utils import random_nix32_hash

from ..serde import BasicDerivation, BuildDerivationRequest, BuildMode, BuildResultStatus, DerivationOutput, StorePath

__all__ = [
    "ACCEPTED",
    "PROBE_CONCURRENCY",
    "PROBE_SYSTEM_SCRIPT",
    "ProbeSend",
    "feature_args",
    "probe_features",
    "probe_request",
    "probe_systems",
    "refusal",
]

log = structlog.get_logger(__name__)

PROBE_CONCURRENCY: Final[int] = 5
"""Probes in flight at once, over every system and every feature.

Each probe is one build, and over SSH each build takes a channel. `sshd`
permits ten sessions by default, and `nix` assumes the same ten for its own
channels, so half of that leaves room for a build running beside a probe.

Measured on issue #15: eighteen probes at once -- two systems by nine
features -- failed eight of them with `OPEN_REQUEST_SESSION_FAILED`, which
is eighteen minus the ten `sshd` allows. The store then sat in a 300 second
cooldown, so the failure was not transient from the caller's side. A client
cannot read `MaxSessions`, so this is a constant and not a negotiation."""

PROBE_SYSTEM_SCRIPT: Final[str] = (
    'echo {system} > "$out"; test -e "$out" && echo "probe wrote $out" >&2 || echo "probe wrote no $out" >&2'
)
"""The shell a system probe runs, and the one line it says about the result.

**A silent builder tells a refusal nothing.** `echo <system> > $out` alone
exits 0 and prints nothing, so a store that answered `failed to produce output
path` carried no evidence of where the builder had written. The report names
the path the builder saw, which a reader compares against the path Nix names
in that message. Issue #47."""

ACCEPTED: Final[frozenset[int]] = frozenset(
    {
        BuildResultStatus.BUILT,
        BuildResultStatus.SUBSTITUTED,
        BuildResultStatus.ALREADY_VALID,
        BuildResultStatus.RESOLVES_TO_ALREADY_VALID,
    }
)
"""The results that prove the store builds the probe."""

_BUILDER_OUTPUT_LINES: Final[int] = 6
"""How many lines of a refused probe's build log travel with its reason.

Enough to carry what the builder said and not enough to fill a log line. The
build log of a probe is six lines of `nix` chatter at most, because the
derivation is one `echo`."""

_BUILD_LOG_RESULTS: Final[frozenset[int]] = frozenset({ResultType.BUILD_LOG_LINE, ResultType.POST_BUILD_LOG_LINE})


class ProbeSend(Protocol):
    """Send one probe build: its name, whether the store took it, and why it did not."""

    def __call__(
        self,
        name: str,
        system: str,
        required_features: str,
        args: list[str],
        extra_env: dict[str, str] | None = None,
        /,
    ) -> Awaitable[tuple[str, bool, str]]: ...


def probe_request(
    name: str, system: str, required_features: str, args: list[str], extra_env: dict[str, str] | None = None
) -> BuildDerivationRequest:
    """A derivation with no inputs that runs `/bin/sh` *args*, under a fresh hash so no store holds its output."""
    drv_hash = random_nix32_hash()
    out_path = f"{store_prefix()}{drv_hash}-{name}"
    env: dict[str, str] = {"builder": "/bin/sh", "name": name, "out": out_path, "system": system, "hash": drv_hash}
    if required_features:
        env["requiredSystemFeatures"] = required_features
    if extra_env:
        env.update(extra_env)
    return BuildDerivationRequest(
        drv_path=StorePath(path=f"{store_prefix()}{drv_hash}-{name}.drv"),
        derivation=BasicDerivation(
            outputs={"out": DerivationOutput(path=out_path, method="", hash_digest="")},
            input_srcs=set(),
            platform=system,
            builder="/bin/sh",
            args=args,
            env=env,
        ),
        build_mode=BuildMode.NORMAL,
    )


def feature_args(feature: str) -> list[str]:
    """What a feature probe runs: `kvm` needs a writable `/dev/kvm`, every other feature only the build."""
    if feature == "kvm":
        return ["-c", "test -w /dev/kvm && echo kvm > $out || { echo 'kvm: /dev/kvm not writable' >&2; exit 1; }"]
    return ["-c", f"echo {feature} > $out"]


def builder_lines(messages: Iterable[Any]) -> list[str]:
    """Every line the builder wrote, from a build's log messages.

    **Build output is an activity result, not a log line.** The daemon sends
    it as `STDERR_RESULT` with type `BUILD_LOG_LINE`, and the text sits in
    `fields[0].valstr`. `LogResult` carries no `text` attribute at all, so
    reading `message.text` collected Nix's own progress chatter and dropped
    every line the builder wrote. Measured against nix 2.34.8: a builder that
    printed one line to stderr and produced no output path answered with
    exactly one `BUILD_LOG_LINE` result holding that line. Issue #47.
    """
    lines: list[str] = []
    for message in messages:
        if getattr(message, "result_type", None) not in _BUILD_LOG_RESULTS:
            continue
        lines.extend(field.valstr.rstrip("\n") for field in message.fields if field.valstr and field.valstr.strip())
    return lines


def refusal(error_msg: str, status: int, messages: Iterable[Any]) -> str:
    """Why a store refused a probe, with what its builder said.

    **`BuildResult.error_msg` alone does not say why a build produced no
    output.** On a GitHub runner the x86_64-linux probe answered `builder for
    '...-probe-system-x86_64-linux.drv' failed to produce output path for
    output 'out'`, which states the symptom and nothing else. pynixd issue #47.

    Nix's progress lines are the fallback, because a store that refuses before
    it starts a builder says why there and only there.
    """
    messages = list(messages)
    error = error_msg or f"status {status}"
    lines = builder_lines(messages) or [
        text.rstrip("\n") for message in messages if (text := getattr(message, "text", "")).strip()
    ]
    if not lines:
        return error
    return f"{error} [builder said: {' | '.join(lines[-_BUILDER_OUTPUT_LINES:])}]"


async def probe_systems(
    store_id: str, send: ProbeSend, candidates: set[str], limiter: anyio.CapacityLimiter
) -> set[str]:
    """The *candidates* the store builds an echo for."""
    # An anyio task group hands back no task object, so each child records
    # its answer at its own index.
    ordered = sorted(candidates)
    supported: list[bool] = [False] * len(ordered)
    refusals: list[str] = [""] * len(ordered)

    async def probe_system(index: int, system: str) -> None:
        async with limiter:
            _, ok, reason = await send(
                f"probe-system-{system}", system, "", ["-c", PROBE_SYSTEM_SCRIPT.format(system=system)]
            )
        supported[index] = ok
        refusals[index] = reason

    async with anyio.create_task_group() as tg:
        for index, system in enumerate(ordered):
            tg.start_soon(probe_system, index, system)

    systems = {system for system, ok in zip(ordered, supported, strict=True) if ok}
    log.info("systems_probed", store_id=store_id, systems=sorted(systems))
    if not systems:
        # A warning with the reasons: the scheduler answers every request with
        # `No compatible store for <system>`, which names no store. A CI run
        # reported 35 of that message and nothing about its cause. Issue #47.
        log.warning(
            "store_probed_no_system",
            store_id=store_id,
            candidates=ordered,
            refusals={system: reason for system, reason in zip(ordered, refusals, strict=True) if reason},
        )
    return systems


async def probe_features(
    store_id: str, send: ProbeSend, systems: set[str], features: set[str], limiter: anyio.CapacityLimiter
) -> dict[str, set[str]]:
    """Each of *systems*, with the *features* the store builds a probe for on it."""
    probes: list[Callable[[], Awaitable[tuple[str, bool, str]]]] = []
    keys: list[tuple[str, str]] = []
    for system in systems:
        for feature in features:
            extra_env = {
                "NIXBUILDNET_MIN_CPU": "1",
                "NIXBUILDNET_MAX_CPU": "1",
                "NIXBUILDNET_MIN_MEM": "128",
                "NIXBUILDNET_MAX_MEM": "128",
            }
            keys.append((system, feature))
            probes.append(
                functools.partial(send, f"probe-feature-{feature}", system, feature, feature_args(feature), extra_env)
            )

    supported: list[bool] = [False] * len(probes)

    async def run_probe(index: int, probe: Callable[[], Awaitable[tuple[str, bool, str]]]) -> None:
        async with limiter:
            _, ok, _reason = await probe()
        supported[index] = ok

    async with anyio.create_task_group() as tg:
        for index, probe in enumerate(probes):
            tg.start_soon(run_probe, index, probe)

    matrix: dict[str, set[str]] = {system: set() for system in systems}
    for (system, feature), ok in zip(keys, supported, strict=True):
        if ok:
            matrix[system].add(feature)
    log.info("features_probed", store_id=store_id, feature_matrix={k: sorted(v) for k, v in matrix.items()})
    return matrix
