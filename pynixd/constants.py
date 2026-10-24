"""
Nix daemon protocol constants and magic numbers.
"""

from typing import Final

# The feature names are wire constants, and `nix_daemon_protocol` holds them
# with the reference to the Nix source of each one. This module states the
# rest of the wire constants itself, and a second copy of the feature names
# would be a second thing to keep true.
from nix_daemon_protocol.constants import (
    FEATURE_ADD_TEMP_ROOTS as FEATURE_ADD_TEMP_ROOTS,
    FEATURE_ADD_TO_STORE_SCANNING as FEATURE_ADD_TO_STORE_SCANNING,
    FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS as FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS,
    FEATURE_DISABLE_SET_OPTIONS as FEATURE_DISABLE_SET_OPTIONS,
    FEATURE_EXCHANGE_PROTOCOL as FEATURE_EXCHANGE_PROTOCOL,
    FEATURE_REALISATION_WITH_PATH as FEATURE_REALISATION_WITH_PATH,
    FEATURE_SUBMIT_OUTPUT as FEATURE_SUBMIT_OUTPUT,
    MINIMUM_CLIENT_PROTOCOL as MINIMUM_CLIENT_PROTOCOL,
    STANDARD_FEATURES as STANDARD_FEATURES,
    SUPPORTED_STANDARD_FEATURES as SUPPORTED_STANDARD_FEATURES,
    negotiate_features as negotiate_features,
)


def proto(major: int, minor: int) -> int:
    """Encode a protocol version as a single int."""
    return (major << 8) | minor


def proto_str(version: int) -> str:
    """Format a protocol version int as 'major.minor'."""
    return f"{version >> 8}.{version & 0xFF}"


# ── Protocol Magic ──────────────────────────────────────────────────

WORKER_MAGIC_1: Final[int] = 0x6E697863  # client hello
WORKER_MAGIC_2: Final[int] = 0x6478696F  # server hello
PROTOCOL_VERSION: Final[int] = proto(1, 38)
MINIMUM_REMOTE_PROTOCOL: Final[int] = proto(1, 32)


# ── Diagnostics ───────────────────────────────────────────────────

# The default cap on the stall dump file (`stall_traceback_path`).
#
# Here rather than in `health.py` so `config.py` can name it without importing
# health, which would pull `prometheus_client` into the import path of
# anything that reads configuration. Issue #30 measured what an eager import
# costs every daemon start.
STALL_TRACEBACK_MAX_BYTES: Final[int] = 8 * 1024 * 1024


# ── SSH ───────────────────────────────────────────────────────────

# **Prefer AES-GCM, because almost every box pynixd runs on has AES-NI.**
# asyncssh offers chacha20-poly1305 first, which is the right default for a
# library that cannot know its host and the wrong one here. Measured on a host
# with `aes` and `vaes`, one MiB through asyncssh's own cipher objects:
#
#     chacha20-poly1305@openssh.com   3678 MiB/s   0.27 ms/MiB
#     aes256-gcm@openssh.com          9041 MiB/s   0.11 ms/MiB
#     aes128-gcm@openssh.com         10806 MiB/s   0.09 ms/MiB
#
# That is 2.46x on the cipher, and it is event loop CPU on every byte.
#
# **It does not show end to end, and the reason is worth keeping.** Eight
# concurrent clients moving 128 MiB measured 185-193 MiB/s pushing under
# chacha20 and 165-177 under AES-GCM -- within noise, and if anything worse.
# Crypto is about 0.27 ms/MiB against roughly 7 ms/MiB for the Python that
# forwards the bytes, so it is ~4% of the cost and cannot move the total. The
# saving is real loop CPU (~0.16 ms/MiB, about 460 ms over a 2.8 GiB closure)
# and nothing more. Do not quote the 2.46x as a transfer speedup.
#
# **This only binds where pynixd is the SSH client.** The chosen cipher is the
# first entry of the *client's* list that the server also offers (RFC 4253
# 7.1), and OpenSSH leads with chacha20-poly1305. So a `nix copy` into pynixd
# keeps chacha20 whatever the server offers; changing that needs
# `NIX_SSHOPTS="-c aes256-gcm@openssh.com"` on the pushing side.
#
# **The `^` prefix makes this a preference and not a whitelist.** asyncssh
# reads it as "these first, then the defaults" (`connection._expand_algs`), so
# chacha20 stays available. That matters: a host without AES-NI does software
# AES far slower than chacha20, and pinning AES would punish exactly the hosts
# the default was protecting.
SSH_ENCRYPTION_ALGS: Final[str] = "^aes256-gcm@openssh.com,aes128-gcm@openssh.com"

# A peer that stops answering sends no FIN, so without keepalives a connection
# to it never closes and the builds on it wait forever: 18 builds sat 2 h on a
# wedged VM store. Four missed 15 s probes close it in about a minute, and the
# build then retries elsewhere. nixidae/nixidae#1063.
SSH_KEEPALIVE_INTERVAL: Final[float] = 15.0
SSH_KEEPALIVE_COUNT_MAX: Final[int] = 4


# ── Stderr message types ──────────────────────────────────────────

STDERR_NEXT: Final[int] = 0x6F6C6D67
STDERR_LAST: Final[int] = 0x616C7473
STDERR_ERROR: Final[int] = 0x63787470
STDERR_START_ACTIVITY: Final[int] = 0x53545254
STDERR_STOP_ACTIVITY: Final[int] = 0x53544F50
STDERR_RESULT: Final[int] = 0x52534C54
