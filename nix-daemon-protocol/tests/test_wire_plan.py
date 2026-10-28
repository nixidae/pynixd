"""The per-session wire plan: one resolved codec per (model, version, features).

A session negotiates one version and one feature set, then decodes thousands
of operations under them. The plan resolves every field's reader, writer and
default once for that shape, so the per-operation path is one cache lookup
and straight-line reads. These pin the sharing and the version boundary: a
plan that ignored the version would write 1.37 fields to a 1.36 peer.
"""

from __future__ import annotations

from nix_daemon_protocol import BuildResult, proto
from nix_daemon_protocol.wire_message import _wire_plan


def test_the_same_session_shape_shares_one_plan() -> None:
    """The same key returns the identical plan, not an equal one."""
    first = _wire_plan(BuildResult, proto(1, 38), frozenset())  # type: ignore[arg-type]  # zuban-bug: zuban checks the model class against Hashable; mypy accepts it as an lru_cache key
    assert _wire_plan(BuildResult, proto(1, 38), frozenset()) is first  # type: ignore[arg-type]  # zuban-bug: zuban checks the model class against Hashable; mypy accepts it as an lru_cache key


def test_a_version_boundary_resolves_a_different_plan() -> None:
    """`BuildResult` grows CPU fields at 1.37; the plan follows the version."""
    old = _wire_plan(BuildResult, proto(1, 36), frozenset())  # type: ignore[arg-type]  # zuban-bug: zuban checks the model class against Hashable; mypy accepts it as an lru_cache key
    new = _wire_plan(BuildResult, proto(1, 37), frozenset())  # type: ignore[arg-type]  # zuban-bug: zuban checks the model class against Hashable; mypy accepts it as an lru_cache key

    assert old is not new
    old_names = [name for name, _reader, _depends, _deserialize in old[0]]
    new_names = [name for name, _reader, _depends, _deserialize in new[0]]
    assert "cpu_user" not in old_names
    assert "cpu_user" in new_names
