"""The remount of a read-only store keeps every flag its mount has, as `remountReadOnlyWritable` does.

`libutil/linux/linux-namespaces.cc`: in a user namespace a mount's `nodev`,
`nosuid` and `noexec` are locked, and a remount that drops one fails with EPERM.
"""

from __future__ import annotations

import os

import pytest

from pynixd_store import mount_namespace as mn

pytestmark = pytest.mark.skipif(not mn.LINUX, reason="Linux mount flags")


def test_a_plain_mount_is_remounted_as_a_bind() -> None:
    assert mn.remount_flags(os.ST_RDONLY) == mn.MS_REMOUNT | mn.MS_BIND


def test_each_flag_of_the_mount_is_kept() -> None:
    found = os.ST_RDONLY | os.ST_NOSUID | os.ST_NODEV | os.ST_NOATIME | mn.ST_NOSYMFOLLOW
    want = mn.MS_REMOUNT | mn.MS_BIND | mn.MS_NOSUID | mn.MS_NODEV | mn.MS_NOATIME | mn.MS_NOSYMFOLLOW
    assert mn.remount_flags(found) == want


def test_read_only_itself_is_dropped() -> None:
    assert mn.remount_flags(os.ST_RDONLY | os.ST_NOEXEC) == mn.MS_REMOUNT | mn.MS_BIND | mn.MS_NOEXEC
