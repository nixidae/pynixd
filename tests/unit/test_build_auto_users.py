"""`pynixd_store.builder.users.acquire_auto`: `AutoUserLock::acquire` (`unix/user-lock.cc:165`)."""

from __future__ import annotations

import os
import pwd
from typing import TYPE_CHECKING

import pytest

from pynixd_store.builder.users import IDS_PER_BUILD, BuildUserError, acquire_auto

if TYPE_CHECKING:
    from pathlib import Path

START = 0x34000000


def test_each_build_takes_the_next_free_slot(tmp_path: Path) -> None:
    """A slot is `flock` on `slot-<n>`; its uid is `start-id + n * 65536`, and in a user namespace so is its gid."""
    first = acquire_auto(tmp_path, "", start_id=START, id_count=2 * IDS_PER_BUILD, user_namespace=True)
    second = acquire_auto(tmp_path, "", start_id=START, id_count=2 * IDS_PER_BUILD, user_namespace=True)
    assert first is not None
    assert second is not None
    assert (first.uid, first.gid, first.groups) == (START, START, ())
    assert (second.uid, second.gid) == (START + IDS_PER_BUILD, START + IDS_PER_BUILD)
    assert acquire_auto(tmp_path, "", start_id=START, id_count=2 * IDS_PER_BUILD, user_namespace=True) is None
    first.release()
    again = acquire_auto(tmp_path, "", start_id=START, id_count=2 * IDS_PER_BUILD, user_namespace=True)
    assert again is not None
    assert again.uid == START
    again.release()
    second.release()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["slot-0", "slot-1"]


def test_a_uid_range_build_owns_the_whole_slot(tmp_path: Path) -> None:
    """`getUIDRange` (`user-lock.hh:20`): 65536 ids from the slot's uid with `uid-range`, one without."""
    one = acquire_auto(tmp_path, "", start_id=START, id_count=2 * IDS_PER_BUILD, user_namespace=True)
    whole = acquire_auto(
        tmp_path, "", start_id=START, id_count=2 * IDS_PER_BUILD, user_namespace=True, ids=IDS_PER_BUILD
    )
    assert one is not None
    assert whole is not None
    assert (one.uid, one.last_uid) == (START, START)
    assert (whole.uid, whole.last_uid) == (START + IDS_PER_BUILD, START + 2 * IDS_PER_BUILD - 1)
    one.release()
    whole.release()


def test_without_a_user_namespace_the_gid_is_the_build_users_group(tmp_path: Path) -> None:
    """`AutoUserLock::acquire` (`user-lock.cc:207`) reads `build-users-group`, and names a missing one."""
    with pytest.raises(BuildUserError, match="the group 'no-such-group' specified in 'build-users-group'"):
        acquire_auto(tmp_path, "no-such-group", start_id=START, id_count=IDS_PER_BUILD, user_namespace=False)
    # The refusal lets go of the slot.
    found = acquire_auto(tmp_path, "", start_id=START, id_count=IDS_PER_BUILD, user_namespace=True)
    assert found is not None
    found.release()


def test_a_uid_with_an_account_is_refused(tmp_path: Path) -> None:
    """`user-lock.cc:199`: an allocated uid must have no account."""
    own = os.getuid()
    name = pwd.getpwuid(own).pw_name
    with pytest.raises(BuildUserError, match=f"auto-allocated UID {own} clashes with existing user account '{name}'"):
        acquire_auto(tmp_path, "", start_id=own, id_count=IDS_PER_BUILD, user_namespace=True)
