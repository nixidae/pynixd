"""The port of `authPeer` and `matchUser`, `src/nix/unix/daemon.cc:178-257`."""

from __future__ import annotations

import grp
import os
import pwd
import socket
from types import SimpleNamespace

import pytest

from nix_daemon_protocol.find_roots import FindRootsEntry
from pynixd.handlers.find_roots import censor
from pynixd.serde.auth import Role
from pynixd.trust import Peer, PeerRefusedError, TrustPolicy, authorise, match_user, peer_of, restrict_overrides

USERS = {1000: "alice", 1001: "bob", 0: "root", 30001: "nixbld1"}
GROUPS = {100: ("users", ["alice", "bob"]), 10: ("wheel", ["alice"]), 30000: ("nixbld", ["nixbld1"])}


@pytest.fixture(autouse=True)
def databases(monkeypatch: pytest.MonkeyPatch) -> None:
    """A fixed passwd and group database, so no test depends on the host's."""

    def getpwuid(uid: int) -> SimpleNamespace:
        if uid not in USERS:
            raise KeyError(uid)
        return SimpleNamespace(pw_name=USERS[uid])

    def getgrgid(gid: int) -> SimpleNamespace:
        if gid not in GROUPS:
            raise KeyError(gid)
        return SimpleNamespace(gr_name=GROUPS[gid][0], gr_mem=GROUPS[gid][1])

    def getgrnam(name: str) -> SimpleNamespace:
        for group_name, members in GROUPS.values():
            if group_name == name:
                return SimpleNamespace(gr_name=group_name, gr_mem=members)
        raise KeyError(name)

    monkeypatch.setattr(pwd, "getpwuid", getpwuid)
    monkeypatch.setattr(grp, "getgrgid", getgrgid)
    monkeypatch.setattr(grp, "getgrnam", getgrnam)


class TestMatchUser:
    def test_star_matches_anyone(self):
        assert match_user(None, None, ["*"])

    def test_name(self):
        assert match_user("alice", "users", ["alice"])
        assert not match_user("bob", "users", ["alice"])

    def test_primary_group_by_name(self):
        assert match_user("carol", "users", ["@users"])

    def test_secondary_group_through_gr_mem(self):
        assert match_user("alice", "users", ["@wheel"])
        assert not match_user("bob", "users", ["@wheel"])

    def test_an_unknown_group_matches_nothing(self):
        assert not match_user("alice", "users", ["@nosuchgroup"])


class TestAuthorise:
    def test_defaults_trust_root_alone(self):
        assert authorise(Peer(1, 0, 0), TrustPolicy()) == (Role.ADMIN, "root")
        assert authorise(Peer(1, 1000, 100), TrustPolicy()) == (Role.USER, "alice")

    def test_trusted_through_a_group(self):
        policy = TrustPolicy(trusted_users=("root", "@wheel"))
        assert authorise(Peer(1, 1000, 100), policy)[0] == Role.ADMIN
        assert authorise(Peer(1, 1001, 100), policy)[0] == Role.USER

    def test_a_user_outside_allowed_users_is_refused_with_nixs_message(self):
        policy = TrustPolicy(allowed_users=("alice",))
        with pytest.raises(PeerRefusedError, match=r"^user 'bob' is not allowed to connect to the Nix daemon$"):
            authorise(Peer(1, 1001, 100), policy)

    def test_trusted_users_need_not_be_in_allowed_users(self):
        """`daemon.cc:254`: the allowed test runs only for an untrusted user."""
        policy = TrustPolicy(trusted_users=("bob",), allowed_users=("alice",))
        assert authorise(Peer(1, 1001, 100), policy)[0] == Role.ADMIN

    def test_the_build_users_group_is_refused_even_when_trusted(self):
        policy = TrustPolicy(trusted_users=("*",), build_users_group="nixbld")
        with pytest.raises(PeerRefusedError, match="'nixbld1'"):
            authorise(Peer(1, 30001, 30000), policy)

    def test_an_unset_build_users_group_refuses_nobody(self):
        assert authorise(Peer(1, 30001, 30000), TrustPolicy())[0] == Role.USER

    def test_a_uid_without_a_passwd_entry_is_named_by_its_number(self):
        """A container user with no entry here. Nix names it by the number too."""
        assert authorise(Peer(1, 4242, 4242), TrustPolicy()) == (Role.USER, "4242")
        assert authorise(Peer(1, 4242, 4242), TrustPolicy(trusted_users=("4242",)))[0] == Role.ADMIN


class TestRestrictOverrides:
    """`ClientSettings::apply`, `daemon.cc:234-310`."""

    POLICY = TrustPolicy(substituters=("https://cache.nixos.org/",), trusted_substituters=("https://t.example/",))

    def test_the_four_unrestricted_settings_pass(self):
        overrides = {"timeout": "5", "max-silent-time": "6", "build-poll-interval": "1", "connect-timeout": "2"}
        assert restrict_overrides(overrides, self.POLICY) == (overrides, [])

    def test_an_alias_is_refused(self):
        """Nix compares `buildTimeout.name`, which is `timeout`, and no alias."""
        kept, warnings = restrict_overrides({"build-timeout": "5"}, self.POLICY)
        assert kept == {}
        assert warnings == [
            "ignoring the client-specified setting 'build-timeout', because it is a restricted setting "
            "and you are not a trusted user"
        ]

    def test_builders_passes_only_when_empty(self):
        assert restrict_overrides({"builders": ""}, self.POLICY) == ({"builders": ""}, [])
        assert restrict_overrides({"builders": "ssh://x"}, self.POLICY)[0] == {}

    def test_a_trusted_key_is_refused(self):
        assert restrict_overrides({"trusted-public-keys": "k:abc"}, self.POLICY)[0] == {}
        assert restrict_overrides({"require-sigs": "false"}, self.POLICY)[0] == {}

    def test_substituters_keep_the_trusted_ones_and_add_a_slash(self):
        kept, warnings = restrict_overrides(
            {"substituters": "https://cache.nixos.org https://t.example/ https://evil.example"}, self.POLICY
        )
        assert kept == {"substituters": "https://cache.nixos.org/ https://t.example/"}
        assert warnings == [
            "ignoring untrusted substituter 'https://evil.example', you are not a trusted user.\n"
            "Run `man nix.conf` for more information on the `substituters` configuration option."
        ]

    def test_extra_substituters_is_a_restricted_setting(self):
        """`setSubstituters` matches the name and its aliases, and `extra-` is neither."""
        assert restrict_overrides({"extra-substituters": "https://t.example/"}, self.POLICY)[0] == {}

    def test_settings_nix_ignores_for_everyone_pass_through(self):
        overrides = {"experimental-features": "nix-command", "plugin-files": ""}
        assert restrict_overrides(overrides, self.POLICY) == (overrides, [])


def test_censor_hides_temporary_and_runtime_roots():
    """`gc.cc:207,343`: one `{censored}` per path, and a `gcroots` link stays."""
    roots = [
        FindRootsEntry(link="/home/u/result", target="/nix/store/a"),
        FindRootsEntry(link="{temp:12}", target="/nix/store/a"),
        FindRootsEntry(link="/proc/12/maps", target="/nix/store/b"),
        FindRootsEntry(link="{memory:1}", target="/nix/store/b"),
    ]
    assert censor(roots) == [
        FindRootsEntry(link="/home/u/result", target="/nix/store/a"),
        FindRootsEntry(link="{censored}", target="/nix/store/a"),
        FindRootsEntry(link="{censored}", target="/nix/store/b"),
    ]


def test_policy_from_nix_config_show():
    config = {
        "trusted-users": {"value": ["root", "@wheel"]},
        "allowed-users": {"value": ["*"]},
        "build-users-group": {"value": "nixbld"},
        "substituters": {"value": ["https://cache.nixos.org/"]},
        "trusted-substituters": {"value": []},
        "experimental-features": {"value": ["ca-derivations", "nix-command"]},
        "max-substitution-jobs": {"value": 4},
    }
    assert TrustPolicy.from_nix_config(config) == TrustPolicy(
        ("root", "@wheel"),
        ("*",),
        "nixbld",
        experimental_features=("ca-derivations", "nix-command"),
        max_substitution_jobs=4,
    ), "the default substituters are Nix's, so this equality also checks the two list fields"


def test_peer_of_reads_this_process():
    """The kernel's record of the connecting process, over a real socket pair."""
    left, right = socket.socketpair(socket.AF_UNIX)
    with left, right:
        peer = peer_of(left)
    assert peer == Peer(pid=os.getpid(), uid=os.getuid(), gid=os.getgid())


def test_dropped_settings_warn_in_name_order():
    """`ClientSettings::apply` walks a `std::map`, so its warnings come sorted (`daemon.cc:232`)."""
    _kept, warnings = restrict_overrides({"zzz-setting": "1", "aaa-setting": "1"}, TrustPolicy())

    assert "'aaa-setting'" in warnings[0]
    assert "'zzz-setting'" in warnings[1]
