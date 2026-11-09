"""`pynixd.machines` against Nix's own cases, `src/libstore-tests/machines.cc`.

The `@file` cases use the two files of `src/libstore-tests/data/machines/`,
written here, because the test reads the data from no Nix source.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pynixd.machines import MachinesError, parse_config

if TYPE_CHECKING:
    from pathlib import Path

ARCH = frozenset({"TEST_ARCH-TEST_OS"})

VALID = """\
nix@scratchy.labs.cs.uu.nl  i686-linux      /home/nix/.ssh/id_scratchy_auto        8 1 kvm
nix@itchy.labs.cs.uu.nl     i686-linux      /home/nix/.ssh/id_scratchy_auto        8 2
nix@poochie.labs.cs.uu.nl   i686-linux      /home/nix/.ssh/id_scratchy_auto        1 2 kvm benchmark c3NoLXJzYQo=
"""


def authorities(builders: str) -> set[str]:
    return {m.reference.authority for m in parse_config(set(), builders)}


def test_empty_builders() -> None:
    assert parse_config(set(), "") == []


def test_uri_only() -> None:
    [m] = parse_config(ARCH, "nix@scratchy.labs.cs.uu.nl")
    assert m.store_uri == "ssh://nix@scratchy.labs.cs.uu.nl"
    assert m.systems == ARCH
    assert m.ssh_key is None
    assert m.max_jobs == 1
    assert m.speed_factor == 1
    assert m.supported_features == frozenset()
    assert m.mandatory_features == frozenset()
    assert m.ssh_public_host_key == ""


def test_uri_with_port() -> None:
    [m] = parse_config(ARCH, "nix@scratchy.labs.cs.uu.nl:2222")
    assert m.store_uri == "ssh://nix@scratchy.labs.cs.uu.nl:2222"


def test_defaults() -> None:
    [m] = parse_config(ARCH, "nix@scratchy.labs.cs.uu.nl - - - - - - -")
    assert m.store_uri == "ssh://nix@scratchy.labs.cs.uu.nl"
    assert m.systems == ARCH
    assert (m.ssh_key, m.max_jobs, m.speed_factor, m.ssh_public_host_key) == (None, 1, 1, "")


def test_new_line_separator() -> None:
    assert authorities("nix@scratchy.labs.cs.uu.nl\nnix@itchy.labs.cs.uu.nl") == {
        "nix@scratchy.labs.cs.uu.nl",
        "nix@itchy.labs.cs.uu.nl",
    }


def test_semicolon_separator() -> None:
    assert len(parse_config(set(), "nix@scratchy.labs.cs.uu.nl ; nix@itchy.labs.cs.uu.nl")) == 2


def test_comments_and_semicolon_separator() -> None:
    builders = (
        "# This is a comment ; this is still that comment\n"
        "nix@scratchy.labs.cs.uu.nl ; nix@itchy.labs.cs.uu.nl\n"
        "# This is also a comment ; this also is still that comment\n"
        "nix@scabby.labs.cs.uu.nl\n"
    )
    assert authorities(builders) == {
        "nix@scratchy.labs.cs.uu.nl",
        "nix@itchy.labs.cs.uu.nl",
        "nix@scabby.labs.cs.uu.nl",
    }


def test_funny_whitespace() -> None:
    builders = (
        "        # comment ; comment\n"
        "   nix@scratchy.labs.cs.uu.nl ; nix@itchy.labs.cs.uu.nl   \n"
        "\n    \n"
        "\n ;;; \n"
        "\n ; ; \n"
        "nix@scabby.labs.cs.uu.nl\n\n"
    )
    assert len(parse_config(set(), builders)) == 3


@pytest.mark.parametrize("sep", [" ", "\t"])
def test_complete_single_builder(sep: str) -> None:
    line = sep.join(
        [
            "nix@scratchy.labs.cs.uu.nl",
            "i686-linux",
            "/home/nix/.ssh/id_scratchy_auto",
            "8",
            "3",
            "kvm",
            "benchmark",
            "SSH+HOST+PUBLIC+KEY+BASE64+ENCODED==",
        ]
    )
    [m] = parse_config(set(), line)
    assert m.reference.authority == "nix@scratchy.labs.cs.uu.nl"
    assert m.systems == {"i686-linux"}
    assert m.ssh_key == "/home/nix/.ssh/id_scratchy_auto"
    assert (m.max_jobs, m.speed_factor) == (8, 3)
    assert m.supported_features == {"kvm"}
    assert m.mandatory_features == {"benchmark"}
    assert m.ssh_public_host_key == "SSH+HOST+PUBLIC+KEY+BASE64+ENCODED=="


def test_multi_options() -> None:
    builders = "nix@scratchy.labs.cs.uu.nl Arch1,Arch2 - - - SupportedFeature1,SupportedFeature2 MandatoryFeature1,MandatoryFeature2"
    [m] = parse_config(set(), builders)
    assert m.systems == {"Arch1", "Arch2"}
    assert m.supported_features == {"SupportedFeature1", "SupportedFeature2"}
    assert m.mandatory_features == {"MandatoryFeature1", "MandatoryFeature2"}


@pytest.mark.parametrize(
    "line",
    [
        "nix@scratchy.labs.cs.uu.nl - - eight",
        "nix@scratchy.labs.cs.uu.nl - - -1",
        "nix@scratchy.labs.cs.uu.nl - - 8 three",
        "nix@scratchy.labs.cs.uu.nl - - 8 -3",
        "nix@scratchy.labs.cs.uu.nl - - 8 3 - - BAD_BASE64",
    ],
)
def test_incorrect_format(line: str) -> None:
    with pytest.raises(MachinesError):
        parse_config(set(), line)


def test_file_reference(tmp_path: Path) -> None:
    machines = tmp_path / "valid"
    machines.write_text(VALID)
    assert authorities(f"@{machines}") == {
        "nix@scratchy.labs.cs.uu.nl",
        "nix@itchy.labs.cs.uu.nl",
        "nix@poochie.labs.cs.uu.nl",
    }


def test_file_reference_to_empty_file(tmp_path: Path) -> None:
    machines = tmp_path / "empty-machines"
    machines.write_text("")
    assert parse_config(set(), f"@{machines}") == []


def test_missing_file_reference() -> None:
    assert parse_config(set(), "@/not/a/file") == []


def test_file_reference_to_incorrect_file(tmp_path: Path) -> None:
    machines = tmp_path / "bad_format"
    machines.write_text("nix@scratchy.labs.cs.uu.nl - - eight\n")
    with pytest.raises(MachinesError):
        parse_config(set(), f"@{machines}")


def test_build_remote_lines() -> None:
    """The three builders of `tests/functional/build-remote.sh:20-25`, with the store params Nix completes."""
    root = "/t"
    builders = "; ".join(
        [
            f"ssh://localhost?remote-store={root}/machine1?system-features=foo - - 1 1 foo",
            f"{root}/machine2 - - 1 1 bar",
            f"ssh-ng://localhost?remote-store={root}/machine3?system-features=baz - - 1 1 baz",
        ]
    )
    legacy, local, ng = parse_config({"x86_64-linux"}, builders)

    assert legacy.reference.scheme == "ssh"
    assert dict(legacy.params) == {
        "remote-store": f"{root}/machine1?system-features=foo",
        "max-connections": "1",
        "log-fd": "4",
        "system-features": "foo",
    }
    assert (local.reference.scheme, local.reference.path) == ("local", f"{root}/machine2")
    assert dict(local.params) == {"system-features": "bar"}
    assert (ng.reference.scheme, ng.reference.authority) == ("ssh-ng", "localhost")
    assert dict(ng.params) == {"remote-store": f"{root}/machine3?system-features=baz", "system-features": "baz"}
    assert ng.supported_features == {"baz"}


def test_features_of_the_url_come_first() -> None:
    """`completeStoreReference` appends the line's features to the URL's own `system-features`."""
    [m] = parse_config(set(), "/s?system-features=kvm - - 1 1 big-parallel benchmark")
    assert m.params["system-features"] == "kvm big-parallel benchmark"


def test_mandatory_features() -> None:
    [m] = parse_config({"x86_64-linux"}, "/s - - 1 1 kvm benchmark")
    assert m.all_supported({"kvm", "benchmark"})
    assert not m.mandatory_met({"kvm"})
    assert m.mandatory_met({"kvm", "benchmark"})
    assert m.system_supported("builtin")
