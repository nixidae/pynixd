"""The darwin sandbox profile is the one `DarwinDerivationBuilder::setUser` writes (`darwin-derivation-builder.cc:41`).

Pure text and `lstat`, so it runs on every platform. `test_build_child_darwin.py` applies it on darwin.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from pynixd_store.builder import darwin
from pynixd_store.hintfmt import magenta

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd_store.builder.spec import DarwinSpec

STORE = "/nix/store"
OUT = f"{STORE}/00000000000000000000000000000000-out"


def _spec(tmp_path: Path, **over: object) -> DarwinSpec:
    build = tmp_path / "build"
    build.mkdir(exist_ok=True)
    spec: DarwinSpec = {
        "sandbox": True,
        "paths": [(str(build), str(build), False)],
        "inputs": [],
        "scratch": [OUT],
        "store_dir": STORE,
        "network": False,
        "log_violations": False,
        "additional_profile": "",
        "build_top": str(build),
        "allow_local_networking": False,
        "platform": "aarch64-darwin",
    }
    spec.update(over)  # type: ignore[typeddict-item]  # the tests vary one key at a time
    return spec


def test_without_the_sandbox_the_profile_only_denies_setuid_files(tmp_path: Path) -> None:
    text = darwin.profile(_spec(tmp_path, sandbox=False, paths=[("/missing", "/missing", False)]))
    assert text == "(version 1)\n" + darwin.MINIMAL


def test_the_sandbox_denies_by_default_and_opens_the_outputs_paths_and_ancestry(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    build = spec["build_top"]
    text = darwin.profile(spec)
    head = "(version 1)\n(deny default (with no-log))\n" + darwin.DEFAULTS
    assert darwin.NETWORK not in text
    # Each parent of the build directory, `/` included, and the store directory with its own parents but not `/`.
    parents = {"/nix", STORE}
    cur = build
    while cur != "/":
        cur = cur.rsplit("/", 1)[0] or "/"
        parents.add(cur)
    assert text == (
        head
        + f'(allow file-read* file-write* process-exec\n\t(subpath "{OUT}")\n)\n'
        + f'(allow file-read* file-write* process-exec\n\t(subpath "{build}")\n)\n'
        + "(allow file-read*\n"
        + "".join(f'\t(literal "{p}")\n' for p in sorted(parents))
        + ")\n"
    )


def test_an_input_is_opened_but_adds_no_ancestry(tmp_path: Path) -> None:
    """The ancestry comes from the paths before the inputs join them (`darwin-derivation-builder.cc:56`)."""
    deep = tmp_path / "deep/er"
    deep.mkdir(parents=True)
    text = darwin.profile(_spec(tmp_path, inputs=[str(deep)]))
    assert f'\t(subpath "{deep}")\n' in text
    assert f'\t(literal "{deep.parent}")\n' not in text


def test_a_file_is_a_literal_and_a_symlink_is_not_followed(tmp_path: Path) -> None:
    file = tmp_path / "file"
    file.write_text("x")
    link = tmp_path / "link"
    link.symlink_to(tmp_path)
    text = darwin.profile(_spec(tmp_path, inputs=[str(file), str(link)]))
    assert f'\t(literal "{file}")\n' in text
    assert f'\t(literal "{link}")\n' in text


def test_an_optional_path_that_is_absent_is_left_out(tmp_path: Path) -> None:
    missing = str(tmp_path / "missing")
    spec = _spec(tmp_path)
    spec["paths"].append((missing, missing, True))
    assert missing not in darwin.profile(spec)


def test_a_required_path_that_is_absent_fails_in_nix_words(tmp_path: Path) -> None:
    missing = str(tmp_path / "missing")
    spec = _spec(tmp_path)
    spec["paths"].append((missing, missing, False))
    with pytest.raises(darwin.ProfileError) as caught:
        darwin.profile(spec)
    assert str(caught.value) == f"getting attributes of required path '{magenta(missing)}: No such file or directory"


def test_a_path_mapped_elsewhere_fails_in_nix_words(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    spec["paths"].append(("/bin/sh", str(tmp_path), False))
    with pytest.raises(darwin.ProfileError, match="mismatched impure paths not supported on Darwin"):
        darwin.profile(spec)


def test_a_fixed_output_reaches_the_network_and_logging_shows_violations(tmp_path: Path) -> None:
    text = darwin.profile(_spec(tmp_path, network=True, log_violations=True))
    assert text.startswith("(version 1)\n(deny default)\n")
    assert darwin.NETWORK in text


def test_the_derivations_profile_comes_last(tmp_path: Path) -> None:
    assert darwin.profile(_spec(tmp_path, additional_profile='(allow file-read* (subpath "/x"))')).endswith(
        ')\n(allow file-read* (subpath "/x"))'
    )


def test_many_paths_split_into_several_allow_groups(tmp_path: Path) -> None:
    """NixOS/nix#4119: the interpreter refuses one large group; Nix starts a new one past 16 KiB."""
    inputs = []
    for i in range(400):
        path = tmp_path / f"{i:040d}"
        path.mkdir()
        inputs.append(str(path))
    text = darwin.profile(_spec(tmp_path, inputs=inputs))
    assert text.count("(allow file-read* file-write* process-exec\n") >= 3


def test_an_impure_host_dependency_must_be_allowed(tmp_path: Path) -> None:
    """`getPathsInSandbox` (`derivation-builder.cc:518`): checked with or without the sandbox, and optional."""
    build = str(tmp_path / "build")
    paths = darwin.paths_in_sandbox({}, build, STORE, "/nix/store/x.drv", ["/usr/lib/libSystem.B.dylib"], ["/usr/lib"])
    assert paths == {build: (build, False), "/usr/lib/libSystem.B.dylib": ("/usr/lib/libSystem.B.dylib", True)}
    with pytest.raises(ValueError, match=r"requested impure path .*but it was not in allowed-impure-host-deps"):
        darwin.paths_in_sandbox({}, build, STORE, "/nix/store/x.drv", ["/usr/libexec"], ["/usr/lib"])


def test_a_build_directory_that_holds_the_store_is_refused() -> None:
    with pytest.raises(ValueError, match="must not contain the storeDir"):
        darwin.paths_in_sandbox({}, "/nix", STORE, "/nix/store/x.drv", [], [])


def test_the_parameters_name_the_build_and_the_resolved_temporary_directory(tmp_path: Path) -> None:
    link = tmp_path / "tmp-link"
    link.symlink_to(tmp_path)
    spec = _spec(tmp_path, allow_local_networking=True)
    params = darwin.parameters(spec, {"TMPDIR": f"{link}/"})
    assert params == [
        "_NIX_BUILD_TOP",
        spec["build_top"],
        "_GLOBAL_TMP_DIR",
        str(tmp_path.resolve()),
        "_ALLOW_LOCAL_NETWORKING",
        "1",
    ]


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        ({"__impureHostDeps": "/b\t/a  /b"}, ["/a", "/b"]),
        ({"__json": json.dumps({"__impureHostDeps": ["/b", "/a"]})}, ["/a", "/b"]),
        ({}, None),
    ],
)
def test_impure_host_deps_read_as_a_set(env: dict[str, str], expected: list[str] | None) -> None:
    assert darwin.string_set_attr(env, "__impureHostDeps") == expected
