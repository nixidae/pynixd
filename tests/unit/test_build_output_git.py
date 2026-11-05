"""`pynixd_store.add.git_hash` hashes a tree as `git::dumpHash` does (`libutil/git.cc:314`).

The expected hashes are what `nix hash path --mode git --base16` printed for the same tree under Nix 2.35.3.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pynixd_store.add import git_hash

if TYPE_CHECKING:
    from pathlib import Path

TREE = {
    "sha1": "b3b64aae71b6d7998af471d95a9afcbfada2ea13",
    "sha256": "c0f60b1f7e966d9f1aeea4782a4b34a192d98eec7232c88ab7b1c1a05b3a803e",
}
BLOB = {
    "sha1": "ce013625030ba8dba906f756967f9e9ca394464a",
    "sha256": "2cf8d83d9ee29543b34a87727421fdecb7e3f3a183d337639025de576db9ebb4",
}


def _tree(root: Path) -> Path:
    """`a.txt` sorts before the directory `a`, which Git compares as `a/`."""
    (root / "a" / "deep").mkdir(parents=True)
    (root / "a.txt").write_text("hello\n")
    (root / "a" / "deep" / "f").write_text("x")
    (root / "run").write_text("#!/bin/sh\n")
    (root / "run").chmod(0o755)
    (root / "link").symlink_to("a.txt")
    return root


@pytest.mark.parametrize("algo", ["sha1", "sha256"])
def test_a_tree_hashes_as_nix_hashes_it(tmp_path: Path, algo: str) -> None:
    root = _tree(tmp_path)
    assert git_hash(root, algo) == (0o40000, bytes.fromhex(TREE[algo]))
    assert git_hash(root / "a.txt", algo) == (0o100644, bytes.fromhex(BLOB[algo]))
    assert git_hash(root / "run", algo)[0] == 0o100755
    assert git_hash(root / "link", algo)[0] == 0o120000
