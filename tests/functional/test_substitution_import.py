"""pynixd's own imports follow AddToStoreNar's route: a substitution, and the HTTP cache's upload.

Store A's closure goes into a binary cache, and pynixd substitutes it into
store B on the `pynixd` route and store C on the daemon route. Rows and files
must agree, and only the `pynixd` route imports in pynixd.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from pynixd.store.add import import_nar
from tests._conftest.import_lab import REFS, VALID_PATHS, files, lab, rows, run, serve_directory, server
from tests.conftest import NIX_BIN

if TYPE_CHECKING:
    from pathlib import Path

    from nix_daemon_protocol.valid_path_info import ValidPathInfo
    from pynixd.store import LocalDBStore


@pytest.fixture
def imported(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The paths the substitution queue imported in pynixd."""
    seen: list[str] = []

    async def counting(store: LocalDBStore, info: ValidPathInfo, *args: Any, **kwargs: Any) -> None:
        seen.append(str(info.path))
        await import_nar(store, info, *args, **kwargs)

    monkeypatch.setattr("pynixd.substitution_queue.import_nar", counting)
    return seen


async def test_a_substitution_imports_as_the_daemon_does(tmp_path: Path, imported: list[str]) -> None:
    a, order, _ = await lab(tmp_path)
    nix = str(NIX_BIN)
    cache = tmp_path / "cache"
    await run(nix, "copy", "--no-check-sigs", "--from", str(a), "--to", f"file://{cache}", *order)
    wanted = [p for p in order if not p.endswith(".drv")]
    b, c = tmp_path / "b", tmp_path / "c"
    async with serve_directory(cache) as url:
        for root, route in ((b, "pynixd"), (c, "daemon")):
            socket = tmp_path / f"{route}.sock"
            async with server(root, socket, route, upstream=url):
                await run(nix, "build", "--no-link", "--store", f"unix://{socket}?root={root}", *wanted)
            if route == "pynixd":
                assert imported, "pynixd imported the substitutes"
                assert set(imported) <= set(wanted)
                imported.clear()
    assert imported == []

    for query in (VALID_PATHS, REFS):
        assert rows(b, query) == rows(c, query)
    assert {p for (p, *_) in rows(b, VALID_PATHS)} >= set(wanted)
    for path, *_ in rows(b, VALID_PATHS):
        assert files(b, path) == files(c, path), path
    assert not list((b / "nix" / "store").glob("tmp-*"))
    await run(nix, "store", "verify", "--store", str(b), "--no-trust", *wanted)
