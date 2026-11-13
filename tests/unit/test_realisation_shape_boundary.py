"""Op 42/43 carry one shape across the hop that negotiates apart.

The client decodes a `QueryRealisation` or `RegisterDrvOutput` under its own
feature set, and the pooled connection encodes it under the store's. The two
shapes of a realisation reference share no byte, so a request that holds only
the shape the store did not agree to must gain the other one before the
forward -- or the forward must refuse. `register_drv_output` and
`query_realisation` of `store/daemon.py` do that work. Issue #84.

No line of the functional suite covers this: one daemon serves every script
there, so client and backend never negotiate apart. These tests stand alone.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

import nix_daemon_protocol as ndp
from nix_daemon_protocol.store_dir import store_prefix
from pynixd.exceptions import BackendError
from pynixd.store.daemon import DaemonStore

FEATURE = ndp.FEATURE_REALISATION_WITH_PATH
DIGEST = "f" * 64
OUT_BARE = "bbbb-out"


def _drv_path() -> str:
    return store_prefix() + "aaaa-local.drv"


def _out_path() -> str:
    return store_prefix() + OUT_BARE


def _keyed() -> Any:
    return ndp.KeyedDrvOutput(drv_path=ndp.StorePath(_drv_path()), output_name="out")


def _unkeyed() -> Any:
    return ndp.UnkeyedRealisation(out_path=ndp.StorePath(_out_path()), signatures={ndp.Signature("sig")})


def _old_id() -> Any:
    return ndp.DrvOutput(f"sha256:{DIGEST}!out")


def _store(monkeypatch: pytest.MonkeyPatch, features: set[str]) -> Any:
    """A store reduced to what the shape boundary reads, recording the forward."""
    forwarded: list[Any] = []

    async def _call(request: Any, **kwargs: Any) -> Any:
        forwarded.append(request)
        return request

    async def _read_derivation(path: str) -> Any:
        return object()

    async def _fake_hashes(parsed: Any, read: Any) -> dict[str, str]:
        return {"out": DIGEST}

    monkeypatch.setattr("pynixd.store.scheduled.output_hashes", _fake_hashes)
    store = SimpleNamespace(
        store_id="local",
        features=features,
        read_derivation=_read_derivation,
        call=_call,
        forwarded=forwarded,
    )
    # The real boundary logic, bound to the fake: `query_realisation` reaches
    # the helpers through `self`, and the fake holds no methods of its own.
    for name in (
        "request_in_the_shape_this_store_reads",
        "_old_drv_output_id",
        "_old_realisation",
    ):
        setattr(store, name, getattr(DaemonStore, name).__get__(store))
    return store


async def test_new_shape_query_to_old_store_gains_the_old_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """The hash comes from the derivation the store holds."""
    store = _store(monkeypatch, set())
    request = ndp.QueryRealisationRequest(drv_output=None, keyed_drv_output=_keyed())

    await DaemonStore.query_realisation(store, request)

    (forwarded,) = store.forwarded
    assert forwarded.drv_output == _old_id()
    assert forwarded.keyed_drv_output == _keyed()


async def test_new_shape_register_to_old_store_gains_the_whole_realisation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The old JSON shape carries the bare name, not the whole path."""
    store = _store(monkeypatch, set())
    request = ndp.RegisterDrvOutputRequest(realisation=None, keyed_drv_output=_keyed(), unkeyed_realisation=_unkeyed())

    await DaemonStore.register_drv_output(store, request)

    (forwarded,) = store.forwarded
    assert forwarded.realisation.id == _old_id()
    # `StorePath` holds the base name and `str` puts the directory back, so
    # equality -- and not the string -- says which form the value carries.
    assert forwarded.realisation.out_path == ndp.StorePath(OUT_BARE)
    assert sorted(forwarded.realisation.signatures) == ["sig"]


async def test_old_shape_query_to_new_store_refuses(monkeypatch: pytest.MonkeyPatch) -> None:
    """The path cannot be recovered from the hash; no index maps one to the other."""
    store = _store(monkeypatch, {FEATURE})
    request = ndp.QueryRealisationRequest(drv_output=_old_id(), keyed_drv_output=None)

    with pytest.raises(BackendError, match="cannot be recovered"):
        await DaemonStore.query_realisation(store, request)
    assert store.forwarded == []


async def test_uncomputable_hash_refuses(monkeypatch: pytest.MonkeyPatch) -> None:
    """A zero id would query a realisation that was never registered."""
    store = _store(monkeypatch, set())
    request = ndp.QueryRealisationRequest(drv_output=None, keyed_drv_output=_keyed())

    async def _no_hashes(parsed: Any, read: Any) -> None:
        return None

    monkeypatch.setattr("pynixd.store.scheduled.output_hashes", _no_hashes)
    with pytest.raises(BackendError, match="not computable"):
        await DaemonStore.query_realisation(store, request)
    assert store.forwarded == []


async def test_half_new_shape_register_refuses(monkeypatch: pytest.MonkeyPatch) -> None:
    """The key without the body is half a value, and the old shape needs the whole."""
    store = _store(monkeypatch, set())
    request = ndp.RegisterDrvOutputRequest(realisation=None, keyed_drv_output=_keyed(), unkeyed_realisation=None)

    with pytest.raises(BackendError, match="half of the new shape"):
        await DaemonStore.register_drv_output(store, request)
    assert store.forwarded == []


async def test_both_shapes_pass_through_untouched(monkeypatch: pytest.MonkeyPatch) -> None:
    """pynixd's own queries fill each field; the codec drops the one not agreed to."""
    for features in (set(), {FEATURE}):
        store = _store(monkeypatch, features)
        request = ndp.QueryRealisationRequest(drv_output=_old_id(), keyed_drv_output=_keyed())

        await DaemonStore.query_realisation(store, request)

        (forwarded,) = store.forwarded
        assert forwarded is request


async def test_matching_shapes_pass_through_untouched(monkeypatch: pytest.MonkeyPatch) -> None:
    """No work when the request already speaks the shape the store reads."""
    store = _store(monkeypatch, set())
    request = ndp.QueryRealisationRequest(drv_output=_old_id(), keyed_drv_output=None)

    await DaemonStore.query_realisation(store, request)

    (forwarded,) = store.forwarded
    assert forwarded is request
