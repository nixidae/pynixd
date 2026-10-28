"""Roundtrip test: WireModel ↔ existing dataclass."""

from __future__ import annotations

from pynixd.serde import (
    BuildResult,
    OptMicroseconds,
    Realisation,
    StorePath,
    WireModel,
)


async def test_wire_build_result_json_roundtrip() -> None:
    """BuildResult JSON roundtrip (exercises wire_conditional + dict + primitives)."""
    br = BuildResult(
        status=0,
        error_msg="",
        times_built=1,
        is_non_deterministic=0,
        start_time=1000000,
        stop_time=1000500,
        built_outputs={"sha256:abc!out": Realisation(out_path=StorePath(path="/nix/store/xxx-foo"))},
    )
    br.cpu_user = OptMicroseconds(tag=1, value=50000)
    br.cpu_system = OptMicroseconds(tag=0, value=None)

    # to_json
    data = br.to_json()
    assert '"status":0' in data
    assert '"error_msg":""' in data
    assert '"times_built":1' in data
    assert '"start_time":1000000' in data
    assert '"built_outputs":' in data
    assert '"cpu_user":{"tag":1,"value":50000}' in data
    assert '"cpu_system":{"tag":0,"value":null}' in data

    # from_json
    br2 = BuildResult.from_json(data)
    assert isinstance(br2, BuildResult)
    assert br2.status == 0
    assert br2.times_built == 1
    assert br2.start_time == 1000000
    assert br2.built_outputs == {"sha256:abc!out": Realisation(out_path=StorePath(path="/nix/store/xxx-foo"))}
    assert br2.cpu_user.tag == 1
    assert br2.cpu_user.value == 50000
    assert br2.cpu_system.tag == 0


async def test_wire_store_path_json() -> None:
    """A store path travels as the base name in JSON, and whole on the wire.

    `adl_serializer<nix::StorePath>::to_json`, `src/libstore/path.cc:95` of
    Nix, writes `storePath.to_string()`, which is the base name, and
    `from_json` builds one straight back from it. That is what a
    `Realisation` carries, and `Realisation` is the only model here that
    serializes itself as JSON on the wire.

    **Nix has a second JSON form, and it is chosen by the codec rather than
    by the value.** `ValidPathInfo::toJSON`, `src/libstore/path-info.cc:197`,
    writes `store->printStorePath(ref)` under `PathInfoJsonFormat::V1` and
    the base name otherwise. So a surface that needs the whole path asks for
    it; the default matches Nix's default. Issue #4.

    This test asserted the whole path before, which no measurement of Nix
    supported.
    """
    sp = StorePath(path="/nix/store/abc-test")

    class Req(WireModel):
        path: StorePath

    req = Req(path=sp)

    assert req.to_json() == '{"path":"abc-test"}'
    assert sp.to_wire() == "/nix/store/abc-test"

    # Reading takes either form, because the constructor does.
    for data in ('{"path":"abc-test"}', '{"path":"/nix/store/abc-test"}'):
        back = Req.from_json(data)
        assert isinstance(back.path, StorePath)
        assert back.path == sp
        assert str(back.path) == "/nix/store/abc-test"


async def test_wire_build_result_json_null_conditional() -> None:
    """OptMicroseconds not present → null in JSON."""
    br = BuildResult(status=0, error_msg="")
    # cpu_user is OptMicroseconds(tag=0) by default
    assert br.cpu_user.tag == 0

    json_str = br.to_json()
    assert '"cpu_user":{"tag":0,"value":null}' in json_str
    assert '"cpu_system":{"tag":0,"value":null}' in json_str

    wm = BuildResult.from_json(json_str)
    assert wm.cpu_user.tag == 0  # pyright: ignore[reportAttributeAccessIssue]
    assert wm.cpu_system.tag == 0  # pyright: ignore[reportAttributeAccessIssue]
