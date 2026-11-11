"""`pynixd_store.builder.env` builds what `initEnv` builds (`unix/build/derivation-builder.cc:692`)."""

from __future__ import annotations

import pytest

from nix_daemon_protocol.store_path import StorePath
from pynixd_store.builder import env as env_module
from pynixd_store.builder.env import (
    build_env,
    dump_float,
    dump_json,
    export_requests,
    hash_placeholder,
    structured_attrs_reason,
    write_shell,
)
from pynixd_store.builder.outputs import OutputChecks, discarded_references, output_checks


def _env(env: dict[str, str], *, fixed_output: bool = False, sandboxed: bool = True) -> dict[str, str]:
    built = build_env(
        env,
        outputs=["out"],
        store_dir="/nix/store",
        build_cores=4,
        rewrites={hash_placeholder("out"): "/nix/store/x-out"},
        fixed_output=fixed_output,
        sandboxed=sandboxed,
        impure_source={"http_proxy": "http://proxy"},
        exported={},
    )
    return built.variables


def test_the_placeholder_is_what_builtins_placeholder_returns() -> None:
    assert hash_placeholder("out") == "/1rz4g4znpzjwh1xymhjpm42vipw92pr73vdgl6xs1hycac8kf2n9"


def test_nix_sets_its_own_variables_over_the_derivations() -> None:
    env = _env({"PWD": "/elsewhere", "out": hash_placeholder("out"), "PATH": "/bin"})
    assert env["PWD"] == env["TMPDIR"] == env["NIX_BUILD_TOP"] == "/build"
    assert env["PATH"] == "/bin"
    assert env["out"] == "/nix/store/x-out"
    assert (env["HOME"], env["NIX_BUILD_CORES"], env["NIX_LOG_FD"]) == ("/homeless-shelter", "4", "2")
    assert "NIX_OUTPUT_CHECKED" not in env


def test_pass_as_file_moves_the_value_into_the_build_directory() -> None:
    built = build_env(
        {"passAsFile": "text", "text": "hello"},
        outputs=["out"],
        store_dir="/nix/store",
        build_cores=1,
        rewrites={},
        fixed_output=False,
        sandboxed=True,
        impure_source={},
        exported={},
    )
    assert "text" not in built.variables
    ((name, contents),) = built.files.items()
    assert name.startswith(".attr-")
    assert built.variables["textPath"] == f"/build/{name}"
    assert contents == "hello"


def test_only_an_unsandboxed_derivation_sees_impure_variables() -> None:
    drv = {"impureEnvVars": "http_proxy ftp_proxy"}
    assert "http_proxy" not in _env(drv)
    fixed = _env(drv, fixed_output=True, sandboxed=False)
    assert (fixed["http_proxy"], fixed["ftp_proxy"], fixed["NIX_OUTPUT_CHECKED"]) == ("http://proxy", "", "1")


def test_an_impure_derivation_sees_impure_variables_but_is_not_checked() -> None:
    """`initEnv` (`derivation-builder.cc:749`, `:761`): `NIX_OUTPUT_CHECKED` needs `isFixed`, the variables `!isSandboxed`."""
    impure = _env({"impureEnvVars": "http_proxy"}, sandboxed=False)
    assert impure["http_proxy"] == "http://proxy"
    assert "NIX_OUTPUT_CHECKED" not in impure


def test_structured_attrs_reach_the_builder_as_two_files_and_nothing_else() -> None:
    """`DesugaredEnv::create` (`derivation-env-desugar.cc:28`): `drv.env` is not exported."""
    raw = '{"b":[1,"x"],"a":{"k":true},"dest":"' + hash_placeholder("out") + '/foo"}'
    built = build_env(
        {"__json": raw, "ignored": "1"},
        outputs=["out", "dev"],
        store_dir="/nix/store",
        build_cores=1,
        rewrites={hash_placeholder("out"): "/nix/store/x-out"},
        fixed_output=False,
        sandboxed=True,
        impure_source={},
        exported={},
    )
    assert "ignored" not in built.variables
    assert built.variables["NIX_ATTRS_JSON_FILE"] == "/build/.attrs.json"
    assert built.variables["NIX_ATTRS_SH_FILE"] == "/build/.attrs.sh"
    dev = hash_placeholder("dev")
    assert built.files[".attrs.json"] == (
        '{"a":{"k":true},"b":[1,"x"],"dest":"/nix/store/x-out/foo",'
        f'"outputs":{{"dev":"{dev}","out":"/nix/store/x-out"}}}}'
    )
    assert built.files[".attrs.sh"] == (
        "declare -A a=(['k']=1 )\n"
        "declare -a b=(1 'x' )\n"
        "declare dest='/nix/store/x-out/foo'\n"
        f"declare -A outputs=(['dev']='{dev}' ['out']='/nix/store/x-out' )\n"
    )


def test_write_shell_declares_what_bash_can_hold() -> None:
    """The cases of Nix's `tests/functional/structured-attrs.nix`; its float is not declared (`:28`)."""
    attrs = {
        "int": 123456789,
        "float": 123.456,
        "boolTrue": True,
        "boolFalse": False,
        "nothing": None,
        "hardening": {"format": True, "fortify": False},
        "buildInputs": ["a", "b", "c", 123, "'", '"', None],
        "outer": {"inner": [1, 2, 3]},
        "foo bar": "BAD",
        "1foobar": "BAD",
        "foo$": "BAD",
        "big": 2**40 + 7,
    }
    assert write_shell(attrs) == (
        "declare big=7\n"
        "declare boolFalse=\n"
        "declare boolTrue=1\n"
        "declare -a buildInputs=('a' 'b' 'c' 123 ''\\''' '\"' '' )\n"
        "declare -A hardening=(['format']=1 ['fortify']= )\n"
        "declare int=123456789\n"
        "declare nothing=''\n"
    )


def test_a_structured_build_pynixd_cannot_reproduce_goes_to_the_daemon() -> None:
    def reason(raw: str, **env: str) -> str | None:
        return structured_attrs_reason({"__json": raw, **env})

    assert reason('{"a":1,"s":"x","l":[null,true]}') is None
    assert reason('{"float":123.456}') is None
    assert reason('{"float":3e9}') is None
    assert reason('{"big":18446744073709551616}') is not None
    assert reason('{"bad":"\\ud800"}') is not None
    assert reason("[1]") is not None
    assert reason("{") is not None
    assert reason('{"allowedReferences":[]}') is not None
    assert reason("{}", passAsFile="x") is not None
    assert reason('{"__noChroot":1}') is not None
    assert reason('{"requiredSystemFeatures":["kvm",1]}') is not None


A = "/nix/store/" + "a" * 32 + "-a"
B = "/nix/store/" + "b" * 32 + "-b"


def test_export_references_graph_names_store_paths_by_file_name() -> None:
    """`DerivationOptions::exportReferencesGraph` (`derivation-options.cc:310`), both forms."""
    a, b = StorePath(A), StorePath(B)
    assert export_requests({"exportReferencesGraph": f"z {A}\t\na {B}"}, "/nix/store") == {"a": [b], "z": [a]}
    raw = f'{{"exportReferencesGraph":{{"g":["{B}",["{A}"]]}}}}'
    assert export_requests({"__json": raw}, "/nix/store") == {"g": [a, b]}
    assert export_requests({}, "/nix/store") == {}


@pytest.mark.parametrize(
    "env",
    [
        {"exportReferencesGraph": f"a {A} b"},
        {"exportReferencesGraph": f".a {A}"},
        {"exportReferencesGraph": f"a {A}/bin"},
        {"exportReferencesGraph": f"a {A}^out"},
        {"exportReferencesGraph": f"a\x0b{A}"},
        {"__json": '{"exportReferencesGraph":["x"]}'},
        {"__json": '{"exportReferencesGraph":{"g":[1]}}'},
    ],
)
def test_an_export_nix_refuses_or_warns_about_goes_to_the_daemon(env: dict[str, str]) -> None:
    with pytest.raises(ValueError, match=r"."):
        export_requests(env, "/nix/store")


def test_structured_output_checks_are_per_output_and_keep_self_references() -> None:
    """`DerivationOptions::outputChecks` (`derivation-options.cc:214`)."""
    raw = '{"outputChecks":{"out":{"maxSize":5,"allowedReferences":["out"]}},"unsafeDiscardReferences":{"out":true,"dev":false}}'
    assert output_checks({"__json": raw}) == {"out": OutputChecks(max_size=5, allowed_references=frozenset({"out"}))}
    assert discarded_references({"__json": raw}) == {"out"}
    assert output_checks({"disallowedReferences": "a b"}) == OutputChecks(
        ignore_self_refs=True, disallowed_references=frozenset({"a", "b"})
    )
    assert discarded_references({"unsafeDiscardReferences": "1"}) == set()


@pytest.mark.parametrize(
    "raw",
    [
        '{"outputChecks":[]}',
        '{"outputChecks":{"out":[]}}',
        '{"outputChecks":{"out":{"maxSize":-1}}}',
        '{"outputChecks":{"out":{"maxSize":true}}}',
        '{"outputChecks":{"out":{"allowedReferences":"x"}}}',
        '{"unsafeDiscardReferences":{"out":1}}',
    ],
)
def test_structured_checks_nix_refuses_go_to_the_daemon(raw: str) -> None:
    def both(env: dict[str, str]) -> None:
        output_checks(env)
        discarded_references(env)

    with pytest.raises(ValueError, match=r"."):
        both({"__json": raw})


# What `builtins.toJSON (builtins.fromJSON ...)` printed under Nix 2.35.3: nlohmann's Grisu2 and `format_buffer`.
NLOHMANN_FLOATS = {
    1e15: "1e+15",
    1e16: "1e+16",
    1e-5: "1e-05",
    1e-4: "0.0001",
    0.1: "0.1",
    1.5: "1.5",
    123456789012345.0: "123456789012345.0",
    1234567890123456.0: "1.234567890123456e+15",
    -0.0: "-0.0",
    5e-324: "5e-324",
    1.7976931348623157e308: "1.7976931348623157e+308",
    7e22: "7.0000000000000004e+22",
    -476.4663769236316: "-476.46637692363163",
    1626983080611305.8: "1.6269830806113057e+15",
}


@pytest.mark.parametrize(("value", "printed"), NLOHMANN_FLOATS.items())
def test_a_float_dumps_as_nlohmann_dumps_it(value: float, printed: str) -> None:
    """Grisu2 is not always shortest: 259 of 200,012 random doubles differ from `repr`, and none from this port."""
    assert dump_float(value) == printed


def test_dump_json_sorts_keys_and_keeps_floats_nlohmann_shaped() -> None:
    assert dump_json({"b": [1e15, 2.0], "a": {"x": None, "y": "é"}}) == '{"a":{"x":null,"y":"é"},"b":[1e+15,2.0]}'


def test_a_float_reaches_the_shell_only_when_its_single_precision_value_is_whole() -> None:
    """`handleSimpleType` (`parsed-derivations.cc:131`): `get<float>()`, then `get<int>()` of the double."""
    shell = write_shell({"whole": 2.0, "frac": 2.5, "near": 0.99999999, "single": 16777217.5, "list": [1.5]})
    assert shell == "declare near=0\ndeclare single=16777217\ndeclare whole=2\n"


@pytest.mark.parametrize(
    ("number", "x86", "saturated"),
    [
        (2147483647.0, "2147483647", "2147483647"),
        (2147483648.0, "-2147483648", "2147483647"),
        (3e9, "-2147483648", "2147483647"),
        (-3e9, "-2147483648", "-2147483648"),
        (1e15, "-2147483648", "2147483647"),
        (1e39, "-2147483648", "2147483647"),
        (-1e300, "-2147483648", "-2147483648"),
    ],
)
def test_a_whole_float_out_of_int_range_declares_what_the_machine_casts(
    monkeypatch: pytest.MonkeyPatch, number: float, x86: str, saturated: str
) -> None:
    """`get<int>()` of a double out of `int` range is undefined in C++ (Lillecarl/nix#418). Nix 2.35.3 on
    x86_64 wrote the x86 column; `1e39` overflows `get<float>()` to an infinity, which `ceil` keeps."""
    assert structured_attrs_reason({"__json": f'{{"x":{number!r}}}'}) is None
    monkeypatch.setattr(env_module, "_X86", True)
    assert write_shell({"x": number, "l": [number]}) == f"declare -a l=({x86} )\ndeclare x={x86}\n"
    monkeypatch.setattr(env_module, "_X86", False)
    assert write_shell({"x": number}) == f"declare x={saturated}\n"
