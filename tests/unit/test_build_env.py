"""`pynixd_store.builder.env` builds what `initEnv` builds (`unix/build/derivation-builder.cc:692`)."""

from __future__ import annotations

from pynixd_store.builder.env import build_env, hash_placeholder, structured_attrs_reason, write_shell


def _env(env: dict[str, str], *, fixed_output: bool = False) -> dict[str, str]:
    built = build_env(
        env,
        outputs=["out"],
        store_dir="/nix/store",
        build_cores=4,
        rewrites={hash_placeholder("out"): "/nix/store/x-out"},
        fixed_output=fixed_output,
        impure_source={"http_proxy": "http://proxy"},
    )
    assert built is not None
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
        impure_source={},
    )
    assert built is not None
    assert "text" not in built.variables
    ((name, contents),) = built.files.items()
    assert name.startswith(".attr-")
    assert built.variables["textPath"] == f"/build/{name}"
    assert contents == "hello"


def test_only_a_fixed_output_derivation_sees_impure_variables() -> None:
    drv = {"impureEnvVars": "http_proxy ftp_proxy"}
    assert "http_proxy" not in _env(drv)
    fixed = _env(drv, fixed_output=True)
    assert (fixed["http_proxy"], fixed["ftp_proxy"], fixed["NIX_OUTPUT_CHECKED"]) == ("http://proxy", "", "1")


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
        impure_source={},
    )
    assert built is not None
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
    """The cases of Nix's `tests/functional/structured-attrs.nix`, without its float."""
    attrs = {
        "int": 123456789,
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
    assert reason('{"float":123.456}') is not None
    assert reason('{"big":18446744073709551616}') is not None
    assert reason('{"bad":"\\ud800"}') is not None
    assert reason("[1]") is not None
    assert reason("{") is not None
    assert reason('{"exportReferencesGraph":{}}') is not None
    assert reason('{"allowedReferences":[]}') is not None
    assert reason("{}", passAsFile="x") is not None
    assert reason('{"__noChroot":1}') is not None
    assert reason('{"requiredSystemFeatures":["kvm",1]}') is not None
