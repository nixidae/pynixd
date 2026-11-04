"""`pynixd_store.builder.env` builds what `initEnv` builds (`unix/build/derivation-builder.cc:692`)."""

from __future__ import annotations

from pynixd_store.builder.env import build_env, hash_placeholder


def _env(env: dict[str, str], *, fixed_output: bool = False) -> dict[str, str]:
    built = build_env(
        env,
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


def test_structured_attrs_are_left_to_the_daemon() -> None:
    assert (
        build_env(
            {"__json": "{}"}, store_dir="/nix/store", build_cores=1, rewrites={}, fixed_output=False, impure_source={}
        )
        is None
    )
