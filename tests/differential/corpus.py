"""The derivations that the differential suite builds twice.

Each case is one shape that a goal system has to get right. The value of the
suite is in this file: two engines agree trivially on a corpus that asks them
nothing.

Every derivation here is self-contained. It names no channel and no nixpkgs,
it builds with `/bin/sh`, and its script uses shell builtins only -- `echo` and
redirection. The Nix sandbox supplies `/bin/sh` and nothing else, so a script
that calls `cat` or `cp` fails for a reason that has no bearing on either goal
system.

A dependency is expressed by interpolating the dependency into the script.
That does two things at once, and both are wanted: it makes the dependency an
input derivation, so it must be built first, and it writes the path into the
output, so the output refers to it. The comparison reads both.
"""

from __future__ import annotations

import dataclasses

# The prelude every case shares. `mk` keeps each case to the shape that the
# case is about, rather than to six lines of `derivation` boilerplate.
_PRELUDE = """
let
  system = builtins.currentSystem;
  mk = name: script: derivation {
    inherit name system;
    builder = "/bin/sh";
    args = [ "-c" script ];
  };
in
"""


@dataclasses.dataclass(frozen=True, slots=True)
class Case:
    """One derivation, and what it asks of a goal system."""

    name: str
    """The identifier, which is also the pytest parameter id."""

    body: str
    """A Nix expression, evaluated after the shared prelude."""

    probes: str
    """What this case is for. It appears in the failure message."""

    expect_success: bool = True
    """Whether the build is meant to succeed. A failing build is a real case:
    the two engines must agree on the failure as well as on the success."""

    experimental_features: tuple[str, ...] = ()
    """Experimental features the case needs. A case that names one is skipped
    where the linked Nix does not have it."""

    @property
    def expression(self) -> str:
        """The whole expression, prelude included."""
        return _PRELUDE + self.body


CORPUS: tuple[Case, ...] = (
    Case(
        name="single",
        body='mk "diff-single" "echo body > $out"',
        probes="one derivation, no inputs -- the floor of the whole suite",
    ),
    Case(
        name="chain",
        body="""
        let
          base = mk "diff-chain-base" "echo base > $out";
        in mk "diff-chain-top" "echo ${base} > $out"
        """,
        probes="a two-level chain: an input derivation must be realised first",
    ),
    Case(
        name="diamond",
        body="""
        let
          base = mk "diff-diamond-base" "echo base > $out";
          left = mk "diff-diamond-left" "echo ${base} > $out";
          right = mk "diff-diamond-right" "echo ${base} > $out";
        in mk "diff-diamond-top" "echo ${left} ${right} > $out"
        """,
        probes=(
            "a shared input reached by two branches. The engine must build "
            "`base` once. Both engines report success either way, so the "
            "store is what states the answer."
        ),
    ),
    Case(
        name="multi-output",
        body="""
        derivation {
          name = "diff-multi-output";
          system = builtins.currentSystem;
          builder = "/bin/sh";
          outputs = [ "out" "dev" ];
          args = [ "-c" "echo body > $out; echo $out > $dev" ];
        }
        """,
        probes=(
            "two outputs, and `dev` refers to `out`. Reference scanning "
            "between the outputs of one derivation is a separate code path "
            "from scanning against the inputs."
        ),
    ),
    Case(
        name="failing",
        body='mk "diff-failing" "echo partial > $out; exit 1"',
        probes=(
            "a build that fails after it writes its output. Neither store may "
            "hold the output, and both engines must say so the same way."
        ),
        expect_success=False,
    ),
    Case(
        name="failing-dependency",
        body="""
        let
          broken = mk "diff-dep-broken" "exit 1";
        in mk "diff-dep-top" "echo ${broken} > $out"
        """,
        probes=("a failure one level down. The parent must not build, and neither store may hold either output."),
        expect_success=False,
    ),
    Case(
        name="binary-log",
        # Four backslashes in this file are two in the expression (Python),
        # which are one on the shell (Nix `"..."`), which the shell reads as
        # octal. The bytes never reach the store -- only `$out` does -- so
        # this case asks the engines to survive them, not to agree on them.
        body='mk "diff-binary-log" "printf \'\\\\037\\\\213\\\\010not-utf8\\\\n\' >&2; echo done > $out"',
        probes=(
            "a builder that prints bytes no UTF-8 decoder accepts. Both "
            "engines must build it and register the output; before the "
            "lossless log codec, pynixd failed the build and held nothing. "
            "Issue nixidae/nixidae#662."
        ),
    ),
)


CA_CORPUS: tuple[Case, ...] = (
    Case(
        name="floating-ca",
        body="""
        derivation {
          name = "diff-floating-ca";
          system = builtins.currentSystem;
          builder = "/bin/sh";
          args = [ "-c" "echo body > $out" ];
          __contentAddressed = true;
          outputHashMode = "recursive";
          outputHashAlgo = "sha256";
        }
        """,
        probes=(
            "a floating content-addressed output. The final path is not known "
            "before the build, so each engine has to resolve it and register a "
            "realisation. This is the case the goal systems are most likely to "
            "disagree on."
        ),
        experimental_features=("ca-derivations",),
    ),
)
"""Kept apart from `CORPUS` because these need an experimental feature.

They are the cases most worth having and the ones most likely to be skipped,
so the split makes a skipped run visible rather than quiet.
"""
