# The test workflow, as a value rather than as text.
#
# nixidae renders it to `.github/workflows/pynixd-tests.yml`, with pynixd as
# the working directory of every step. Tests run only in nixidae; pynixd's
# own repository runs ci/workflows/pages.nix. nixidae/nixidae#4.
#
# ghanix is the schema. It takes `lib` and nothing else, which is what lets
# this repository use it while pinning its own nixpkgs.
#
{
  lib,
  ghalib,
  nixVersion,
}:
let
  inherit (import ./bootstrap.nix { inherit lib nixVersion; }) divertedStores;

  # A gate, as one step. Each one names what broke without a reader opening a
  # log, and each pytest suite is its own process.
  #
  # **`tests/unit` and `nix-daemon-protocol/tests` interfere.** Four of the
  # protocol tests pass alone and fail beside the pynixd suites. One process
  # for both is how 57 failures hid behind a green run of 684, and one of
  # them was a shipped regression. Issue #33.
  pytestSuite = name: paths: {
    inherit name;
    run = "nix develop --impure --file shell.nix --command pytest ${lib.concatStringsSep " " paths}";
  };
in
ghalib.evalWorkflow {
  name = "tests";

  on = {
    push.branches = [ "**" ];
    pull_request = null;
  };

  jobs = {
    /*
      Nix's own functional suite, against a plain daemon and against pynixd.

      **The verdict is the regression count, and not the failure count.** The
      runner builds the suite twice, once against `nix daemon` and once
      against pynixd, and `compare.py` names the tests that pynixd alone
      fails. A test that fails in both arms is a defect of Nix or of the
      harness, and it must not fail this job: `main:db-migration` is one, it
      wants an older Nix, and issue #45 holds it.

      `nanopynix-nixft-nix_2_34 all` already ends in that comparison and exits
      non-zero only on a regression, so the job is the program and its exit
      code.

      Measured at pynixd fc88ad6c on a 16-core machine: 3m25s at `JOBS=4` for
      both arms, 170 OK, 36 SKIP, 1 FAIL, 0 regressions. On a runner, in run
      35200463822: **5m07s for the whole job** -- 26 s to build the suite and
      4m30s for both arms. So the cap is a backstop against a hang and not a
      bound on the work, which is what a cap is for.

      `freeDiskSpace` is not needed for room here: the whole work directory
      came to 228 MB, of which 144 MB is the stores of the 205 tests, against
      the 14 GB a runner starts with. ghanix runs it anyway, because a job
      that builds Nix derivations never wants the runner's bundled
      toolchains.

      **`NIXFT_WORK` must be short, and outside `$HOME`.** A store under a
      long path gives a daemon socket over `sun_path`'s 108 bytes, and the
      failure then blames the daemon rather than the path. `$HOME` is mode
      700, and a sandboxed build runs as `nixbld1`, which cannot traverse it:
      seven tests that a plain daemon passes fail there, and a broken control
      arm hides a regression rather than reporting one.

      Issues #11 and #19.
    */
    nix-functional-tests = {
      env = {
        NIXFT_WORK = "/tmp/nixft";
        JOBS = "4";
      };
      runs-on = "ubuntu-latest";
      timeout-minutes = 90;
      ghanix = divertedStores;
      steps = [
        {
          name = "Build the suite for nix 2.34";
          run = "nix build --file . nixFunctionalTests.nix_2_34 --out-link nixft --print-build-logs";
        }
        {
          name = "Both arms, and the comparison";
          run = "./nixft/bin/nanopynix-nixft-nix_2_34 all";
        }
        # The meson log holds the whole output of every test. The step output
        # holds the tail of the failed ones, which is not enough to tell a
        # defect of pynixd from a defect of the harness -- and the harness is
        # the more common answer of the two.
        {
          name = "Keep the test log of a red run";
          "if" = "failure()";
          uses = "actions/upload-artifact@v4";
          "with" = {
            name = "nixft-logs";
            path = "/tmp/nixft/build/meson-logs";
            retention-days = 7;
            if-no-files-found = "warn";
          };
        }
      ];
    };

    test = {
      runs-on = "ubuntu-latest";
      # **A hang here used to cost six hours, and the log came back empty.**
      # pytest printed `149 errors in 11.91s` and then did not exit, and the
      # runner killed the job at its own six-hour limit. Twenty-two runs
      # queued behind that. The whole job takes under fifteen minutes when it
      # works, so this is far above a good run and far below the limit that
      # hurt. Issue #47.
      timeout-minutes = 45;
      ghanix = divertedStores;
      steps = [
        # **The settings the suite builds under, recorded on every run.**
        # `sandbox`, `sandbox-shell`, `build-users-group` and `system` decide
        # what a builder sees, and a runner sets them differently from a
        # developer machine. `|| true`, because a diagnostic must never be
        # the thing that fails a run.
        {
          name = "Record the Nix settings of this runner";
          run = "nix config show || true";
        }
        {
          name = "Generate SSH key for ssh-ng:// tests";
          run = ''
            mkdir -p ~/.ssh
            ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N "" -q
          '';
        }
        {
          name = "Format, lint and types";
          run = "nix build --file . checks.format checks.lint checks.types --no-link --print-build-logs";
        }
        (pytestSuite "Tests" [
          "tests/functional"
          "tests/unit"
        ])
        (pytestSuite "Protocol tests" [ "nix-daemon-protocol/tests" ])
        # The differential suite: pynixd's goal engine against Nix's own,
        # which nanopynix calls in process. It ran nowhere until nixidae
        # supplied nanopynix to the shell, and `tests/differential/conftest.py`
        # skips the lot rather than failing where the oracle is absent -- so a
        # green run here is not proof on its own that it ran. 16 tests, 47 s.
        (pytestSuite "Differential tests" [ "tests/differential" ])
        # The parity suite: the same workload against `nix daemon` and against
        # pynixd, byte for byte. `AGENTS.md` calls this the measure of
        # pynixd's contract, and its own docstring lists six defects it found
        # -- and no wired path ran it. Issue #33 is the same hole one suite
        # over. 8 tests, 22 s.
        (pytestSuite "Parity tests" [ "tests/parity" ])
        # After the suites: a drift in the Pages render must not hide them.
        # Issue #49.
        {
          name = "The workflow render";
          run = "nix build --file . checks.ciWorkflow --no-link --print-build-logs";
        }
        # **The pytest output alone does not say why a suite went red here.**
        # Each test writes `filtered.log`, `unfiltered.log` and
        # `exceptions.jsonl` under this directory, and those hold the probe
        # results, the daemon's own messages and the store each test used.
        # The step output holds the assertion and nothing else.
        #
        # This repository has already published one wrong cause for a CI
        # failure by reading the summary and not the per-test logs -- issue
        # #37. The logs were beside it and were not uploaded.
        {
          name = "Keep the per-test logs of a red run";
          "if" = "failure()";
          uses = "actions/upload-artifact@v4";
          "with" = {
            name = "pynixd-logs";
            path = "/tmp/pynixd-logs";
            retention-days = 7;
            if-no-files-found = "warn";
          };
        }
      ];
    };
  };
}
