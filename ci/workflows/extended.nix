# The extended set on a schedule (nixidae/nixidae#1247): each suite that
# `tests/suites.toml` flags, with `-m extended`. A push runs the standard set
# (`tests.nix`). nixidae renders this to `.github/workflows/pynixd-extended.yml`.
#
# Host pytest, as `tests.nix` runs it. `root` needs a root user and runs in the
# guest driver only (`internal.tests.guest-extended`). A suite's `lanes` run it
# again in that lane's shell (`internal.laneShells`, nixidae/nixidae#1249).
{
  lib,
  ghalib,
  nixVersion,
}:
let
  inherit (import ./bootstrap.nix { inherit lib nixVersion; }) divertedStores;

  suites = lib.filterAttrs (
    _: suite: (suite.extended or false) && (suite.user or "tester") != "root"
  ) (builtins.fromTOML (builtins.readFile ../../tests/suites.toml));
in
ghalib.evalWorkflow {
  name = "extended";

  on = {
    schedule = [ { cron = "41 2 * * *"; } ];
    workflow_dispatch = null;
  };

  jobs.extended = {
    runs-on = "ubuntu-latest";
    timeout-minutes = 120;
    ghanix = divertedStores;
    steps = [
      {
        name = "Generate SSH key for ssh-ng:// tests";
        run = ''
          mkdir -p ~/.ssh
          ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N "" -q
        '';
      }
    ]
    ++ lib.concatLists (
      lib.mapAttrsToList (
        name: suite:
        [
          {
            name = "Extended ${name} tests";
            run = "nix develop --impure --file shell.nix --command pytest ${suite.path} -m extended";
          }
        ]
        ++ map (lane: {
          name = "Extended ${name} tests on ${lane}";
          run = "nix develop --impure --file . internal.laneShells.${lane} --command pytest ${suite.path} -m extended";
        }) (suite.lanes or [ ])
      ) suites
    );
  };
}
