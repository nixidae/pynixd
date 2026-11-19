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

  # Builders launched on demand through vivarium's cloud API, in the Nix
  # sandbox: UML guests need user namespaces and no /dev/kvm. nixidae/nixidae#1198.
  # About 9 minutes of guest time; the UML kernel comes from the cache or is built first.
  jobs.provisioning = {
    runs-on = "ubuntu-latest";
    timeout-minutes = 90;
    ghanix = divertedStores;
    steps = [
      {
        name = "Provision, interrupt, crash and restart builders";
        run = "nix build --file . internal.tests.provisioning --no-link --print-build-logs";
      }
      {
        name = "Boot an EC2 builder with no Nix from its user data";
        run = "nix build --file . internal.tests.provisioning-ec2 --no-link --print-build-logs";
      }
      {
        name = "Run a GitHub Actions job as a builder";
        run = "nix build --file . internal.tests.provisioning-gha --no-link --print-build-logs";
      }
    ];
  };

  # Every test store with no Nix daemon under it (`PYNIXD_NO_DAEMON`,
  # `tests/_conftest/config.py`). The standard set keeps the daemon, so a store
  # with none advertised systems its sandbox could not build, and no lane saw
  # it. nixidae/nixidae#1541.
  jobs.no-daemon = {
    runs-on = "ubuntu-latest";
    timeout-minutes = 60;
    ghanix = divertedStores;
    env.PYNIXD_NO_DAEMON = "1";
    steps = [
      {
        name = "Generate SSH key for ssh-ng:// tests";
        run = ''
          mkdir -p ~/.ssh
          ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N "" -q
        '';
      }
      {
        name = "Functional tests with no daemon";
        run = "nix develop --impure --file shell.nix --command pytest tests/functional";
      }
      {
        name = "Unit tests with no daemon";
        run = "nix develop --impure --file shell.nix --command pytest tests/unit";
      }
    ];
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
