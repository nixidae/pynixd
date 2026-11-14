# pynixd's tarball on GitHub's runners: nixidae/nixidae#1365.
#
# `tarball` builds `internal.installer` per system with Nix from the action.
# `runner` installs that tarball on a fresh runner, with pynixd as the only
# daemon, and builds the same tarball again through it: every path of it is a
# substitution or a build that pynixd serves. Its out path must be the one
# `tarball` built. nixidae renders this to `.github/workflows/pynixd-installer.yml`.
{
  lib,
  ghalib,
  nixVersion,
}:
let
  inherit (import ./bootstrap.nix { inherit lib nixVersion; }) bootstrap settings;
  inherit (ghalib) steps;

  matrix.include = [
    {
      runner = "ubuntu-latest";
      system = "x86_64-linux";
    }
    {
      runner = "ubuntu-24.04-arm";
      system = "aarch64-linux";
    }
  ];
  artifactName = "pynixd-tarball-\${{ matrix.system }}";
in
ghalib.evalWorkflow {
  name = "installer";

  on = {
    push.branches = [ "**" ];
    workflow_dispatch = null;
  };

  jobs = {
    tarball = {
      runs-on = "\${{ matrix.runner }}";
      strategy = {
        fail-fast = false;
        inherit matrix;
      };
      timeout-minutes = 60;
      ghanix = bootstrap;
      steps = [
        {
          name = "Build the tarball";
          run = "nix build --file . internal.installer --print-out-paths --out-link tarball > out-path";
        }
        {
          name = "Collect the tarball";
          run = "mkdir -p upload && cp -L tarball/*.tar.zst out-path upload/";
        }
        (steps.uploadArtifact {
          inherit artifactName;
          # Under the working directory, `pynixd/`; download-artifact writes to the workspace's top.
          path = "upload";
          cond = null;
        })
      ];
    };

    runner = {
      needs = "tarball";
      runs-on = "\${{ matrix.runner }}";
      strategy = {
        fail-fast = false;
        inherit matrix;
      };
      timeout-minutes = 60;
      ghanix.checkout.enable = true;
      steps = [
        (steps.downloadArtifact { inherit artifactName; })
        (steps.installPynixd {
          tarball = "file://\${{ github.workspace }}";
          inherit settings;
        })
        {
          name = "pynixd is the only daemon";
          run = "systemctl is-active pynixd.service && ! pidof nix-daemon && nix store info";
        }
        {
          name = "Build the tarball through pynixd";
          run = ''nix build --file . internal.installer --print-out-paths --no-link > rebuilt && diff "$GITHUB_WORKSPACE/out-path" rebuilt'';
        }
        {
          name = "pynixd's journal";
          "if" = "\${{ !cancelled() }}";
          run = "sudo journalctl -u pynixd.service --no-pager";
        }
      ];
    };
  };
}
