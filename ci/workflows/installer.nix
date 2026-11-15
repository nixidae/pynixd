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
      ghanix = {
        checkout.enable = true;
        nix.install = {
          enable = true;
          daemon = "pynixd";
          pynixdArtifact = artifactName;
          inherit settings;
        };
      };
      steps = [
        {
          name = "pynixd is the only daemon";
          run = "systemctl is-active pynixd.service && ! pidof nix-daemon && nix store info";
        }
        {
          name = "Build the tarball through pynixd";
          run = ''nix build --file . internal.installer --print-out-paths --no-link > rebuilt && diff "$RUNNER_TEMP/pynixd-tarball/out-path" rebuilt'';
        }
        {
          name = "pynixd's journal";
          "if" = "\${{ !cancelled() }}";
          run = "sudo journalctl -u pynixd.service --no-pager";
        }
      ];
    };

    /*
      The release `nix.install.pynixdTarball` names, replaced from main once
      both systems' `runner` passed. One tag, moved to the commit each time,
      so the release names the commit that built what it holds. `pynixd-`
      names the source: every source shares one tag namespace.
    */
    publish = {
      needs = "runner";
      "if" = "github.ref == 'refs/heads/main'";
      runs-on = "ubuntu-latest";
      timeout-minutes = 15;
      permissions.contents = "write";
      # A later push waits, so two runs never replace the assets at once.
      concurrency = {
        group = "pynixd-installer-publish";
        cancel-in-progress = false;
      };
      ghanix.freeDiskSpace.enable = false;
      steps = [
        {
          name = "Download both tarballs";
          uses = "actions/download-artifact@main";
          timeout-minutes = 10;
          "with" = {
            pattern = "pynixd-tarball-*";
            path = "\${{ runner.temp }}/release";
            merge-multiple = true;
          };
        }
        {
          name = "Replace the release";
          timeout-minutes = 10;
          shell = "bash";
          # This job has no checkout, so the workflow-wide `pynixd` directory
          # does not exist, and the runner fails the step before bash starts.
          working-directory = "/";
          env = {
            GH_TOKEN = "\${{ github.token }}";
            TAG = "pynixd-installer";
          };
          run = lib.concatStringsSep " && " [
            ''cd "$RUNNER_TEMP/release"''
            ''(gh release view "$TAG" > /dev/null 2>&1 || gh release create "$TAG" --target "$GITHUB_SHA" --prerelease --title "pynixd installer" --notes "pynixd as the only Nix daemon of a machine: nixidae/nixidae#1365")''
            ''gh api -X PATCH "repos/$GITHUB_REPOSITORY/git/refs/tags/$TAG" -f sha="$GITHUB_SHA" -F force=true > /dev/null''
            ''gh release upload "$TAG" pynixd-*.tar.zst --clobber''
            ''gh release edit "$TAG" --notes "pynixd as the only Nix daemon of a machine: nixidae/nixidae#1365. Built from $GITHUB_SHA."''
          ];
        }
      ];
    };
  };
}
