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
    # pynixd builds darwin derivations with no nix-daemon: nixidae/nixidae#1367.
    {
      runner = "macos-latest";
      system = "aarch64-darwin";
    }
  ];
  artifactName = "pynixd-tarball-\${{ matrix.system }}";
  # One tarball per system, installed on each volume a Mac can give /nix: macOS's
  # case-insensitive default, and the case-sensitive one a Linux builder's names need.
  runnerMatrix.include =
    map (
      row: row // lib.optionalAttrs (row.system == "aarch64-darwin") { volume = "case-insensitive"; }
    ) matrix.include
    ++ [
      {
        runner = "macos-latest";
        system = "aarch64-darwin";
        volume = "case-sensitive";
      }
    ];

  # A path no cache holds before this run: its build is this run's alone.
  probe = ''nix build --impure --print-out-paths --no-link --expr '(import ./. { }).internal.pkgs.runCommand "pynixd-gha-cache-probe" { run = builtins.getEnv "RUN"; } "echo $run > $out"' '';
  cacheJob = needs: steps: {
    inherit needs;
    runs-on = "ubuntu-latest";
    timeout-minutes = 30;
    env.RUN = "\${{ github.run_id }}-\${{ github.run_attempt }}";
    ghanix = {
      checkout.enable = true;
      nix.install = {
        enable = true;
        daemon = "pynixd";
        pynixdArtifact = "pynixd-tarball-x86_64-linux";
        pynixdGhaCache = true;
        inherit settings;
      };
    };
    steps = steps ++ [
      {
        name = "pynixd's journal";
        "if" = "\${{ !cancelled() }}";
        run = "sudo journalctl -u pynixd.service --no-pager";
      }
    ];
  };
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
          # `.tar.zst` on Linux, `.tar.xz` on darwin (`nix/installer/default.nix`).
          run = "mkdir -p upload && cp -L tarball/pynixd-*.tar.* out-path upload/";
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
        matrix = runnerMatrix;
      };
      timeout-minutes = 60;
      ghanix = {
        checkout.enable = true;
        nix.install = {
          enable = true;
          daemon = "pynixd";
          pynixdArtifact = artifactName;
          # A Linux row has no `volume`; the step reads it on macOS alone.
          macosVolume = "\${{ matrix.volume }}";
          inherit settings;
        };
      };
      steps = [
        {
          name = "pynixd is the only daemon";
          # `pgrep -x`, not `pidof`: macOS has no `pidof`.
          run = ''if [ "$RUNNER_OS" = macOS ]; then sudo launchctl print system/org.nixidae.pynixd > /dev/null; else systemctl is-active pynixd.service; fi && ! pgrep -x nix-daemon && nix store info'';
        }
        {
          name = "/nix is the volume asked for, and use-case-hack follows it";
          "if" = "runner.os == 'macOS'";
          env.VOLUME = "\${{ matrix.volume }}";
          run = ''if [ "$VOLUME" = case-sensitive ]; then fs="Case-sensitive APFS" hack=false; else fs=APFS hack=true; fi && test "$(diskutil info -plist /nix | plutil -extract FilesystemName raw -)" = "$fs" && test "$(nix config show use-case-hack)" = "$hack"'';
        }
        {
          name = "Build the tarball through pynixd";
          run = ''nix build --file . internal.installer --print-out-paths --no-link > rebuilt && diff "$RUNNER_TEMP/pynixd-tarball/out-path" rebuilt'';
        }
        {
          name = "pynixd's journal";
          "if" = "\${{ !cancelled() }}";
          # launchd's job writes to the file its plist names (`nix/installer/default.nix`).
          run = ''if [ "$RUNNER_OS" = macOS ]; then sudo cat /var/log/pynixd.log; else sudo journalctl -u pynixd.service --no-pager; fi'';
        }
      ];
    };

    /*
      The GitHub Actions cache store (nixidae/nixidae#1377): `cache-write`
      builds a path of this run and uploads it, and `cache-read`, on a fresh
      runner, substitutes it and builds nothing. The fake service of the
      unit tests holds pynixd's reading of the protocol; this is the proof.
    */
    cache-write = cacheJob "tarball" [
      {
        name = "Build a path of this run";
        run = "${probe}> probe";
      }
    ];
    cache-read =
      cacheJob
        [ "tarball" "cache-write" ]
        [
          {
            name = "Substitute the path of this run";
            run = "${probe}--max-jobs 0";
          }
        ];

    /*
      The release `nix.install.pynixdTarball` names, replaced from main once
      every system's `runner` passed. One tag, moved to the commit each time,
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
            # With no checkout, `gh` has no `.git` to read the repository from.
            GH_REPO = "\${{ github.repository }}";
            TAG = "pynixd-installer";
          };
          run = lib.concatStringsSep " && " [
            ''cd "$RUNNER_TEMP/release"''
            # Each call names itself: a 403 says only "not accessible", not which call.
            ''echo "publish: release" >&2''
            ''(gh release view "$TAG" > /dev/null 2>&1 || gh release create "$TAG" --target "$GITHUB_SHA" --prerelease --title "pynixd installer" --notes "pynixd as the only Nix daemon of a machine: nixidae/nixidae#1365")''
            ''echo "publish: move the tag" >&2''
            ''gh api -X PATCH "repos/$GITHUB_REPOSITORY/git/refs/tags/$TAG" -f sha="$GITHUB_SHA" -F force=true > /dev/null''
            ''echo "publish: upload" >&2''
            ''gh release upload "$TAG" pynixd-*.tar.* --clobber''
            ''echo "publish: notes" >&2''
            ''gh release edit "$TAG" --notes "pynixd as the only Nix daemon of a machine: nixidae/nixidae#1365. Built from $GITHUB_SHA."''
          ];
        }
      ];
    };
  };
}
