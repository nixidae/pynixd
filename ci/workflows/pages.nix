# The Pages workflow, as a value rather than as text.
#
# `.github/workflows/pages.yml` is rendered from this file. Edit this one;
# `checks.ciWorkflow` compares the render against what is committed and fails
# when they differ. `nix run --file . ci-workflow-update` writes the new
# render.
#
# It is the one workflow of pynixd's own repository, which josh exports from
# nixidae (nixidae/nixidae#4). The tests run in nixidae, from
# ci/workflows/tests.nix.
{
  lib,
  ghalib,
  nixVersion,
}:
let
  inherit (import ./bootstrap.nix { inherit lib nixVersion; }) bootstrap;

  # The Pages deployment has one destination, so only main publishes.
  mainOnly = "github.ref == 'refs/heads/main'";
in
ghalib.evalWorkflow {
  name = "Pages";

  on = {
    push.branches = [ "main" ];
    workflow_dispatch = null;
  };

  jobs = {
    docs-build = {
      "if" = mainOnly;
      runs-on = "ubuntu-24.04";
      # Under a minute when it works. A job with no bound waits six hours.
      timeout-minutes = 20;
      ghanix = lib.mkMerge [
        bootstrap
        { nix.cachix.enable = true; }
      ];
      steps = [
        {
          name = "Build documentation";
          run = "nix build --file . pynixd-docs --out-link result --print-build-logs --print-out-paths";
        }
        {
          name = "Verify docs closure";
          run = "nix store verify --recursive --no-trust \"$(readlink -f result)\"";
        }
        {
          name = "Prepare Pages artifact";
          run = ''
            mkdir -p public
            cp -r --no-preserve=mode,ownership result/. public/
          '';
        }
        {
          uses = "actions/upload-pages-artifact@v3";
          "with".path = "public";
        }
      ];
    };

    docs-deploy = {
      "if" = mainOnly;
      needs = "docs-build";
      runs-on = "ubuntu-24.04";
      # It deploys an artifact `docs-build` made, and installs no Nix, so it
      # has nothing to make room for.
      ghanix.freeDiskSpace.enable = false;
      permissions = {
        pages = "write";
        id-token = "write";
      };
      environment = {
        name = "github-pages";
        url = "\${{ steps.deployment.outputs.page_url }}";
      };
      concurrency = {
        group = "pages";
        cancel-in-progress = false;
      };
      steps = [
        {
          name = "Deploy to GitHub Pages";
          id = "deployment";
          uses = "actions/deploy-pages@v4";
        }
      ];
    };
  };
}
