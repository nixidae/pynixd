# What GitHub's Actions cache service does where its documentation is silent:
# nixidae/nixidae#1377. By hand only (`gh workflow run`): `reads` rate limits
# the repository's cache for up to a minute on purpose.
#
# `orphan` leaves two entries unfinalized, `orphan-later` asks about them from
# a fresh job. `recheck` names an earlier run's tag to ask again hours later.
# nixidae renders this to `.github/workflows/pynixd-gha-cache-probe.yml`.
{ ghalib, ... }:
let
  inherit (ghalib) steps;
  probeJob = needs: run: {
    inherit needs;
    runs-on = "ubuntu-latest";
    timeout-minutes = 15;
    env.TAG = "\${{ inputs.recheck || github.run_id }}";
    ghanix.checkout.enable = true;
    steps = [
      steps.ghaCacheCredentials
      {
        name = "Probe";
        inherit run;
      }
    ];
  };
in
ghalib.evalWorkflow {
  name = "gha-cache-probe";

  on.workflow_dispatch.inputs.recheck = {
    description = "An earlier run's tag: only ask about its unfinalized entries";
    required = false;
    default = "";
  };

  jobs = {
    orphan = (probeJob [ ] ''python3 ci/gha_cache_probe.py orphan "$TAG"'') // {
      "if" = "\${{ inputs.recheck == '' }}";
    };
    orphan-later = (probeJob [ "orphan" ] ''python3 ci/gha_cache_probe.py orphan-check "$TAG"'') // {
      "if" = "\${{ !cancelled() }}";
    };
    reads = (probeJob [ "orphan-later" ] ''python3 ci/gha_cache_probe.py reads "$TAG"'') // {
      "if" = "\${{ inputs.recheck == '' }}";
    };
  };
}
