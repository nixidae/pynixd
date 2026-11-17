# A GitHub Actions job as a pynixd builder: nixidae/nixidae#1473.
#
# `GhaProvider` dispatches one run per machine; the inputs are the names
# `pynixd/provisioning/gha.py` `dispatch_inputs` sends. The job installs pynixd
# from the tarball, registers by the sealed join ticket, and holds the runner
# until the controller cancels the run or the idle guard finds it quiet.
# `run-name` carries the launch's tags, which is how the provider finds its runs.
#
# Linux runners: the guard and the installer need systemd. No checkout: the job
# needs nothing of the repository but the secret `PYNIXD_TICKET_KEY`.
# nixidae renders this to `.github/workflows/pynixd-builder.yml`.
{ ghalib, ... }:
ghalib.evalWorkflow {
  name = "builder";
  run-name = "pynixd-builder \${{ inputs.tags }}";

  on.workflow_dispatch.inputs = {
    tags = {
      description = "The launch's tags, as JSON";
      required = true;
    };
    ticket = {
      description = "The join ticket, sealed under PYNIXD_TICKET_KEY to the tags";
      required = true;
    };
    runs_on = {
      description = "The runner's labels, as a JSON list";
      required = true;
    };
    installer = {
      description = "The URL of the installer tarball";
      required = true;
    };
    idle_seconds = {
      description = "The job ends after this many seconds with no build";
      required = true;
    };
    bootstrap_minutes = {
      description = "The install step's timeout";
      required = true;
    };
  };

  jobs.builder = {
    runs-on = "\${{ fromJSON(inputs.runs_on) }}";
    # GitHub's limit for a job on its own runners.
    timeout-minutes = 360;
    permissions = { };
    # Its `docker` and paths are GitHub's image's, and another service's runner may lack them.
    ghanix.freeDiskSpace.enable = false;
    # Not the source's directory, which nixidae makes the default: nothing is checked out.
    defaults.run = {
      working-directory = ".";
      shell = "bash";
    };
    steps = [
      {
        name = "Install pynixd and register as a builder";
        timeout-minutes = "\${{ fromJSON(inputs.bootstrap_minutes) }}";
        env = {
          INSTALLER = "\${{ inputs.installer }}";
          PYNIXD_TICKET = "\${{ inputs.ticket }}";
          PYNIXD_TICKET_CONTEXT = "\${{ inputs.tags }}";
          PYNIXD_TICKET_KEY = "\${{ secrets.PYNIXD_TICKET_KEY }}";
        };
        run = ''
          curl --fail --silent --show-error --location "$INSTALLER" | zstd -dc | sudo tar -x -C /
          sudo --preserve-env=PYNIXD_TICKET,PYNIXD_TICKET_CONTEXT,PYNIXD_TICKET_KEY /nix/.pynixd/install --sealed-join-ticket
        '';
      }
      {
        name = "Build until idle";
        env.IDLE_SECONDS = "\${{ inputs.idle_seconds }}";
        run = ''sudo /nix/.pynixd/idle-guard --wait "$IDLE_SECONDS"'';
      }
    ];
  };
}
