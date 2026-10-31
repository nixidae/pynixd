# Test Conventions

## Running the suites in a guest

```sh
nix build --file . internal.tests.guest                        # QEMU, needs /dev/kvm
nix run --file . internal.tests.guest.driver -- --out ./o         # the same, by hand
nix run --file . internal.tests.guest.driver -- --out ./o --only unit --break-on-failure
```

One NixOS guest, one phase per suite, poweroff. The phases are
`prepare`, then `unit`, `protocol` and `parity` at once, a guest each,
then `leaks`. Each suite needs only `prepare`, so one failing skips none
of the others. `leaks`
runs even after a failure. Nothing survives the run: no store under
`/tmp`, no daemon, no socket. That is what it is for — these suites
start daemons and build into stores they make, and a build sandbox
cleans up files and not processes.

**The attempt never fails.** `tests.guest` reads `tests.guest.attempt`,
whose output keeps a failed run: `phases.json`, `junit.xml`,
`events.jsonl`, each suite's log under `artifacts/<suite>/`, and
each guest's process census. Every test of every suite is a case in `junit.xml`,
because each suite writes JUnit to `/artifacts/junit/`, and
vivarium reads it back. Read them there rather than running it
again:

```sh
a=$(nix eval --raw --file . internal.tests.guest.attempt)
jq -r '.phases[] | "\(.name)\t\(.state)"' $a/phases.json
jq -c 'select(.kind == "case" and .data.outcome == "failed") | .text' $a/events.jsonl
```

`--break-on-failure` keeps the guest up after a failing suite, and
`vivarium ctl --out ./o exec ...` reaches in. See vivarium's
AGENTS.md.

Two things a test in there must respect:

- Write evidence to `/artifacts`, which is a host directory. Anything
  under `/tmp` in the guest dies with the guest. `$PYNIXD_TEST_LOG_DIR`
  is what points the suite's own per-test logs at it.
- The suites run as `tester`, who does not own the store, so every `nix`
  call goes through the guest's daemon — the same shape as a developer's
  machine, and not the same as running as root.

The run counts every process in the guest before and after, and fails on a
daemon that outlived its suite. That check found issue #36 on its first
run.

`tests/guest/` holds the phase scripts: `prepare.py`, `suite.py` (one
script for every suite, told apart by `vms.phase`) and `leaks.py`.
`tests/derivations/guest/` declares the phases and the suites. They live
here rather than in vivarium: that repository is the library, and
a test about pynixd belongs beside pynixd.

## `@pytest.mark.asyncio`

Do **NOT** add `@pytest.mark.asyncio` markers to any test function. The conftest.py hook `pytest_collection_modifyitems` already auto-detects `async def` test functions and wraps them with `anyio.fail_after`. Adding the explicit marker is redundant.
