"""The tarball on a machine with no Nix: unpack, install, then a user's work through pynixd alone.

A user builds, copies the output to a binary cache, deletes it and gets it
back by substitution. Each request goes to pynixd on the daemon socket; no
nix-daemon runs on the guest.
"""

from __future__ import annotations

import shlex

from vivarium_runner import Machine, Machines

NIX = "/nix/var/nix/profiles/default/bin/nix"


async def as_tester(vm: Machine, command: str) -> str:
    """`command`'s stdout alone, run as tester; stderr still reaches a failure's message."""
    out = "/tmp/as-tester.out"
    await vm.succeed(f"su - tester -c {shlex.quote(command)} > {out}", timeout=300)
    return (await vm.succeed(f"cat {out}; rm -f {out}")).strip()


def expression(python: str, system: str) -> str:
    """A derivation whose builder is the closure's own Python: the store holds nothing else."""
    return (
        "derivation {"
        ' name = "pynixd-installer-check";'
        f' system = "{system}";'
        f' builder = "${{builtins.storePath "{python}"}}";'
        ' args = [ "-c" "import os; open(os.environ[\\"out\\"], \\"w\\").write(\\"built\\\\n\\")" ];'
        " }"
    )


async def test(vms: Machines) -> None:
    vm = vms["runner"]
    try:
        await work(vms, vm)
    finally:
        _, log = await vm.execute("journalctl -u pynixd.service --no-pager")
        (vms.artifacts / "runner" / "pynixd.log").write_text(log)


async def work(vms: Machines, vm: Machine) -> None:
    settings = vms.settings
    await vm.fail("test -e /nix/var/nix/db/db.sqlite")
    await vm.succeed(f"zstd -dc {settings['tarball']} | tar -x -C /")
    # As ghanix passes a job's settings: `experimental-features` here must not turn off pynixd's build users.
    await vm.succeed("echo 'experimental-features = nix-command' > /tmp/extra.conf")
    await vm.succeed("/nix/.pynixd/install --trusted-user tester --extra-conf /tmp/extra.conf", timeout=300)
    print("[test] installed")

    await vm.succeed("systemctl is-active pynixd.service pynixd.socket")
    if await vm.count_processes("nix-daemon"):
        raise AssertionError("a nix-daemon runs: pynixd must be the only daemon")

    expr = shlex.quote(expression(settings["python"], settings["system"]))
    out = await as_tester(vm, f"{NIX} build --no-link --print-out-paths --impure --expr {expr}")
    content = (await vm.succeed(f"cat {out}")).strip()
    if content != "built":
        raise AssertionError(f"{out} holds {content!r}")
    # Without build users a root daemon builds as root, and the build still succeeds: the lock is the proof.
    await vm.succeed("test -e /nix/var/nix/userpool2/slot-0")
    print(f"[test] built {out} as an auto-allocated build user")

    # Signed: a client cannot turn off `require-sigs`, but a trusted one may name the key.
    await as_tester(vm, f"{NIX} key generate-secret --key-name check > /tmp/check.key")
    public = await as_tester(vm, f"{NIX} key convert-secret-to-public < /tmp/check.key")
    await as_tester(vm, f"{NIX} copy --to 'file:///tmp/cache?secret-key=/tmp/check.key' {out}")
    await as_tester(vm, f"{NIX} store delete {out}")
    await vm.fail(f"test -e {out}")
    print("[test] copied to a binary cache and deleted")

    await as_tester(
        vm,
        f"{NIX} build --no-link --impure --expr {expr} --max-jobs 0"
        f" --option substituters file:///tmp/cache --option trusted-public-keys {public}",
    )
    await vm.succeed(f"test -e {out}")
    print("[test] substituted it back with no build")
