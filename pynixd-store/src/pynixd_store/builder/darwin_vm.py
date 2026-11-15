"""A darwin build in a macOS guest started from a snapshot (nixidae/nixidae#1168).

The guest runs pynixd's darwin helper as the host would (`darwin.py`): the same spec, the same profile and the
same paths. Three virtio-fs shares give it what the build may see, each at its host path:

- `store`: the build store, read-only, with the build's inputs as its allowlist (and the helper's closure when
  the build store is the host's). A tmpfs over it takes what the builder writes, the outputs among it.
- `host`: the host's store, read-only, with the helper's closure as its allowlist, when the build store is
  another one.
- `top`: the build's top directory, writable: the spec, and the helper's status and usage. The build directory
  in it is a tmpfs in the guest (`darwin_vm_guest`).

After the helper ends, the guest copies each output into `top/vm-out`. The launcher moves it
to its place in the store, where `collect_outputs` finds it as after a build on the host. A virtio-fs share
cannot be the upper layer of a union: it hides the layer below (`macos-guest-spike/README.md`).

The snapshot is machine state, not a store path: `darwin_builder.py save` with `PYNIXD=1` makes it. Its
`snapshot.json` names QEMU, its arguments, darwin-virtiofsd and the agent's vsock port.

    python -m pynixd_store.builder.darwin_vm TOP/darwin-vm.json
"""

from __future__ import annotations

import contextlib
import functools
import json
import os
import resource
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import anyio
from anyio.abc import SocketStream

from ..add import delete_path
from .restriction import SOCKET_NAME
from .usage import from_rusage, write_usage
from .vm import EXIT_NAME, STORE_ALLOWLIST_NAME, VMM_USAGE_NAME

if TYPE_CHECKING:
    from anyio.abc import Process

    from .site import BuildPaths

JOB_NAME = "darwin-vm.json"
OUTPUTS_NAME = "vm-out"
KEPT_NAME = "vm-build"
HOST_ALLOWLIST_NAME = "vm-allowlist-host"
HOST_STORE = Path("/nix/store")
#: The guest relays each connection to the builder's socket to this host port, which QEMU gives to the unix socket
#: `run/vsock.sock_<port>`.
BUILDER_VSOCK_PORT = 3041
#: The run directory holds QEMU's sockets, and a top directory is longer than darwin's `sun_path`.
RUN_PARENT = Path("/private/tmp")
#: Measured in the spike: a fresh guest once ignored one handshake for good; each is retried.
HANDSHAKE_SECONDS = 2.0
READY_SECONDS = 60.0
#: A watchdog kills QEMU and the shares' servers when the launcher dies: macOS has no parent-death signal.
WATCHDOG = """import os, select, signal, sys
kq = select.kqueue()
try:
    kq.control([select.kevent(int(sys.argv[1]), select.KQ_FILTER_PROC, select.KQ_EV_ADD, select.KQ_NOTE_EXIT)], 1)
except ProcessLookupError:
    pass
else:
    kq.control(None, 1)
for pid in sys.argv[2:]:
    try:
        os.kill(int(pid), signal.SIGKILL)
    except ProcessLookupError:
        pass
"""


@dataclass(frozen=True)
class DarwinBuildVm:
    snapshot: Path
    python: Path
    """The interpreter that runs the helper in the guest: a store path, unlike an editable `sys.executable`."""
    helper_closure: frozenset[str]

    @classmethod
    def from_environment(cls, snapshot: Path) -> DarwinBuildVm:
        """What the package names: the helper's interpreter (`PYNIXD_DARWIN_VM_PYTHON`) and its closure
        (`PYNIXD_DARWIN_VM_CLOSURE`, a file of store paths)."""
        names = ("PYNIXD_DARWIN_VM_PYTHON", "PYNIXD_DARWIN_VM_CLOSURE")
        missing = [name for name in names if name not in os.environ]
        if missing:
            raise ValueError(
                f"darwin_build_vm needs {', '.join(missing)} in the environment; the pynixd package sets them"
            )
        if not (snapshot / "snapshot.json").is_file():
            raise ValueError(f"darwin_build_vm: {snapshot} holds no snapshot.json")
        return cls(
            snapshot=snapshot,
            python=Path(os.environ["PYNIXD_DARWIN_VM_PYTHON"]),
            helper_closure=frozenset(Path(os.environ["PYNIXD_DARWIN_VM_CLOSURE"]).read_text().split()),
        )

    def write_job(
        self,
        paths: BuildPaths,
        spec_file: Path,
        store_dir: Path,
        visible: frozenset[str],
        outputs: list[str],
        *,
        keep_failed: bool,
        recursive: bool,
    ) -> Path:
        """The launcher's job in the top directory: *visible* are the build store's paths the guest may see,
        *outputs* the store paths the builder may make. With *recursive*, the guest relays the builder's socket
        to pynixd's, at the same path on the host."""
        top = paths.top
        job = {
            "snapshot": str(self.snapshot),
            "python": str(self.python),
            "store_dir": str(store_dir),
            "visible": sorted(visible),
            "helper": sorted(self.helper_closure),
            "top": str(top),
            "build": str(paths.tmp),
            "keep_failed": keep_failed,
            "builder_socket": str(paths.tmp / SOCKET_NAME) if recursive else None,
            "spec": str(spec_file),
            "outputs": outputs,
        }
        path = top / JOB_NAME
        path.write_text(json.dumps(job))
        return path


class Qmp:
    def __init__(self, stream: SocketStream) -> None:
        self.stream = stream
        self.buffer = b""

    async def line(self) -> dict[str, Any]:
        while b"\n" not in self.buffer:
            self.buffer += await self.stream.receive()
        raw, self.buffer = self.buffer.split(b"\n", 1)
        return json.loads(raw)

    async def call(self, execute: str, **arguments: object) -> Any:
        message: dict[str, object] = {"execute": execute}
        if arguments:
            message["arguments"] = arguments
        await self.stream.send(json.dumps(message).encode() + b"\n")
        while True:
            reply = await self.line()
            if "error" in reply:
                raise RuntimeError(f"QMP {execute}: {reply['error']}")
            if "return" in reply:
                return reply["return"]


async def _connect_guest(vsock: Path, port: int) -> SocketStream:
    """A stream to the guest's *port*, through virtio-vsock-unix's handshake."""
    stream = await anyio.connect_unix(vsock)
    line = b""
    try:
        with anyio.fail_after(HANDSHAKE_SECONDS):
            await stream.send(f"CONNECT {port}\n".encode())
            while not line.endswith(b"\n"):
                line += await stream.receive(1)
    except BaseException:
        await stream.aclose()
        raise
    if not line.startswith(b"OK "):
        await stream.aclose()
        raise OSError(f"guest port {port} refused: {line!r}")
    return stream


async def _ask(vsock: Path, port: int, request: bytes) -> bytes:
    async with await _connect_guest(vsock, port) as stream:
        await stream.send(request)
        await stream.send_eof()
        reply = b""
        with contextlib.suppress(anyio.EndOfStream):
            while True:
                reply += await stream.receive()
    return reply


async def _until_ready(vsock: Path, port: int, vm: Process) -> None:
    with anyio.fail_after(READY_SECONDS):
        while True:
            if vm.returncode is not None:
                raise RuntimeError(f"QEMU exited with {vm.returncode} before the guest answered")
            with contextlib.suppress(OSError, anyio.EndOfStream, anyio.BrokenResourceError):
                if await _ask(vsock, port, b"ping\n") == b"ok\n":
                    return
            await anyio.sleep(0.05)


async def _server(server: str, run: Path, tag: str, shared: Path, *options: str) -> Process:
    sock = run / f"{tag}.sock"
    with (run / f"{tag}d.log").open("ab") as log:
        proc = await anyio.open_process(
            [server, "--socket-path", str(sock), "--shared-dir", str(shared), *options],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
        )
    with anyio.fail_after(5):
        while not sock.exists():
            if proc.returncode is not None:
                raise RuntimeError(f"darwin-virtiofsd for {tag} exited with {proc.returncode}; see {run}/{tag}d.log")
            await anyio.sleep(0.005)
    return proc


async def _run(job: dict[str, Any], run: Path) -> None:
    top = Path(job["top"])
    store_dir = Path(job["store_dir"])
    snapshot = Path(job["snapshot"])
    manifest = json.loads((snapshot / "snapshot.json").read_text())
    pynixd = manifest["pynixd"]
    server = functools.partial(_server, pynixd["server"], run)

    def at(name: str) -> str:
        return name if Path(name).is_absolute() else str(snapshot / name)

    await anyio.run_process(["cp", "-c", at(manifest["aux"]), str(run / "aux.img")])
    await anyio.run_process(
        [manifest["qemuImg"], "create", "-q", "-f", "qcow2", "-F", "qcow2", "-b", at(manifest["overlay"]),
         str(run / "overlay.qcow2")]
    )  # fmt: skip
    separate = store_dir != HOST_STORE
    shown, host = set(job["visible"]), set(job["helper"])
    # The profile's paths in a store: an impure host dependency, such as the host's store for a recursive build.
    darwin = json.loads(Path(job["spec"]).read_text())["darwin"]
    for _target, source, _optional in darwin["paths"]:
        if Path(source).is_relative_to(store_dir):
            shown.add(source)
        elif Path(source).is_relative_to(HOST_STORE):
            host.add(source)
    if not separate:
        shown |= host
    (top / STORE_ALLOWLIST_NAME).write_text("".join(f"{p}\n" for p in sorted(shown)))
    (top / HOST_ALLOWLIST_NAME).write_text("".join(f"{p}\n" for p in sorted(host)))
    if job["builder_socket"] is not None:
        (run / f"vsock.sock_{BUILDER_VSOCK_PORT}").symlink_to(job["builder_socket"])
    empty = run / "empty"
    empty.mkdir()
    async with contextlib.AsyncExitStack() as stack:
        children: list[Process] = []

        async def started(proc: Process) -> Process:
            children.append(proc)
            stack.push_async_callback(_end, proc)
            return proc

        await started(await server("store", store_dir, "--allowlist", str(top / STORE_ALLOWLIST_NAME)))
        if separate:
            await started(await server("host", HOST_STORE, "--allowlist", str(top / HOST_ALLOWLIST_NAME)))
        else:
            # The snapshot has the device; QEMU needs a server behind it.
            await started(await server("host", empty))
        await started(await server("top", top, "--writable"))
        args = [a.replace("{run}", str(run)).replace("{snapshot}", str(snapshot)) for a in manifest["args"]]
        with (run / "qemu.log").open("ab") as log:
            vm = await started(
                await anyio.open_process([manifest["qemu"], *args], stdin=subprocess.DEVNULL, stdout=log, stderr=log)
            )
        watchdog = await anyio.open_process(
            [sys.executable, "-I", "-c", WATCHDOG, str(os.getpid()), *(str(p.pid) for p in children)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        stack.push_async_callback(_end, watchdog)
        with anyio.fail_after(10):
            while True:
                try:
                    qmp = Qmp(await anyio.connect_unix(run / "qmp.sock"))
                    break
                except OSError:
                    if vm.returncode is not None:
                        raise RuntimeError(f"QEMU exited with {vm.returncode}; see {run}/qemu.log") from None
                    await anyio.sleep(0.01)
            await qmp.line()
            await qmp.call("qmp_capabilities")
        stack.push_async_callback(_quit, qmp, vm)
        if channels := manifest.get("mappedRam"):
            await qmp.call(
                "migrate-set-capabilities",
                capabilities=[{"capability": "mapped-ram", "state": True}, {"capability": "multifd", "state": True}],
            )
            await qmp.call("migrate-set-parameters", **{"multifd-channels": channels})
        await qmp.call("migrate-incoming", uri=f"file:{at(manifest['state'])}")
        while (await qmp.call("query-status"))["status"] == "inmigrate":
            await anyio.sleep(0.01)
        await qmp.call("cont")
        vsock = run / "vsock.sock"
        await _until_ready(vsock, pynixd["agentVsockPort"], vm)
        clock = await _ask(vsock, manifest["clockVsockPort"], f"{int(time.time())}\n".encode())
        if not clock.startswith(b"ok"):
            raise RuntimeError(f"the guest did not set its clock: {clock!r}")
        commands = [f"mount store {store_dir}", f"union {store_dir}"]
        if separate:
            commands.append(f"mount host {HOST_STORE}")
        commands += [
            f"mount top {top}",
            f"run {job['python']} -I -m pynixd_store.builder.darwin_vm_guest {top / JOB_NAME}",
        ]
        if spaced := [p for p in (store_dir, top, job["python"]) if " " in str(p)]:
            raise ValueError(f"the guest's agent splits words on spaces, and '{spaced[0]}' has one")
        last = b""
        async with await _connect_guest(vsock, pynixd["agentVsockPort"]) as stream:
            await stream.send("".join(f"{c}\n" for c in commands).encode())
            with contextlib.suppress(anyio.EndOfStream, anyio.BrokenResourceError):
                while True:
                    piece = await stream.receive()
                    last = (last + piece)[-4096:]
                    os.write(1, piece)
        if not (top / EXIT_NAME).exists():
            reason = last.decode(errors="replace").strip().splitlines()[-1:] or ["the guest ended the connection"]
            (top / EXIT_NAME).write_text(f"init {reason[0]}\n")
        _move_outputs(top / OUTPUTS_NAME, store_dir)
        if (kept := top / KEPT_NAME).is_dir():
            build = Path(job["build"])
            delete_path(build)
            os.rename(kept, build)


def _move_outputs(outputs: Path, store_dir: Path) -> None:
    if not outputs.is_dir():
        return
    for output in outputs.iterdir():
        target = store_dir / output.name
        delete_path(target)
        os.rename(output, target)


async def _end(proc: Process) -> None:
    with anyio.CancelScope(shield=True):
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.terminate()
            with anyio.move_on_after(5):
                await proc.wait()
            if proc.returncode is None:
                proc.kill()
        await proc.wait()


async def _quit(qmp: Qmp, vm: Process) -> None:
    with anyio.CancelScope(shield=True), anyio.move_on_after(2), contextlib.suppress(Exception):
        await qmp.call("quit")
    with anyio.CancelScope(shield=True):
        with anyio.move_on_after(5):
            await vm.wait()
        await qmp.stream.aclose()


async def main(job_path: Path) -> int:
    job = json.loads(job_path.read_text())
    top = Path(job["top"])
    run = Path(tempfile.mkdtemp(prefix="pynixd-vm-", dir=RUN_PARENT))
    try:
        await _run(job, run)
    except Exception as e:
        print(f"darwin build VM: {e}", file=sys.stderr, flush=True)
        if not (top / EXIT_NAME).exists():
            (top / EXIT_NAME).write_text(f"init {e}\n")
        return 1
    finally:
        write_usage(top / VMM_USAGE_NAME, from_rusage(resource.getrusage(resource.RUSAGE_CHILDREN)))
        if os.environ.get("PYNIXD_DARWIN_VM_KEEP") != "1":
            shutil.rmtree(run, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(anyio.run(main, Path(sys.argv[1])))
