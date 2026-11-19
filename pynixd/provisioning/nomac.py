"""NoMac (nomac.app) Macs as darwin builders: one Mac session per machine.

A session is a fresh macOS VM with no Nix. When it is ready, the provider
posts one job through NoMac's jobs API that makes the /nix volume, unpacks
the darwin installer tarball and installs pynixd with the join ticket. The
builder then dials the controller like any other. No SSH key is needed.

A session carries no tags, so the provider keeps them in `state_file`,
written before each start. The start sends an `Idempotency-Key`: NoMac
answers a repeat of it with the same session, so a start whose answer was
lost is asked again rather than started twice. A session with no entry in
the file is not this provider's (the account's owner may run Macs of their
own) and is never listed or ended.

The controller ends an idle builder itself. NoMac ends a session at its time
limit, its spend cap or its own idle timeout, so a dead controller costs no
more than those. A build keeps the Mac awake while it uses the CPU.
Measured 2026-11-19: ready 27-28 s after the start; `BOOTSTRAP` 30-33 s, run
verbatim with a throwaway ticket; the jobs POST answers in 0.4 s. A repeated
start key answers 200 with the same session, a stop of a stopped session
200, and the list is newest first. The VM has no Hypervisor.framework
(`kern.hv_support` 0), so it builds no Linux guest.
"""

from __future__ import annotations

import datetime
import json
import os
import time
import uuid
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Final

import aiohttp
import anyio
import structlog
from pydantic import BaseModel, ConfigDict

from .provider import Machine, MachineState, NoCapacity, NotFound, ProviderError
from .ticket import JoinTicket

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ..config import NomacSettings, NomacTemplateSettings

log = structlog.get_logger(__name__)

_TIMEOUT: Final = aiohttp.ClientTimeout(total=60)
_ENDED: Final = frozenset({"stopping", "stopped", "failed"})
_GONE: Final = frozenset({HTTPStatus.NOT_FOUND, HTTPStatus.GONE})

BOOTSTRAP: Final = r"""set -eu -o pipefail
if [ ! -d /nix ]; then
  printf 'nix\n' | sudo tee -a /etc/synthetic.conf > /dev/null
  sudo /System/Library/Filesystems/apfs.fs/Contents/Resources/apfs.util -t || test -d /nix
  sudo diskutil apfs addVolume "$(diskutil info -plist / | plutil -extract ParentWholeDisk raw -)" "$PYNIXD_VOLUME" Nix -mountpoint /nix > /dev/null
  sudo diskutil enableOwnership /nix > /dev/null
fi
curl --fail --silent --show-error --location "$PYNIXD_INSTALLER" | sudo tar -x -C /
sudo mkdir -p /etc/pynixd
printf '%s' "$PYNIXD_TICKET" | sudo sh -c 'umask 077 && cat > /etc/pynixd/join-ticket.json'
printf '%s' "$PYNIXD_INITIATOR" | sudo sh -c 'cat > /etc/pynixd/reverse-initiator.json'
printf '%s' "$PYNIXD_EXTRA_CONF" | sudo sh -c 'cat > /etc/pynixd/extra-nix.conf'
sudo /nix/.pynixd/install --reverse-initiator /etc/pynixd/reverse-initiator.json --extra-conf /etc/pynixd/extra-nix.conf
"""
"""The job that makes a fresh Mac a builder. The ticket travels in the job's environment, never its command
line or output. Its file is made under umask 077, so no other user can read it at any moment.
`/etc/pynixd/join-ticket.json` is the installer's `JOIN_TICKET`. The volume is made as ghanix's darwin
install step makes it (`ghanix/steps.nix`)."""

type Session = dict[str, Any]
"""A session as NoMac's REST API answers it: its schema types it, not Python."""


class Refused(ProviderError):
    """NoMac answered the start with a refusal: no session exists to find later."""


class Entry(BaseModel):
    """What the provider remembers of one start."""

    model_config = ConfigDict(extra="forbid")

    request_key: str
    template: str
    tags: dict[str, str]
    launched_at: float
    session_id: str | None = None
    """`None` until NoMac answers the start."""
    ticket: str | None = None
    """The join ticket until the bootstrap job is posted; then dropped."""
    abandoned: bool = False
    """The launch was reported failed: a session a repeated start finds is ended at once."""


class State(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entries: dict[str, Entry] = {}
    """By request key."""


def builder_files(ticket: str) -> tuple[str, str]:
    """The reverse initiator settings and the `nix.conf` lines of a builder set up from *ticket*.

    The same as the installer's `builder_settings`, which the installer tarball holds and pynixd does not.
    """
    parsed = JoinTicket.model_validate_json(ticket)
    initiator: dict[str, object] = {  # object: JSON, which `ReverseInitiatorSettings` parses
        "join_ticket": "/etc/pynixd/join-ticket.json",
        "system_features": parsed.system_features,
    }
    if parsed.max_jobs is not None:
        initiator["max_jobs"] = parsed.max_jobs
    extra = f"extra-system-features = {' '.join(parsed.system_features)}\n" if parsed.system_features else ""
    return json.dumps(initiator), extra


def start_body(template: NomacTemplateSettings) -> dict[str, object]:  # object: the start's JSON
    body: dict[str, object] = {
        "profile": "standard",
        "limits": {
            "max_duration_seconds": template.max_duration_seconds,
            "idle_timeout_seconds": template.idle_timeout_seconds,
            # The API takes the cap as a decimal string of millionths of a dollar.
            "spend_cap_microunits": str(round(template.spend_cap_usd * 1_000_000)),
        },
        "access": {"drive": "none"},
    }
    if template.parallel:
        body["parallel"] = True
    return body


def write_private(path: os.PathLike[str], text: str) -> None:
    """Replace *path* with *text*, readable by its owner alone."""
    target = os.fspath(path)
    temporary = f"{target}.{os.getpid()}"
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as out:
        out.write(text)
    os.replace(temporary, target)


def _epoch(value: object) -> float | None:
    if isinstance(value, int | float):
        return value / 1000
    if isinstance(value, str):
        return datetime.datetime.fromisoformat(value).timestamp()
    return None


def machine(session: Session, entry: Entry) -> Machine:
    state = str(session.get("state"))
    if state in _ENDED:
        found = MachineState.ENDED
    elif state == "ready" and entry.ticket is None:
        found = MachineState.RUNNING
    else:
        # Queued, starting, or ready and not yet a builder: it is booting.
        found = MachineState.PENDING
    error = None
    if state == "failed":
        error = f"the session failed: {session.get('failure_code') or session.get('end_reason')}"
    notice = None
    if session.get("stop_notice") and found is not MachineState.ENDED:
        # NoMac's docs say answers carry `stop_notice` in a session's last 15 minutes; no answer measured so
        # far held one. `hard_deadline_at` (measured, milliseconds) is when the time limit stops the Mac.
        notice = _epoch((session.get("activity") or {}).get("hard_deadline_at"))
    return Machine(provider_id=str(session["id"]), state=found, tags=dict(entry.tags), notice=notice, error=error)


class NomacProvider:
    name = "nomac"

    def __init__(self, settings: NomacSettings) -> None:
        self._settings = settings
        self._http = aiohttp.ClientSession(timeout=_TIMEOUT)
        self._lock = anyio.Lock()
        """One reader and writer of `state_file` at a time."""

    def _template(self, name: str) -> NomacTemplateSettings:
        try:
            return self._settings.templates[name]
        except KeyError:
            raise NotFound(f"nomac: no template {name!r}") from None

    async def _call(
        self,
        method: str,
        path: str,
        key: str | None = None,
        **kwargs: Any,  # noqa: ANN401 -- aiohttp's request options
    ) -> tuple[int, Any]:  # noqa: ANN401 -- the API's JSON, read by each caller
        api_key = (await anyio.Path(self._settings.api_key_file).read_text()).strip()
        headers = {"Authorization": f"Bearer {api_key}"}
        if key is not None:
            headers["Idempotency-Key"] = key
        url = f"{self._settings.api_url.rstrip('/')}{path}"
        try:
            async with self._http.request(method, url, headers=headers, **kwargs) as response:
                text = await response.text()
                return response.status, json.loads(text) if text else None
        except (aiohttp.ClientError, OSError, TimeoutError, ValueError) as error:
            raise ProviderError(f"nomac: {method} {path}: {error}") from error

    async def _load(self) -> State:
        path = anyio.Path(self._settings.state_file)
        if not await path.exists():
            return State()
        return State.model_validate_json(await path.read_text())

    async def _save(self, state: State) -> None:
        path = anyio.Path(self._settings.state_file)
        await path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        # 0600: an entry holds a join ticket until its bootstrap job is posted.
        await anyio.to_thread.run_sync(write_private, self._settings.state_file, state.model_dump_json(indent=1))

    async def _start(self, entry: Entry) -> Session:
        status, body = await self._call(
            "POST", "/mac/sessions", key=entry.request_key, json=start_body(self._template(entry.template))
        )
        if status in {HTTPStatus.OK, HTTPStatus.CREATED, HTTPStatus.ACCEPTED}:
            return dict(body["session"])
        code = (body or {}).get("error", {}).get("code")
        if status == HTTPStatus.CONFLICT or code in {"capacity_unavailable", "session_limit_reached"}:
            raise NoCapacity(f"nomac: no Mac to start: {status} {code}")
        raise Refused(f"nomac: the start answered {status}: {body}")

    async def launch(self, template: str, tags: Mapping[str, str], user_data: str) -> Machine:
        self._template(template)
        entry = Entry(
            request_key=f"pynixd-{uuid.uuid4().hex}",
            template=template,
            tags=dict(tags),
            launched_at=time.time(),
            ticket=user_data,
        )
        async with self._lock:
            state = await self._load()
            state.entries[entry.request_key] = entry
            await self._save(state)
        try:
            session = await self._start(entry)
        except ProviderError as error:
            async with self._lock:
                state = await self._load()
                if isinstance(error, NoCapacity | Refused):
                    state.entries.pop(entry.request_key, None)
                else:
                    # The answer was lost: `machines` asks again with the same key and ends what it finds.
                    state.entries[entry.request_key].abandoned = True
                await self._save(state)
            raise
        async with self._lock:
            state = await self._load()
            state.entries[entry.request_key].session_id = str(session["id"])
            await self._save(state)
        return machine(session, entry)

    async def terminate(self, provider_id: str) -> None:
        status, body = await self._call("DELETE", f"/mac/sessions/{provider_id}", params={"confirm": "true"})
        # A session that has ended answers 404 or 410; stopping one twice costs nothing.
        if status not in {HTTPStatus.OK, HTTPStatus.ACCEPTED, HTTPStatus.NO_CONTENT, *_GONE}:
            raise ProviderError(f"nomac: stopping {provider_id} answered {status}: {body}")

    async def _bootstrap(self, entry: Entry) -> bool:
        """Post the job that makes *entry*'s ready Mac a builder; True once NoMac took it."""
        if entry.ticket is None or entry.session_id is None:
            return False
        template = self._template(entry.template)
        initiator, extra = builder_files(entry.ticket)
        status, body = await self._call(
            "POST",
            f"/mac/sessions/{entry.session_id}/jobs",
            # The same key for every try: NoMac runs a job once per key.
            key=f"{entry.request_key}-bootstrap",
            json={
                "argv": ["/bin/bash", "-c", BOOTSTRAP],
                "env": {
                    "PYNIXD_TICKET": entry.ticket,
                    "PYNIXD_INITIATOR": initiator,
                    "PYNIXD_EXTRA_CONF": extra,
                    "PYNIXD_INSTALLER": template.installer_url,
                    "PYNIXD_VOLUME": template.volume_format,
                },
                "timeout_seconds": template.bootstrap_seconds,
            },
        )
        if status not in {HTTPStatus.OK, HTTPStatus.CREATED, HTTPStatus.ACCEPTED}:
            raise ProviderError(f"nomac: the bootstrap job for {entry.session_id} answered {status}: {body}")
        return True

    async def machines(self, tags: Mapping[str, str]) -> list[Machine]:
        status, body = await self._call("GET", "/mac/sessions")
        if status != HTTPStatus.OK:
            raise ProviderError(f"nomac: listing sessions answered {status}: {body}")
        sessions = {str(one["id"]): dict(one) for one in body["sessions"]}
        async with self._lock:
            state = await self._load()
            changed = False
            for key, entry in list(state.entries.items()):
                if entry.session_id is None:
                    # The start's answer was lost: the same key gives back the session it started, if any.
                    try:
                        started = await self._start(entry)
                    except (NoCapacity, Refused):
                        state.entries.pop(key)
                        changed = True
                        continue
                    except ProviderError:
                        log.warning("nomac_start_repeat_failed", request_key=key, exc_info=True)
                        continue
                    entry.session_id = str(started["id"])
                    sessions.setdefault(entry.session_id, started)
                    changed = True
                stale = time.time() - entry.launched_at > self._settings.history_seconds
                session = sessions.get(entry.session_id)
                if session is None or (session.get("state") in _ENDED and session.get("cleanup_confirmed")):
                    if stale:
                        state.entries.pop(key)
                        changed = True
                    continue
                try:
                    if entry.abandoned and session.get("state") not in _ENDED:
                        await self.terminate(entry.session_id)
                    elif session.get("state") == "ready" and await self._bootstrap(entry):
                        entry.ticket = None
                        changed = True
                except ProviderError:
                    # The next listing tries again; the pool's `launch_timeout` bounds a Mac that never joins.
                    log.warning("nomac_session_step_failed", session=entry.session_id, exc_info=True)
            if changed:
                await self._save(state)
        found = []
        for entry in state.entries.values():
            if entry.abandoned or entry.session_id not in sessions:
                continue
            one = machine(sessions[entry.session_id], entry)
            if all(one.tags.get(key) == value for key, value in tags.items()):
                found.append(one)
        return found

    async def close(self) -> None:
        await self._http.close()
