"""EC2 spot instances as a provider: builders on AWS. nixidae/nixidae#1198.

botocore resolves the credentials, serializes each call, signs it and parses
the answer; aiohttp carries the request, so no call blocks the loop. A machine
boots a stock image, and its user data installs pynixd from the installer
tarball and registers it as a builder through the join ticket.

Every resource a launch makes (instance, volume, network interface, spot
request) carries the launch's tags plus `resource_tags`, so one tag finds
everything to remove. A launch makes nothing that outlives its instance.
"""

from __future__ import annotations

import base64
import hashlib
import json
from typing import TYPE_CHECKING, Any, Final

import aiohttp
import anyio.to_thread
import structlog
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.exceptions import BotoCoreError
from botocore.parsers import create_parser
from botocore.serialize import create_serializer
from botocore.session import Session

from .provider import Machine, MachineState, NoCapacity, NotFound, ProviderError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from botocore.credentials import Credentials
    from botocore.model import ServiceModel

    from ..config import Ec2Settings, Ec2TemplateSettings

log = structlog.get_logger(__name__)

type Wire = dict[str, Any]
"""A call's parameters or answer, in botocore's shape: the service model types it, not Python."""

_STATES: Final = {
    "pending": MachineState.PENDING,
    "running": MachineState.RUNNING,
    "shutting-down": MachineState.ENDED,
    "terminated": MachineState.ENDED,
    "stopping": MachineState.ENDED,
    "stopped": MachineState.ENDED,
}

NO_CAPACITY: Final = frozenset(
    {
        "InsufficientInstanceCapacity",
        "InsufficientCapacity",
        "InsufficientHostCapacity",
        "MaxSpotInstanceCountExceeded",
        "SpotMaxPriceTooLow",
        "VcpuLimitExceeded",
        "InsufficientFreeAddressesInSubnet",
        # The instance type does not exist in the subnet's zone.
        "Unsupported",
    }
)
"""Error codes that say "not this type now": the launch tries the template's next instance type."""

SPOT_NOTICE_SECONDS: Final = 120.0
"""EC2 gives a spot instance two minutes from `marked-for-termination` to its end."""

TICKET_PATH: Final = "/etc/pynixd/join-ticket.json"

_TIMEOUT: Final = aiohttp.ClientTimeout(total=60)


class ApiError(ProviderError):
    """EC2 answered with an error code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


def machine(instance: Mapping[str, Any], notice: float | None = None) -> Machine:  # noqa: ANN401 -- botocore's parsed Instance
    """A machine from DescribeInstances' or RunInstances' `Instance`."""
    reason: Mapping[str, Any] = instance.get("StateReason") or {}
    return Machine(
        provider_id=str(instance["InstanceId"]),
        state=_STATES.get(str(instance["State"]["Name"]), MachineState.ENDED),
        tags={str(tag["Key"]): str(tag["Value"]) for tag in instance.get("Tags") or []},
        notice=notice,
        error=str(reason["Message"]) if reason.get("Message") else None,
    )


def notice_of(request: Mapping[str, Any]) -> float | None:  # noqa: ANN401 -- botocore's parsed SpotInstanceRequest
    """When a spot request's instance ends, from its status, or `None` when nothing ends it."""
    status: Mapping[str, Any] = request.get("Status") or {}
    if status.get("Code") != "marked-for-termination":
        return None
    return float(status["UpdateTime"].timestamp()) + SPOT_NOTICE_SECONDS


def bootstrap(ticket: str, template: Ec2TemplateSettings) -> str:
    """The user data: a Python script cloud-init runs as root on the stock image.

    It never prints the ticket: cloud-init's output reaches the console, which the API serves.
    A failure, or a set-up that outlasts `bootstrap_deadline`, powers the machine off, so a
    machine that never registers costs no more than that.
    """
    return f"""#!/usr/bin/env python3
import json, os, shutil, subprocess, sys, urllib.request

TICKET = {ticket!r}
URL = {template.installer!r}
IDLE = {template.idle_poweroff!r}
DEADLINE = {template.bootstrap_deadline!r}


def poweroff():
    subprocess.run(["systemctl", "poweroff"], check=False)


subprocess.run(
    ["systemd-run", "--unit=pynixd-bootstrap-deadline", f"--on-active={{DEADLINE:.0f}}s", "systemctl", "poweroff"],
    check=True,
)
try:
    ticket = json.loads(TICKET)
    os.makedirs("/etc/pynixd", exist_ok=True)
    fd = os.open({TICKET_PATH!r}, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as out:
        out.write(TICKET)
    initiator = {{"join_ticket": {TICKET_PATH!r}, "system_features": ticket["system_features"]}}
    if ticket.get("max_jobs") is not None:
        initiator["max_jobs"] = ticket["max_jobs"]
    with open("/etc/pynixd/reverse-initiator.json", "w") as out:
        json.dump(initiator, out)
    with open("/etc/pynixd/extra-nix.conf", "w") as out:
        if ticket["system_features"]:
            out.write("extra-system-features = " + " ".join(ticket["system_features"]) + "\\n")
    unzstd = subprocess.Popen(["zstd", "-dc"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    untar = subprocess.Popen(["tar", "-x", "-C", "/"], stdin=unzstd.stdout)
    unzstd.stdout.close()
    with urllib.request.urlopen(URL, timeout=60) as response:
        shutil.copyfileobj(response, unzstd.stdin)
    unzstd.stdin.close()
    if unzstd.wait() or untar.wait():
        raise RuntimeError("unpacking the installer failed")
    subprocess.run(
        [
            "/nix/.pynixd/install",
            "--reverse-initiator", "/etc/pynixd/reverse-initiator.json",
            "--extra-conf", "/etc/pynixd/extra-nix.conf",
            "--idle-poweroff", str(IDLE),
        ],
        check=True,
    )
    subprocess.run(["systemctl", "stop", "pynixd-bootstrap-deadline.timer"], check=False)
except BaseException as error:
    print(f"pynixd-bootstrap: {{type(error).__name__}}, powering off", file=sys.stderr)
    poweroff()
    raise
"""


def run_params(
    template: Ec2TemplateSettings, instance_type: str, tags: Mapping[str, str], user_data: str, token: str
) -> Wire:
    """RunInstances' parameters for one spot instance of *instance_type*."""
    tag_list = [{"Key": key, "Value": value} for key, value in sorted(tags.items())]
    params: Wire = {
        "ImageId": template.image_id,
        "InstanceType": instance_type,
        "MinCount": 1,
        "MaxCount": 1,
        "ClientToken": token,
        "UserData": base64.b64encode(user_data.encode()).decode(),
        "InstanceInitiatedShutdownBehavior": "terminate",
        "InstanceMarketOptions": {
            "MarketType": "spot",
            "SpotOptions": {"SpotInstanceType": "one-time", "InstanceInterruptionBehavior": "terminate"},
        },
        "BlockDeviceMappings": [
            {
                "DeviceName": template.root_device,
                "Ebs": {"VolumeSize": template.root_gib, "VolumeType": "gp3", "DeleteOnTermination": True},
            }
        ],
        "MetadataOptions": {"HttpTokens": "required"},
        "TagSpecifications": [
            {"ResourceType": kind, "Tags": tag_list}
            for kind in ("instance", "volume", "network-interface", "spot-instances-request")
        ],
    }
    if template.subnet_id is not None:
        params["SubnetId"] = template.subnet_id
    if template.security_group_ids:
        params["SecurityGroupIds"] = list(template.security_group_ids)
    return params


def client_token(tags: Mapping[str, str], instance_type: str) -> str:
    """RunInstances' idempotency token: a launch's tags are unique to it, so a retry starts no second machine.

    One per type: EC2 refuses a token reused with other parameters.
    """
    text = json.dumps([sorted(tags.items()), instance_type])
    return hashlib.sha256(text.encode()).hexdigest()[:64]


def tag_filters(tags: Mapping[str, str]) -> list[Wire]:
    return [{"Name": f"tag:{key}", "Values": [value]} for key, value in sorted(tags.items())]


class Ec2Provider:
    name = "ec2"

    def __init__(self, settings: Ec2Settings) -> None:
        self._settings = settings
        self._session = Session(profile=settings.profile)
        self._model: ServiceModel = self._session.get_service_model("ec2")
        self._serializer = create_serializer(self._model.metadata["protocol"])
        self._parser = create_parser(self._model.metadata["protocol"])
        self._endpoint = settings.endpoint or f"https://ec2.{settings.region}.amazonaws.com/"
        self._credentials: Credentials | None = None
        self._http = aiohttp.ClientSession(timeout=_TIMEOUT)

    async def _frozen(self) -> Any:  # noqa: ANN401 -- botocore's ReadOnlyCredentials, untyped in its stubs
        if self._credentials is None:
            # The chain may read files or ask the instance metadata service, which blocks.
            self._credentials = await anyio.to_thread.run_sync(self._session.get_credentials)
            if self._credentials is None:
                raise ProviderError(f"ec2: no AWS credentials for profile {self._settings.profile!r}")
        # A refreshable credential refreshes here, over the network.
        return await anyio.to_thread.run_sync(self._credentials.get_frozen_credentials)

    async def call(self, operation: str, params: Wire) -> Wire:
        """One EC2 API call: *params* and the answer in botocore's shape."""
        op = self._model.operation_model(operation)
        try:
            wire = self._serializer.serialize_to_request(params, op)
            request = AWSRequest(method="POST", url=self._endpoint, data=wire["body"], headers=wire["headers"])
            SigV4Auth(await self._frozen(), "ec2", self._settings.region).add_auth(request)
            prepared = request.prepare()
            async with self._http.post(self._endpoint, data=prepared.body, headers=dict(prepared.headers)) as response:
                body = await response.read()
                status = response.status
                headers = dict(response.headers)
        except (aiohttp.ClientError, OSError, TimeoutError, BotoCoreError) as error:
            raise ProviderError(f"ec2: {operation}: {error}") from error
        parsed: Wire = self._parser.parse({"status_code": status, "headers": headers, "body": body}, op.output_shape)
        if "Error" in parsed and parsed["Error"].get("Code"):
            raise ApiError(str(parsed["Error"]["Code"]), str(parsed["Error"].get("Message", "")))
        return parsed

    def _template(self, name: str) -> Ec2TemplateSettings:
        try:
            return self._settings.templates[name]
        except KeyError:
            raise NotFound(f"ec2: no template {name!r}") from None

    async def launch(self, template: str, tags: Mapping[str, str], user_data: str) -> Machine:
        spec = self._template(template)
        all_tags = {**self._settings.resource_tags, **tags}
        script = bootstrap(user_data, spec)
        refused: list[str] = []
        for instance_type in spec.instance_types:
            token = client_token(tags, instance_type)
            try:
                answer = await self.call("RunInstances", run_params(spec, instance_type, all_tags, script, token))
            except ApiError as error:
                if error.code not in NO_CAPACITY:
                    raise
                log.info("ec2_no_capacity", instance_type=instance_type, code=error.code)
                refused.append(f"{instance_type}: {error}")
                continue
            return machine(answer["Instances"][0])
        raise NoCapacity("; ".join(refused))

    async def terminate(self, provider_id: str) -> None:
        try:
            await self.call("TerminateInstances", {"InstanceIds": [provider_id]})
        except ApiError as error:
            # An instance EC2 no longer knows costs nothing.
            if error.code not in {"InvalidInstanceID.NotFound", "InvalidInstanceID.Malformed"}:
                raise

    async def machines(self, tags: Mapping[str, str]) -> list[Machine]:
        instances = await self._paged(
            "DescribeInstances",
            {"Filters": tag_filters(tags)},
            lambda page: [one for reservation in page["Reservations"] for one in reservation["Instances"]],
        )
        spot_ids = [str(one["SpotInstanceRequestId"]) for one in instances if one.get("SpotInstanceRequestId")]
        notices = await self._notices(spot_ids)
        return [machine(one, notices.get(str(one.get("SpotInstanceRequestId")))) for one in instances]

    async def _notices(self, request_ids: Sequence[str]) -> dict[str, float]:
        if not request_ids:
            return {}
        requests = await self._paged(
            "DescribeSpotInstanceRequests",
            {"SpotInstanceRequestIds": list(request_ids)},
            lambda page: list(page["SpotInstanceRequests"]),
        )
        notices = {}
        for request in requests:
            notice = notice_of(request)
            if notice is not None:
                notices[str(request["SpotInstanceRequestId"])] = notice
        return notices

    async def _paged(self, operation: str, params: Wire, items: Any) -> list[Wire]:  # noqa: ANN401 -- a page -> its items
        found: list[Wire] = []
        token: str | None = None
        while True:
            page = await self.call(operation, {**params, **({"NextToken": token} if token else {})})
            found.extend(items(page))
            token = page.get("NextToken")
            if not token:
                return found

    async def close(self) -> None:
        await self._http.close()
