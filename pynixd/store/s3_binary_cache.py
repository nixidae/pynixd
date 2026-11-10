"""A binary cache in an S3 bucket: Nix's `S3BinaryCacheStore`, HTTP with an AWS SigV4 signature on each request.

Nix 2.35 reads S3 as plain HTTP through curl (`CURLOPT_AWS_SIGV4`, `filetransfer.cc:728`) and resolves the
credentials through aws-crt (`aws-creds.cc`). Here botocore resolves them and signs; aiohttp carries the request.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio.to_thread
import structlog
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.exceptions import BotoCoreError
from botocore.session import Session

from pynixd_store.s3_url import DEFAULT_REGION

from .http_binary_cache import HTTPBinaryCacheStore

if TYPE_CHECKING:
    from collections.abc import Mapping

    from botocore.credentials import Credentials

    from pynixd_store.builder.spec import AwsCredentials

    from ..config import S3BinaryCacheSpec

log = structlog.get_logger(__name__)


def resolve_aws_credentials(profile: str | None) -> AwsCredentials | None:
    """`getCredentials` (`aws-creds.cc`) for a `builtin:fetchurl` of `s3://`: botocore's chain for *profile*.

    `None` when the chain finds none or fails: Nix's parent drops the error (`derivation-builder.cc:638`). Blocks.
    """
    try:
        credentials = Session(profile=profile).get_credentials()
        frozen = None if credentials is None else credentials.get_frozen_credentials()
    except BotoCoreError as e:
        log.debug("s3_fetch_credentials_failed", profile=profile, error=str(e))
        return None
    if frozen is None:
        return None
    return {"access_key_id": frozen.access_key, "secret_access_key": frozen.secret_key, "session_token": frozen.token}


class S3BinaryCacheStore(HTTPBinaryCacheStore):
    """Read-only Store over an `s3://` binary cache."""

    def __init__(self, spec: S3BinaryCacheSpec) -> None:
        super().__init__(spec, spec.url())
        self.region = DEFAULT_REGION if spec.region is None else spec.region
        self.profile = spec.profile
        self._credentials: Credentials | None = None
        if "." in spec.bucket and spec.addressing_style == "auto" and spec.endpoint is None:
            log.warning("s3_dotted_bucket_takes_path_style", store_id=self.store_id, bucket=spec.bucket)

    async def start(self, sync_paths: bool = True) -> None:
        if not self._started:
            # The chain may ask the instance metadata service, which blocks.
            self._credentials = await anyio.to_thread.run_sync(self._resolve_credentials)
        await super().start(sync_paths)

    def _resolve_credentials(self) -> Credentials | None:
        """botocore's chain for *profile*: environment, profile files, SSO, web identity, ECS, IMDS.

        Without credentials Nix warns and sends the request unsigned (`filetransfer.cc:1309`), so a public bucket
        still answers.
        """
        try:
            credentials = Session(profile=self.profile).get_credentials()
        except BotoCoreError as e:
            log.warning("s3_credentials_failed", store_id=self.store_id, profile=self.profile, error=str(e))
            return None
        if credentials is None:
            log.warning("s3_no_credentials", store_id=self.store_id, profile=self.profile)
        return credentials

    async def _headers(self, url: str) -> Mapping[str, str]:
        if self._credentials is None:
            return {}
        # A refreshable credential refreshes here, over the network.
        frozen = await anyio.to_thread.run_sync(self._credentials.get_frozen_credentials)
        request = AWSRequest(method="GET", url=url)
        S3SigV4Auth(frozen, "s3", self.region).add_auth(request)
        return dict(request.headers.items())
