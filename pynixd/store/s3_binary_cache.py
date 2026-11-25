"""A binary cache in an S3 bucket: Nix's `S3BinaryCacheStore`, HTTP with an AWS SigV4 signature on each request.

Nix 2.35 reads S3 as plain HTTP through curl (`CURLOPT_AWS_SIGV4`, `filetransfer.cc:728`) and resolves the
credentials through aws-crt (`aws-creds.cc`). Here botocore resolves them and signs; aiohttp carries the request.

With a `secret_key` the store also uploads (`upload`): the cold tier of nixidae/nixidae#1417.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

import aiohttp
import anyio
import anyio.to_thread
import structlog
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.exceptions import BotoCoreError
from botocore.session import Session
from yarl import URL

from nix_daemon_protocol.signature import Signature
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from pynixd_store.s3_url import DEFAULT_REGION
from pynixd_store.signing import PublicKey, SecretKey, check_signatures, sign_path_info

from ..exceptions import CacheFault, CacheUnopened, PathGone
from ..serde import QueryPathInfoRequest
from .binary_cache import parse_cache_info
from .daemon import DaemonStore
from .http_binary_cache import HTTPBinaryCacheStore
from .local_db import LocalDBStore
from .nar_reader import NarReader
from .s3_upload import (
    CACHE_INFO_TYPE,
    NAR_TYPE,
    NARINFO_TYPE,
    FileDigest,
    Parts,
    Uploaded,
    cache_info_text,
    complete_body,
    compressor,
    error_of,
    nar_object,
    narinfo_text,
    part_size,
    upload_id,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from botocore.credentials import Credentials

    from nix_daemon_protocol.store_path import StorePath
    from pynixd_store.builder.spec import AwsCredentials

    from ..config import S3BinaryCacheSpec
    from .base import Store

log = structlog.get_logger(__name__)

_UPLOAD_TIMEOUT: Final = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=300)
"""A stall, not a total. `CompleteMultipartUpload` answers only once the bucket joined the parts."""

_ABORT_TIMEOUT_S: Final = 30


def _canonical(url: str) -> str:
    """*url* with its path encoded as SigV4 canonicalises it: every byte but `A-Za-z0-9-._~/`, an escape kept.

    botocore signs the path as it is given, and a server signs its own encoding of it. R2 answered 403 for the build
    trace of a derivation with `+` in its name (nixidae/nixidae#1589).
    """
    parts = urlsplit(url)
    return urlunsplit(parts._replace(path=quote(parts.path, safe="/~%")))


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
    """Store over an `s3://` binary cache; it uploads when its spec names a `secret_key`."""

    def __init__(self, spec: S3BinaryCacheSpec) -> None:
        super().__init__(spec, spec.url())
        self.region = DEFAULT_REGION if spec.region is None else spec.region
        self.profile = spec.profile
        self.secret_key_file = spec.secret_key
        self.compression = spec.compression
        self.chunk_size = spec.multipart_chunk_size
        self.gc_cold = spec.gc_cold
        self.gc_cold_verify = spec.gc_cold_verify
        self._secret_key: SecretKey | None = None
        self._credentials: Credentials | None = None
        if "." in spec.bucket and spec.addressing_style == "auto" and not spec.has_endpoint:
            log.warning("s3_dotted_bucket_takes_path_style", store_id=self.store_id, bucket=spec.bucket)

    @property
    def writable(self) -> bool:
        return self.secret_key_file is not None

    @property
    def joins_every_client(self) -> bool:
        """The cold tier holds what the collector took from the local store, so every client reaches it."""
        return self.gc_cold

    def vouches_for(self, info: ValidPathInfo) -> bool:
        """NIX-DEVIATION (#27): `substitution-goal.cc:116` takes a substitute only under the client's
        `trusted-public-keys`. A cold tier's path is one pynixd signed and the collector deleted from the local
        store, so its own key admits it: a client that sends its own keys still gets back what the collector took.
        Without it that client rebuilds the path. Reverse it if the cold tier serves paths pynixd did not upload.
        """
        key = self._secret_key
        if not self.gc_cold or key is None:
            return False
        return check_signatures(info, {key.name: PublicKey(key.name, key.public_key_bytes)}, self.store_dir) > 0

    async def start(self, sync_paths: bool = True) -> None:
        if not self._started:
            # The chain may ask the instance metadata service, which blocks.
            self._credentials = await anyio.to_thread.run_sync(self._resolve_credentials)
            if self.secret_key_file is not None:
                self._secret_key = await anyio.to_thread.run_sync(SecretKey.from_file, self.secret_key_file)
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

    def _sent(self, url: str) -> URL:
        # `encoded`: yarl decodes what it need not escape, and the signature covers the encoded path.
        return URL(_canonical(url), encoded=True)

    async def _headers(self, url: str) -> Mapping[str, str]:
        if self._credentials is None:
            return {}
        return await self._signed("GET", url, b"")

    async def _signed(
        self, method: str, url: str, body: bytes, headers: Mapping[str, str] | None = None
    ) -> dict[str, str]:
        """*headers* and the SigV4 headers for *method* on *url* with *body*.

        The payload is always hashed into the signature. botocore leaves an `https` request's payload
        unsigned unless told (`S3SigV4Auth._should_sha256_sign_payload`), so moto over `http` and R2 over
        `https` would take different branches; this way both take one.
        """
        if self._credentials is None:
            raise CacheFault(f"{self.store_id} has no AWS credentials")
        # A refreshable credential refreshes here, over the network.
        frozen = await anyio.to_thread.run_sync(self._credentials.get_frozen_credentials)
        request = AWSRequest(method=method, url=_canonical(url), data=body, headers=dict(headers or {}))
        request.context["payload_signing_enabled"] = True
        S3SigV4Auth(frozen, "s3", self.region).add_auth(request)
        return dict(request.headers.items())

    async def _send(
        self, method: str, name: str, body: bytes = b"", *, query: str = "", content_type: str | None = None
    ) -> tuple[int, bytes]:
        """One signed request on the cache's file *name*: the status and the body.

        *query* is already encoded; yarl keeps a valid `%XX` as it is, so the signature covers the sent string.
        """
        url = urljoin(self.url, name) + (f"?{query}" if query else "")
        headers = {"Content-Type": content_type} if content_type is not None else {}
        signed = await self._signed(method, url, body, headers)
        session = self._require_session()
        try:
            async with session.request(
                method,
                self._sent(url),
                data=body,
                headers=signed,
                raise_for_status=False,
                timeout=_UPLOAD_TIMEOUT,
            ) as response:
                return response.status, await response.read()
        except (TimeoutError, aiohttp.ClientError, OSError) as e:
            raise CacheFault(f"{method} {url}: {e!r}") from e

    async def _expect(
        self, method: str, name: str, body: bytes = b"", *, query: str = "", content_type: str | None = None
    ) -> bytes:
        """The body of a request that must answer 2xx with no S3 `<Error>`; `CacheFault` otherwise."""
        status, answer = await self._send(method, name, body, query=query, content_type=content_type)
        error = error_of(answer)
        if not 200 <= status < 300 or error is not None:
            raise CacheFault(f"{method} {name} in '{self.store_id}': HTTP {status} {error or answer[:200]!r}")
        return answer

    async def _load_cache_info(self) -> dict[str, str]:
        """A writable cache without `nix-cache-info` gets one, as `BinaryCacheStore::init` writes it."""
        if not self.writable or self._credentials is None:
            return await super()._load_cache_info()
        try:
            status, body = await self._send("GET", "nix-cache-info")
            if status == 404:
                text = cache_info_text(self.store_dir)
                await self._expect("PUT", "nix-cache-info", text.encode(), content_type=CACHE_INFO_TYPE)
                log.info("s3_cache_info_written", store_id=self.store_id, url=self.url)
                return parse_cache_info(text, self.priority, self.store_dir)
        except CacheFault as e:
            raise CacheUnopened(str(e)) from e
        if status != 200:
            raise CacheUnopened(f"{urljoin(self.url, 'nix-cache-info')}: HTTP {status}")
        return parse_cache_info(body.decode(), self.priority, self.store_dir)

    async def upload(self, local: Store, path: StorePath) -> Uploaded:
        """Put *path* of *local* into the bucket, signed: the NAR first, the narinfo last.

        The narinfo is the commit: a client sees the path only once its NAR is whole. `UPLOADED` comes from the
        narinfo PUT's own answer, `PRESENT` from a read of the bucket, never from the narinfo cache. Its
        references are the caller's to upload first. Raises `CacheFault` when the bucket refuses.
        """
        key = self._secret_key
        if key is None:
            raise CacheFault(f"{self.store_id} has no secret_key, so it does not upload")
        if not isinstance(local, (DaemonStore, LocalDBStore)):
            raise TypeError(f"{local.store_id} serves no NARs to upload")
        name = f"{path.hash_part()}.narinfo"
        if await self.holds(path):
            log.debug("s3_upload_present", store_id=self.store_id, path=str(path))
            return Uploaded.PRESENT
        request = QueryPathInfoRequest(path=path)
        # An upload reads a GC candidate: marking it would make it fresh again.
        resp = await (local.execute(request, mark=False) if isinstance(local, LocalDBStore) else local.execute(request))
        if not resp.valid or resp.info is None:
            raise PathGone(f"{path} is not valid in {local.store_id}")
        info = ValidPathInfo(path=path, info=resp.info)
        signature = Signature(sign_path_info(key, info))
        info = ValidPathInfo(path=path, info=info.info.model_copy(update={"sigs": {*info.info.sigs, signature}}))
        url = nar_object(info, self.compression)
        digest = await self._put_nar(local, info, url)
        text = narinfo_text(info, url=url, compression=self.compression, file_hash=digest.nix32, file_size=digest.size)
        await self._expect("PUT", name, text.encode(), content_type=NARINFO_TYPE)
        self.add_path_info(info)
        log.info("s3_uploaded", store_id=self.store_id, path=str(path), nar_size=info.info.nar_size, size=digest.size)
        return Uploaded.UPLOADED

    async def holds(self, path: StorePath) -> bool:
        """Whether the bucket has *path*'s narinfo now, asked with a HEAD; `CacheFault` when it cannot say."""
        name = f"{path.hash_part()}.narinfo"
        status, _ = await self._send("HEAD", name)
        if status in (200, 404):
            return status == 200
        raise CacheFault(f"HEAD {name} in '{self.store_id}': HTTP {status}")

    async def _put_nar(self, local: DaemonStore | LocalDBStore, info: ValidPathInfo, name: str) -> FileDigest:
        """The NAR of *info*, compressed, into object *name*: one PUT when it fits a part, else a multipart upload."""
        digest = FileDigest()
        squeeze = compressor(self.compression)
        parts = Parts(part_size(info.info.nar_size, self.chunk_size))
        etags: list[str] = []
        multipart: str | None = None

        async def send(part: bytes) -> None:
            nonlocal multipart
            if multipart is None:
                created = await self._expect("POST", name, query="uploads", content_type=NAR_TYPE)
                multipart = quote(upload_id(created), safe="")
            etags.append(await self._put_part(name, multipart, len(etags) + 1, part))

        def take(chunk: bytes | None) -> list[bytes]:
            data = squeeze.flush() if chunk is None else squeeze.compress(chunk)
            digest.update(data)
            return parts.feed(data)

        try:
            async with NarReader(local) as nars:
                async for chunk in nars.read(info.path, info.info.nar_size):
                    for part in await anyio.to_thread.run_sync(take, chunk):
                        await send(part)
            for part in await anyio.to_thread.run_sync(take, None):
                await send(part)
            rest = parts.rest()
            if multipart is None:
                await self._expect("PUT", name, rest, content_type=NAR_TYPE)
                return digest
            if rest:
                await send(rest)
            await self._expect("POST", name, complete_body(etags), query=f"uploadId={multipart}")
        except BaseException:
            if multipart is not None:
                with anyio.CancelScope(shield=True), anyio.move_on_after(_ABORT_TIMEOUT_S):
                    try:
                        status, _ = await self._send("DELETE", name, query=f"uploadId={multipart}")
                    except (CacheFault, RuntimeError):
                        # The bucket keeps the parts until its lifecycle rule ends the upload. RuntimeError: a
                        # shutdown closed the store's session under the cancelled upload.
                        log.warning("s3_multipart_abort_failed", store_id=self.store_id, object=name, exc_info=True)
                    else:
                        log.warning("s3_multipart_aborted", store_id=self.store_id, object=name, status=status)
            raise
        return digest

    async def _put_part(self, name: str, multipart: str, number: int, part: bytes) -> str:
        """One `UploadPart`; its ETag, which `CompleteMultipartUpload` names it by."""
        url = urljoin(self.url, name) + f"?partNumber={number}&uploadId={multipart}"
        signed = await self._signed("PUT", url, part)
        session = self._require_session()
        try:
            async with session.put(
                self._sent(url), data=part, headers=signed, raise_for_status=False, timeout=_UPLOAD_TIMEOUT
            ) as response:
                body = await response.read()
                etag = response.headers.get("ETag")
                if response.status != 200 or etag is None:
                    raise CacheFault(
                        f"part {number} of {name} in '{self.store_id}': HTTP {response.status} {body[:200]!r}"
                    )
                return etag
        except (TimeoutError, aiohttp.ClientError, OSError) as e:
            raise CacheFault(f"part {number} of {name}: {e!r}") from e
