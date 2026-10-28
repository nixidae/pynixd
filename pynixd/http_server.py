# pyright: reportAttributeAccessIssue=false
"""Unified HTTP server for pynixd.

Provides:
- Nix binary cache protocol (narinfo, NAR streaming, uploads)
- Prometheus metrics endpoint (/metrics)
- Kubernetes health check endpoint (/healthz)

Supports granular enabling of features (cache vs metrics) and
optional basic auth/TLS.
"""

from __future__ import annotations

import base64
import binascii
import bz2
import contextlib
import gzip
import lzma
import ssl
import time
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING

import anyio
import brotli
import lz4.frame
import structlog
import zstandard as zstd
from aiohttp import web
from anyio.lowlevel import checkpoint
from anyio.to_thread import run_sync
from passlib.apache import HtpasswdFile

from nix_daemon_protocol.store_dir import store_dir
from nix_daemon_protocol.valid_path_info import ValidPathInfo

from . import metrics
from .serde import (
    AddToStoreNarRequest,
    NarFromPathRequest,
    QueryPathFromHashPartRequest,
    QueryPathInfoRequest,
)
from .serde.context import ReadContext, WriteContext
from .store_path import StorePath
from .wire import _CHUNK_SIZE

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from .health import HealthReport
    from .store import LocalStore
    from .substitution_queue import SubstitutionQueue
    from .wire import NixWriter

log = structlog.get_logger(__name__)


def _rewrite_narinfo_url(raw: str, path_hash: str) -> str:
    """Point an upstream `.narinfo` at this cache's own `/nar` endpoint.

    Every line passes through untouched except `URL:`, which becomes the
    path-hash filename `handle_nar` resolves and redirects. The hashes
    and signatures stay the upstream's, so a client verifies the same
    bytes it would have fetched there. Issue #85.
    """
    rewritten = [f"URL: nar/{path_hash}.nar" if line.startswith("URL: ") else line for line in raw.splitlines()]
    return "\n".join(rewritten) + "\n"


class PynixdHttpServer:
    """Unified HTTP server for pynixd.

    Supports reading and (optionally) writing paths via standard protocol,
    and exposing Prometheus metrics.
    """

    def __init__(
        self,
        local_store: LocalStore,
        *,
        enable_cache: bool = True,
        enable_metrics: bool = True,
        metrics_no_auth: bool = True,
        username: str | None = None,
        password: str | None = None,
        htpasswd_path: str | Path | None = None,
        priority: int = 30,
        upload_dir: str | Path | None = None,
        health_check: Callable[[], HealthReport] | None = None,
        substitution_queue: SubstitutionQueue | None = None,
        upstream_race: bool = True,
        prefer_local_nar: bool = True,
    ) -> None:
        """Initialize the HTTP server and register route handlers.

        Args:
            local_store: Backing store for path resolution and NAR streaming.
            enable_cache: Whether to serve binary cache endpoints.
            enable_metrics: Whether to expose a Prometheus ``/metrics`` endpoint.
            metrics_no_auth: Allow unauthenticated access to ``/metrics``.
            username: Basic auth username (or ``None`` to disable auth).
            password: Basic auth password.
            htpasswd_path: Path to an htpasswd file (overrides username/password).
            priority: Cache priority advertised in ``/nix-cache-info``.
            upload_dir: Directory for staging NAR uploads (``None`` disables uploads).
            health_check: Returns the server's own verdict for ``/healthz``.
                ``None`` leaves the endpoint answering 200 unconditionally,
                which is what a cache started without a Server does.
            substitution_queue: Races the HTTP substituters on a local
                cache miss. ``None`` keeps every miss a 404. Issue #85.
            upstream_race: Whether a local miss may ask the substituters.
            prefer_local_nar: Serve a locally held NAR instead of
                redirecting to the upstream that also holds it.
        """
        self._health_check = health_check
        self.store = local_store
        self.substitution_queue = substitution_queue
        self.upstream_race = upstream_race
        self.prefer_local_nar = prefer_local_nar
        self.enable_cache = enable_cache
        self.enable_metrics = enable_metrics
        self.metrics_no_auth = metrics_no_auth
        self.username = username
        self.password = password
        self.htpasswd = None
        if htpasswd_path:
            self.htpasswd = HtpasswdFile(str(htpasswd_path))
        self.priority = priority
        self.upload_dir = Path(upload_dir) if upload_dir else None
        if self.upload_dir:
            self.upload_dir.mkdir(parents=True, exist_ok=True)

        # aiohttp applies the last middleware innermost, so this one wraps the
        # auth middleware and counts a rejected request as well as a served one.
        self.app = web.Application(
            middlewares=[self.metrics_middleware, self.auth_middleware],
            client_max_size=1024**4,  # 1 TiB
        )

        self.app.router.add_get("/healthz", self.handle_healthz)

        if self.enable_metrics:
            self.app.router.add_get("/metrics", self.handle_metrics)

        if self.enable_cache:
            self.app.router.add_get("/nix-cache-info", self.handle_cache_info)
            self.app.router.add_get("/{hash}.narinfo", self.handle_narinfo)
            self.app.router.add_get("/nar/{filename:.+}", self.handle_nar)

            if self.upload_dir:
                self.app.router.add_put("/nar/{filename:.+}", self.handle_put_nar)
                self.app.router.add_put("/{hash}.narinfo", self.handle_put_narinfo)

    # ── Middleware ─────────────────────────────────────────────────────

    @web.middleware
    async def metrics_middleware(
        self,
        request: web.Request,
        handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
    ) -> web.StreamResponse:
        """Count and time every request, by the route pattern it matched."""
        started = time.monotonic()
        status = "500"
        try:
            response = await handler(request)
        except web.HTTPException as exc:
            status = str(exc.status)
            raise
        else:
            status = str(response.status)
            return response
        finally:
            resource = request.match_info.route.resource
            # A request that matched no route reports one series, and not one
            # per path somebody probed.
            route = resource.canonical if resource is not None else "unmatched"
            metrics.HTTP_REQUESTS.labels(route=route, method=request.method, status=status).inc()
            metrics.HTTP_REQUEST_DURATION.labels(route=route, method=request.method).observe(
                time.monotonic() - started,
            )

    @web.middleware
    async def auth_middleware(
        self,
        request: web.Request,
        handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
    ) -> web.StreamResponse:
        """Enforce HTTP Basic Auth on all endpoints except healthz and optionally metrics."""
        # Health checks and metrics bypass auth
        if request.path == "/healthz":
            return await handler(request)
        if self.enable_metrics and self.metrics_no_auth and request.path == "/metrics":
            return await handler(request)

        if self.username is None and self.htpasswd is None:
            return await handler(request)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Basic "):
            return web.Response(
                status=HTTPStatus.UNAUTHORIZED,
                headers={"WWW-Authenticate": 'Basic realm="nix-cache"'},
                text="Authentication required\n",
            )

        try:
            auth_decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
            user, passwd = auth_decoded.split(":", 1)
        except (ValueError, UnicodeDecodeError, binascii.Error):
            return web.Response(
                status=HTTPStatus.UNAUTHORIZED,
                text="Malformed credentials\n",
            )

        if self.htpasswd:
            if not self.htpasswd.check_password(user, passwd):
                return web.Response(
                    status=HTTPStatus.FORBIDDEN,
                    text="Invalid credentials\n",
                )
        elif self.username is not None and (user != self.username or passwd != self.password):
            return web.Response(
                status=HTTPStatus.FORBIDDEN,
                text="Invalid credentials\n",
            )

        return await handler(request)

    # ── Handlers ──────────────────────────────────────────────────────

    async def handle_healthz(self, request: web.Request) -> web.Response:
        """Kubernetes health check endpoint.

        Answers 503 with the failing check named, so a probe's own response
        body says why rather than sending the reader to a log. With no health
        callback wired it reports 200 and says so, because a server that cannot
        assess itself must not claim the interfaces are up.
        """
        if self._health_check is None:
            return web.Response(status=HTTPStatus.OK, text="ok\nhealth: not assessed\n")

        report = self._health_check()
        status = HTTPStatus.OK if report.ok else HTTPStatus.SERVICE_UNAVAILABLE
        return web.Response(status=status, text=report.as_text())

    async def handle_metrics(self, request: web.Request) -> web.Response:
        """Expose Prometheus metrics."""
        body, content_type = metrics.get_metrics_response()
        # aiohttp: charset must not be in content_type argument
        content_type_only = content_type.split(";")[0]
        return web.Response(body=body, content_type=content_type_only)

    async def handle_cache_info(self, request: web.Request) -> web.Response:
        """Serve the ``/nix-cache-info`` endpoint with store metadata."""
        lines = [
            f"StoreDir: {store_dir()}",
            "WantMassQuery: 1",
            f"Priority: {self.priority}",
        ]
        return web.Response(text="\n".join(lines) + "\n")

    async def handle_narinfo(self, request: web.Request) -> web.Response:
        """Serve a ``.narinfo`` file for a given store path hash."""
        hash_part = request.match_info["hash"]

        # Resolve hash → full path
        path = await self.resolve_path(hash_part)
        if path is None:
            return await self.handle_upstream_narinfo(hash_part)

        # Get path info
        vinfo = await self.get_path_info(path)
        if vinfo is None:
            return await self.handle_upstream_narinfo(hash_part)

        narinfo = vinfo.to_narinfo()
        return web.Response(
            text=narinfo,
            content_type="text/x-nix-narinfo",
        )

    async def handle_upstream_narinfo(self, hash_part: str) -> web.Response:
        """Answer a local `.narinfo` miss from the fastest upstream.

        The winner's text goes out with only the URL line rewritten to
        this cache, so the client fetches the NAR here and the redirect
        decision stays with `handle_nar`. The losing requests are not
        cancelled: they land in the substitution caches and rank the
        later redirect by priority. Issue #85.
        """
        if not self.upstream_race or self.substitution_queue is None:
            return web.Response(status=HTTPStatus.NOT_FOUND, text="not found\n")

        winner = await self.substitution_queue.race_http_narinfo(hash_part)
        if winner is None:
            return web.Response(status=HTTPStatus.NOT_FOUND, text="not found\n")

        log.info(
            "http_cache_narinfo_upstream_hit",
            hash=hash_part,
            store_id=winner.store.store_id,
        )
        return web.Response(
            text=_rewrite_narinfo_url(winner.raw, winner.info.path.hash_part()),
            content_type="text/x-nix-narinfo",
        )

    async def handle_nar(self, request: web.Request) -> web.StreamResponse:
        """Stream raw NAR data for a given store path."""
        filename = request.match_info["filename"]
        # Standard format is /nar/<hash>.nar[.comp]
        hash_part = filename.split(".", 1)[0]

        if not filename.endswith(".nar"):
            # We only serve raw NARs. If Nix asks for .xz or other, we return 404
            # so it might fall back to .nar if it wants.
            return web.Response(
                status=HTTPStatus.NOT_FOUND,
                text="compression not supported\n",
            )

        path = await self.resolve_path(hash_part)
        if path is None:
            return await self.handle_upstream_nar(hash_part)

        # Get path info for NAR size (needed for Content-Length and streaming)
        vinfo = await self.get_path_info(path)
        if vinfo is None:
            return await self.handle_upstream_nar(hash_part)

        if not self.prefer_local_nar:
            redirect = await self.redirect_to_upstream(hash_part)
            if redirect is not None:
                return redirect

        response = web.StreamResponse(
            status=HTTPStatus.OK,
            headers={
                "Content-Type": "application/x-nix-nar",
                "Content-Length": str(vinfo.info.nar_size),
            },
        )
        await response.prepare(request)

        try:
            async with self.store.transfer_conn() as conn:
                await NarFromPathRequest(path=StorePath(path=str(path))).to_writer(WriteContext.from_conn(conn))
                await conn.w.drain()
                await conn.r.drain_stderr()
                remaining = vinfo.info.nar_size
                while remaining > 0:
                    chunk = await conn.r.readexactly(min(remaining, _CHUNK_SIZE))
                    await response.write(chunk)
                    # aiohttp's `write` awaits its payload writer, so this loop
                    # has backpressure. It still needs a guaranteed suspension:
                    # the daemon read is served from a buffer and the write
                    # returns below the high-water mark, so neither reaches the
                    # event loop. See `wire.forward_raw`.
                    await checkpoint()
                    remaining -= len(chunk)
                    metrics.HTTP_NAR_BYTES_SENT.inc(len(chunk))
        except Exception:
            log.exception("nar_from_path_streaming_failed", path=path)
            # Response already started — can't change status code.
            # We close the connection abruptly to signal failure.
            # aiohttp: force-close the underlying transport.
            if request.protocol and request.protocol.transport:
                request.protocol.transport.close()
            raise

        await response.write_eof()
        return response

    async def handle_upstream_nar(self, hash_part: str) -> web.Response:
        """Redirect a NAR the local store does not hold.

        The target is the highest-priority upstream that answered for the
        hash, not the fastest: the race already happened for the
        `.narinfo`, so this ranks its results instead of re-racing.
        A 307 keeps the GET a GET at the upstream. Issue #85.
        """
        redirect = await self.redirect_to_upstream(hash_part)
        if redirect is not None:
            return redirect
        return web.Response(status=HTTPStatus.NOT_FOUND, text="not found\n")

    async def redirect_to_upstream(self, hash_part: str) -> web.Response | None:
        """Highest-priority upstream URL for one NAR hash, if any answered."""
        if not self.upstream_race or self.substitution_queue is None:
            return None
        url = await self.substitution_queue.upstream_nar_redirect(hash_part)
        if url is None:
            return None
        log.info("http_cache_nar_redirect", hash=hash_part, url=url)
        metrics.HTTP_CACHE_UPSTREAM.labels(result="redirect").inc()
        return web.HTTPTemporaryRedirect(location=url)

    async def handle_put_nar(self, request: web.Request) -> web.Response:
        """Receive NAR data and save it to a temporary file."""
        if not self.upload_dir:
            return web.Response(
                status=HTTPStatus.METHOD_NOT_ALLOWED,
                text="Upload disabled\n",
            )

        filename = request.match_info["filename"]
        # Standard format is /nar/<hash>.nar[.comp]
        # Strip all extensions to get the hash part
        hash_part = filename.split(".", 1)[0]

        # Determine compression from extension
        ext = ""
        for known_ext in [".xz", ".bz2", ".gz", ".zst", ".lz4", ".br"]:
            if filename.endswith(known_ext):
                ext = known_ext
                break

        temp_path = self.upload_dir / f"{hash_part}.nar{ext}"

        if ".nar" not in filename:
            log.warning("invalid_nar_upload_name", filename=filename)
            return web.Response(
                status=HTTPStatus.BAD_REQUEST,
                text="Filename must contain .nar\n",
            )

        log.info(
            "receiving_nar_upload",
            hash=hash_part,
            path=str(temp_path),
            filename=filename,
        )
        # Use anyio.open_file for async file writing to keep event loop non-blocking
        async with await anyio.open_file(temp_path, "wb") as f:
            async for chunk in request.content.iter_any():
                await f.write(chunk)
                metrics.HTTP_NAR_BYTES_RECEIVED.inc(len(chunk))

        nar_size = (await anyio.Path(temp_path).stat()).st_size
        log.info("nar_upload_complete", hash=hash_part, size=nar_size)
        return web.Response(status=HTTPStatus.OK, text="ok\n")

    async def handle_put_narinfo(self, request: web.Request) -> web.Response:
        """Receive .narinfo, parse it, and finalize the upload to the Nix store."""
        if not self.upload_dir:
            return web.Response(
                status=HTTPStatus.METHOD_NOT_ALLOWED,
                text="Upload disabled\n",
            )

        content = await request.text()

        try:
            vinfo = ValidPathInfo.from_narinfo(content)
        except (ValueError, KeyError, IndexError) as e:
            log.warning("invalid_narinfo_upload", error=str(e))
            return web.Response(
                status=HTTPStatus.BAD_REQUEST,
                text=f"Invalid .narinfo: {e}\n",
            )

        # Determine the NAR filename from the 'URL' field in .narinfo
        nar_url = ""
        for line in content.splitlines():
            if line.startswith("URL: "):
                nar_url = line.split(": ", 1)[1].strip()
                break

        if not nar_url:
            return web.Response(
                status=HTTPStatus.BAD_REQUEST,
                text="Missing 'URL' field in .narinfo\n",
            )

        # nar_url is usually "nar/<narhash>.nar[.comp]"
        nar_filename = nar_url.split("/")[-1]
        nar_hash_part = nar_filename.split(".", 1)[0]

        # Check for any compressed variant
        nar_temp_path = None
        for ext in ["", ".xz", ".bz2", ".gz", ".zst", ".lz4", ".br"]:
            p = self.upload_dir / f"{nar_hash_part}.nar{ext}"
            if p.exists():
                nar_temp_path = p
                break

        if not nar_temp_path:
            log.warning(
                "nar_missing_for_narinfo",
                nar_hash=nar_hash_part,
                path=str(vinfo.path),
            )
            return web.Response(
                status=HTTPStatus.NOT_FOUND,
                text=f"NAR {nar_hash_part} not found. Upload it first.\n",
            )

        # Now add it to the store
        async def provide_nar(writer: NixWriter):
            log.debug("provide_nar_start", path=nar_temp_path)
            # We need to wrap the raw NAR in Nix framing (framed NAR data)
            framed = writer.framed()

            def decompress_gen(path: Path):
                if path.name.endswith(".xz"):
                    ctx = lzma.open(path, "rb")  # noqa: SIM115 -- the `with ctx` below closes it
                elif path.name.endswith(".bz2"):
                    ctx = bz2.open(path, "rb")  # noqa: SIM115 -- the `with ctx` below closes it
                elif path.name.endswith(".gz"):
                    ctx = gzip.open(path, "rb")  # noqa: SIM115 -- the `with ctx` below closes it
                elif path.name.endswith(".zst"):
                    dctx = zstd.ZstdDecompressor()
                    ctx = dctx.stream_reader(path.open("rb"))
                elif path.name.endswith(".lz4"):
                    ctx = lz4.frame.open(path, "rb")
                elif path.name.endswith(".br"):
                    d = brotli.Decompressor()
                    with path.open("rb") as bf:
                        while True:
                            chunk = bf.read(1024 * 1024)
                            if not chunk:
                                break
                            yield d.process(chunk)
                    return
                else:
                    ctx = path.open("rb")

                with ctx as f:
                    while True:
                        chunk = f.read(1024 * 1024)
                        if not chunk:
                            break
                        yield chunk

            gen = decompress_gen(nar_temp_path)

            def get_next_chunk(g):
                try:
                    return next(g)
                except StopIteration:
                    return None

            sent_bytes = 0
            while True:
                # Get next chunk from generator in a thread to keep event loop free
                chunk = await run_sync(get_next_chunk, gen)
                if chunk is None:
                    break

                # Ensure chunk is bytes for type safety
                if not isinstance(chunk, bytes):
                    raise TypeError(f"Expected bytes, got {type(chunk)}")

                framed.write(chunk)
                # Backpressure. Without this the transport holds every byte the
                # daemon has not taken, which is the whole NAR for an upload
                # that outruns it. `run_sync` above suspends, so the loop does
                # get scheduled -- the buffer was the fault here, not the
                # yield. See `wire.forward_framed`.
                await framed.drain()
                sent_bytes += len(chunk)
                log.debug("provide_nar_progress", sent_bytes=sent_bytes)

            log.debug("provide_nar_finalizing", total_bytes=sent_bytes)
            await framed.finalize()
            log.debug("provide_nar_done")

        log.info("finalizing_upload_to_store", path=str(vinfo.path))
        try:
            async with self.store.transfer_conn() as conn:
                req = AddToStoreNarRequest(info=vinfo, repair=0, dont_check_sigs=0)
                await req.to_writer(WriteContext.from_conn(conn))
                await conn.w.drain()
                await provide_nar(conn.w)
                await conn.w.drain()
                await req.response_type.from_reader(ReadContext.from_conn(conn))
        except Exception as e:
            log.exception("finalize_upload_failed", path=str(vinfo.path))
            return web.Response(
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
                text=f"Finalize failed: {e}\n",
            )
        finally:
            # Clean up temporary NAR
            with contextlib.suppress(OSError):
                nar_temp_path.unlink()

        self.store.add_path_info(vinfo)
        log.info("upload_to_store_complete", path=str(vinfo.path))
        return web.Response(status=HTTPStatus.OK, text="ok\n")

    # ── Path resolution helpers ───────────────────────────────────────

    async def resolve_path(self, hash_part: str) -> StorePath | None:
        """Resolve a store hash or NAR hash to a full store path."""
        # 1. Try resolving as a NAR hash (SHA256, 64 chars) if we have a DB
        if len(hash_part) == 64 and (db := getattr(self.store, "db", None)) is not None:
            for prefix in ["sha256:", ""]:
                full_hash = f"{prefix}{hash_part}"
                async with db.execute(
                    "SELECT path FROM ValidPaths WHERE hash = ?",
                    (full_hash,),
                ) as cursor:
                    row = await cursor.fetchone()
                if row:
                    return StorePath(row[0])

        # 2. Fall back to standard QueryPathFromHashPart
        resp = await self.store.execute(
            QueryPathFromHashPartRequest(path=hash_part),
        )
        if resp.value:
            return StorePath(str(resp.value))

        return None

    async def get_path_info(self, path: StorePath) -> ValidPathInfo | None:
        resp = await self.store.execute(QueryPathInfoRequest(path=path))
        if resp.valid and resp.info:
            return ValidPathInfo(path=path, info=resp.info)
        return None

    # ── Lifecycle ─────────────────────────────────────────────────────

    async def start(
        self,
        host: str = "",
        port: int = 8080,
        ssl_cert: str | None = None,
        ssl_key: str | None = None,
    ) -> tuple[web.AppRunner, int]:
        """Start the HTTP server. Returns (runner, bound_port)."""
        ssl_ctx = None
        if ssl_cert and ssl_key:
            ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ssl_ctx.load_cert_chain(ssl_cert, ssl_key)
            scheme = "https"
        else:
            scheme = "http"

        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, host, port, ssl_context=ssl_ctx)
        await site.start()

        # Resolve the actual bound port (important when port=0)
        # types-aiohttp don't declare sockets on AbstractServer (added in Python 3.7)
        bound_port = site._server.sockets[0].getsockname()[1]  # type: ignore[reportAttributeAccessIssue]
        log.info(
            "pynixd_http_server_listening",
            scheme=scheme,
            host=host,
            port=bound_port,
            enable_cache=self.enable_cache,
            enable_metrics=self.enable_metrics,
        )
        return runner, bound_port


# Backward compatibility alias
BinaryCacheServer = PynixdHttpServer
