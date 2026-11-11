"""`builtin:fetchurl` (`builtins/fetchurl.cc`) in the sandbox helper, over the libcurl that Nix uses.

The helper runs this inside the chroot as the sandbox user, as `runChild` runs a
builtin (`unix/build/derivation-builder.cc:994`). The transfer is a port of
`curlFileTransfer::TransferItem` (`filetransfer.cc`): its curl options, its
error classes, its retries and its messages. Its log is Nix's JSON logger
(`logging.cc:226`), which the parent reads as `@nix` lines.

Every module this one needs loads at import: inside the chroot no library or
Python file is left to load.
"""

from __future__ import annotations

import ctypes
import json
import lzma
import os
import random
import re
import time
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pycurl

from .. import s3_url
from ..hintfmt import ANSI_NORMAL, magenta, show_error
from ..nar_fs import restore_path

if TYPE_CHECKING:
    from collections.abc import Callable

    from .spec import AwsCredentials, FetchSpec

ACT_FILE_TRANSFER = 101
RES_PROGRESS = 105
LVL_WARN = 1
LVL_TALKATIVE = 4
ANSI_WARNING = "\x1b[35;1m"
"""`ANSI_WARNING` (`terminal.hh`): Nix's warnings are magenta."""

#: `successfulStatuses` (`filetransfer.cc:218`); 0 is a protocol with no HTTP status.
SUCCESS = frozenset({200, 201, 204, 206, 304, 0})

#: The curl codes `finish` (`filetransfer.cc:852`) gives up on at once.
HOPELESS = frozenset(
    {
        pycurl.E_FAILED_INIT,
        pycurl.E_URL_MALFORMAT,
        pycurl.E_NOT_BUILT_IN,
        pycurl.E_REMOTE_ACCESS_DENIED,
        pycurl.E_FILE_COULDNT_READ_FILE,
        pycurl.E_FUNCTION_NOT_FOUND,
        pycurl.E_ABORTED_BY_CALLBACK,
        pycurl.E_BAD_FUNCTION_ARGUMENT,
        pycurl.E_INTERFACE_FAILED,
        pycurl.E_UNKNOWN_OPTION,
        pycurl.E_SSL_CACERT_BADFILE,
        pycurl.E_TOO_MANY_REDIRECTS,
        pycurl.E_WRITE_ERROR,
        pycurl.E_UNSUPPORTED_PROTOCOL,
        pycurl.E_BAD_CONTENT_ENCODING,
    }
)

#: S3's retryable XML error codes (`filetransfer.cc:794`).
S3_RETRYABLE = frozenset(
    {
        "IncompleteBody",
        "InternalError",
        "InternalFailure",
        "InternalServerError",
        "RequestExpired",
        "RequestTimeout",
        "RequestTimeTooSkewed",
        "RequestThrottled",
        "SlowDown",
        "ServiceUnavailable",
        "Throttling",
        "ThrottledException",
    }
)
S3_ERROR_CODE = re.compile(r"<Error>.*?<Code>([^<]+)</Code>", re.DOTALL)
STATUS_LINE = re.compile(r"HTTP/[^ ]+ +([0-9]+)(.*)", re.IGNORECASE | re.DOTALL)

#: The libcurl that pycurl loaded: dlopen by soname answers the loaded copy.
_libcurl = ctypes.CDLL("libcurl.so.4")
_libcurl.curl_easy_strerror.restype = ctypes.c_char_p
_libcurl.curl_easy_strerror.argtypes = [ctypes.c_int]
_libc = ctypes.CDLL(None, use_errno=True)


class TransferError(Exception):
    """`FileTransferError`: *kind* is `FileTransfer::Error`, and the text is the message."""

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind
        self.hint = message
        self.traces: tuple[str, ...] = ()


#: A `nix::Error`: `builtinFetchurl` tries the next mirror after one, and `writeFile` adds its trace to one.
NixError = (TransferError, s3_url.S3URLError)

#: `warnOnce` (`s3-url.cc:146`) warns once in a process, and the helper is one process for one build.
_warned_dotted_bucket = False


def preload_nss() -> None:
    """`preloadNSS` (`globals.cc:537`): resolve hosts through files and DNS, fixed before the chroot.

    In the chroot glibc can load no NSS module, and nscd is out of reach.
    """
    try:
        ctypes.CDLL("libnss_dns.so.2", mode=os.RTLD_NOW)
    except OSError:
        log_message(LVL_WARN, warning("unable to load nss_dns backend"))
    _libc.__nss_configure_lookup(b"hosts", b"files dns")


def warning(text: str) -> str:
    """`Logger::warn` (`logging.cc:43`)."""
    return f"{ANSI_WARNING}warning:{ANSI_NORMAL} {text}"


def _write(event: dict[str, object]) -> None:
    """`JSONLogger::write`: nlohmann's object is a sorted map, dumped compact and not ASCII-escaped."""
    line = "@nix " + json.dumps(event, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
    os.write(2, line.encode(errors="replace"))


def log_message(level: int, text: str) -> None:
    _write({"action": "msg", "level": level, "msg": text})


def strerror(code: int) -> str:
    message: bytes = _libcurl.curl_easy_strerror(code)
    return message.decode()


def retry_delay_ms(
    attempt: int, base_ms: int, ceil_ms: int, retry_after_ms: int | None, *, jitter: bool, rng: random.Random
) -> int:
    """`computeRetryDelayMs` (`filetransfer.cc:64`) over `clampedExponential` (`filetransfer-impl.hh:23`)."""
    shift = min(attempt - 1 if attempt else 0, 31)
    backoff = min(base_ms << shift, ceil_ms)
    floor = retry_after_ms or 0
    if not jitter:
        return max(floor, backoff)
    ceiling = min(floor + backoff, 2**32 - 1)
    if ceiling <= floor:
        return floor
    return rng.randint(floor, ceiling)


def error_kind(status: int, code: int, body: str | None) -> str:
    """The `FileTransfer::Error` of a failed transfer (`filetransfer.cc:787`)."""
    s3 = None
    if status in (400, 403, 500, 503) and body is not None and (match := S3_ERROR_CODE.search(body)):
        s3 = match.group(1)
    if s3 in S3_RETRYABLE:
        return "Transient"
    if status in (404, 410) or code == pycurl.E_FILE_COULDNT_READ_FILE:
        return "NotFound"
    if status in (401, 407):
        return "Unauthorized"
    if status == 403:
        return "Forbidden"
    if 400 <= status < 500 and status not in (408, 429):
        return "Misc"
    if status in (501, 505, 511):
        return "Misc"
    if code in HOPELESS:
        return "Misc"
    return "Transient"


def display_uri(url: str) -> str:
    """`FileTransferRequest::displayUri` (`filetransfer.cc:1280`): *url* with no user or password in its authority."""
    scheme, sep, rest = url.partition("://")
    if not sep:
        return url
    end = min((i for i in (rest.find(c) for c in "/?#") if i >= 0), default=len(rest))
    authority = rest[:end]
    if "@" not in authority:
        return url
    return f"{scheme}://{authority.rpartition('@')[2]}{rest[end:]}"


def error_message(url: str, status: int, code: int, errbuf: str, body: str | None) -> str:
    """The text of the `FileTransferError` that `finish` throws, and its response body (`filetransfer.cc:1471`)."""
    if status:
        suffix = "" if code == pycurl.E_OK else f" (curl error: {strerror(code)})"
        hint = f"unable to download '{magenta(url)}': HTTP error {magenta(status)}{magenta(suffix)}"
    else:
        hint = f"unable to download '{magenta(url)}': {magenta(strerror(code))} ({magenta(code)}) {magenta(errbuf)}"
    if body is not None and (len(body) < 1024 or "<html>" in body):
        return f"{hint}\n\nresponse body:\n\n{magenta(body.rstrip(' \n\r\t'))}"
    return hint


def _retry_after_ms(value: str) -> int | None:
    """`Retry-After`: seconds, or an HTTP date (`filetransfer.cc:440`)."""
    if value.isdigit():
        return min(int(value) * 1000, 2**32 - 1)
    try:
        date = parsedate_to_datetime(value).timestamp()
    except (TypeError, ValueError):
        return None
    return min(max(int(date - time.time()), 0) * 1000, 2**32 - 1)


class _S3Signing:
    """How curl signs a request to S3 (`filetransfer.cc:1302`)."""

    def __init__(self, region: str, credentials: AwsCredentials | None) -> None:
        self.provider = f"aws:amz:{region}:s3"
        self.credentials = credentials


def _for_s3(spec: FetchSpec, url: str) -> tuple[str, _S3Signing]:
    """`setupForS3` (`filetransfer.cc:1294`): the HTTP URL of *url*, and its signature."""
    global _warned_dotted_bucket  # noqa: PLW0603 -- `warnOnce`'s static flag
    parsed = s3_url.parse(url)
    https = s3_url.object_url(parsed)
    if s3_url.falls_back_to_path_style(parsed) and not _warned_dotted_bucket:
        _warned_dotted_bucket = True
        log_message(
            LVL_WARN,
            warning(
                f"bucket name '{magenta(parsed.bucket)}' contains a dot; falling back to path-style addressing "
                "(virtual-hosted-style requires non-dotted bucket names for TLS certificate validity); "
                "set 'addressing-style=path' to silence this warning"
            ),
        )
    region = s3_url.DEFAULT_REGION if parsed.region is None else parsed.region
    return https, _S3Signing(region, spec["aws_credentials"].get(url))


class _Transfer:
    """One `TransferItem`: a GET of *url* into *sink*, retried as `maybeRetry` allows."""

    def __init__(
        self, spec: FetchSpec, url: str, sink: Callable[[bytes], object], s3: _S3Signing | None = None
    ) -> None:
        self.spec = spec
        self.url = url
        self.s3 = s3
        #: What every message names: Nix never shows the credentials.
        self.display = display_uri(url)
        self.sink = sink
        self.curl = pycurl.Curl()
        self.attempt = 0
        self.written = 0
        self.activity: int | None = None
        self.callback_error: BaseException | None = None
        self.error_body: bytearray | None = None
        self.accept_ranges = False
        self.content_encoding = False
        self.retry_after_ms: int | None = None
        # pycurl refuses `getinfo` while `perform` runs: the callbacks read the status line instead.
        self.response_code = 0

    def _status(self) -> int:
        """`getHTTPStatus`: the response code of HTTP and HTTPS, and 0 for other protocols."""
        effective: str = self.curl.getinfo(pycurl.EFFECTIVE_URL)
        if effective.split(":", 1)[0].lower() not in ("http", "https"):
            return 0
        code: int = self.curl.getinfo(pycurl.RESPONSE_CODE)
        return code

    def _act(self) -> None:
        """`act()`: the activity starts with the transfer, not when it is queued."""
        if self.activity is None:
            self.activity = (1 << 32) + 1
            _write(
                {
                    "action": "start",
                    "id": self.activity,
                    "level": LVL_TALKATIVE,
                    "type": ACT_FILE_TRANSFER,
                    "text": f"downloading '{self.display}'",
                    "parent": 0,
                    "fields": [self.display],
                }
            )

    def _progress(self, done: int, expected: int) -> None:
        self._act()
        _write({"action": "result", "id": self.activity, "type": RES_PROGRESS, "fields": [done, expected, 0, 0]})

    def stop(self) -> None:
        if self.activity is not None:
            _write({"action": "stop", "id": self.activity})

    def _on_header(self, raw: bytes) -> None:
        try:
            line = raw.decode("latin-1")
            if match := STATUS_LINE.fullmatch(line):
                self.response_code = int(match.group(1))
                self.accept_ranges = False
                self.content_encoding = False
                self.retry_after_ms = None
                return
            name, sep, value = line.partition(":")
            if not sep:
                return
            name, value = name.strip().lower(), value.strip()
            if name == "content-encoding":
                self.content_encoding = value.lower() not in ("", "identity")
            elif name == "accept-ranges" and value.lower() == "bytes":
                self.accept_ranges = True
            elif name == "retry-after":
                self.retry_after_ms = _retry_after_ms(value)
        except BaseException as e:  # noqa: BLE001 -- `callbackException`: kept, then raised after `perform`
            self.callback_error = e
            return

    def _on_data(self, data: bytes) -> int:
        try:
            if self.error_body is None and self.response_code not in SUCCESS:
                self.error_body = bytearray()
            if self.error_body is not None:
                self.error_body.extend(data)
            if self.response_code in SUCCESS:
                self.written += len(data)
                self.sink(data)
        except BaseException as e:  # noqa: BLE001 -- `callbackException`: kept, then raised after `perform`
            self.callback_error = e
            return 0
        return len(data)

    def _on_progress(self, dltotal: int, dlnow: int, _ultotal: int, _ulnow: int) -> int:
        try:
            self._progress(dlnow, dltotal)
        except BaseException as e:  # noqa: BLE001 -- `callbackException`: kept, then raised after `perform`
            self.callback_error = e
            return 1
        return 0

    def _on_resolver_start(self) -> int:
        try:
            self._act()
        except BaseException as e:  # noqa: BLE001 -- `callbackException`: kept, then raised after `perform`
            self.callback_error = e
            return 1
        return 0

    def _setup(self) -> None:
        """`TransferItem::init` (`filetransfer.cc:587`), the options that a GET with no body sets."""
        c, spec = self.curl, self.spec
        c.reset()
        c.setopt(pycurl.URL, self.url)
        if self.written == 0:
            c.setopt(pycurl.ACCEPT_ENCODING, "")
        c.setopt(pycurl.FOLLOWLOCATION, 1)
        c.setopt(pycurl.MAXREDIRS, 10)
        c.setopt(pycurl.NOSIGNAL, 1)
        suffix = f" {spec['user_agent_suffix']}" if spec["user_agent_suffix"] else ""
        # `LIBCURL_VERSION` of Nix's build; pycurl loads the same libcurl.
        c.setopt(pycurl.USERAGENT, f"curl/{pycurl.version_info()[1]} Nix/{spec['nix_version']}{suffix}")
        c.setopt(pycurl.PIPEWAIT, 1)
        if spec["http3"] and pycurl.version_info()[4] & pycurl.VERSION_HTTP3:
            c.setopt(pycurl.HTTP_VERSION, pycurl.CURL_HTTP_VERSION_3)
        elif spec["http2"]:
            c.setopt(pycurl.HTTP_VERSION, pycurl.CURL_HTTP_VERSION_2TLS)
        else:
            c.setopt(pycurl.HTTP_VERSION, pycurl.CURL_HTTP_VERSION_1_1)
        c.setopt(pycurl.WRITEFUNCTION, self._on_data)
        c.setopt(pycurl.HEADERFUNCTION, self._on_header)
        c.setopt(pycurl.XFERINFOFUNCTION, self._on_progress)
        c.setopt(pycurl.NOPROGRESS, 0)
        if spec["download_speed"]:
            c.setopt(pycurl.MAX_RECV_SPEED_LARGE, spec["download_speed"] * 1024)
        c.setopt(pycurl.CAINFO, spec["ca_file"])
        c.setopt(pycurl.CONNECTTIMEOUT, spec["connect_timeout"])
        c.setopt(pycurl.TCP_KEEPALIVE, 1)
        c.setopt(pycurl.TCP_KEEPIDLE, 30)
        c.setopt(pycurl.TCP_KEEPINTVL, 30)
        c.setopt(pycurl.MAXAGE_CONN, 90)
        c.setopt(pycurl.LOW_SPEED_LIMIT, 1)
        c.setopt(pycurl.LOW_SPEED_TIME, spec["stalled_download_timeout"])
        c.setopt(pycurl.NETRC_FILE, spec["netrc_file"])
        c.setopt(pycurl.NETRC, pycurl.NETRC_OPTIONAL)
        if self.written:
            c.setopt(pycurl.RESUME_FROM_LARGE, self.written)
        c.setopt(pycurl.RESOLVER_START_FUNCTION, self._on_resolver_start)
        if self.s3 is not None:
            credentials = self.s3.credentials
            if credentials is not None:
                c.setopt(pycurl.USERNAME, credentials["access_key_id"])
                c.setopt(pycurl.PASSWORD, credentials["secret_access_key"])
                if credentials["session_token"] is not None:
                    c.setopt(pycurl.HTTPHEADER, [f"x-amz-security-token: {credentials['session_token']}"])
            c.setopt(pycurl.AWS_SIGV4, self.s3.provider)

    def run(self, rng: random.Random) -> None:
        spec = self.spec
        while True:
            self._setup()
            self.callback_error = None
            self.response_code = 0
            code, errbuf = pycurl.E_OK, ""
            try:
                self.curl.perform()
            except pycurl.error as e:
                code, errbuf = e.args
            status = self._status()
            if self.callback_error is not None:
                raise self.callback_error
            if code == pycurl.E_OK and status in SUCCESS:
                size: int = self.curl.getinfo(pycurl.SIZE_DOWNLOAD_T)
                self._progress(size, size)
                return
            body = self.error_body.decode(errors="replace") if self.error_body is not None else None
            kind = error_kind(status, code, body)
            self.attempt += 1
            message = error_message(self.display, status, code, errbuf, body)
            base = spec["retry_delay_rate_limited_ms"] if status in (429, 503) else spec["retry_delay_ms"]
            retry = kind == "Transient" and self.attempt < spec["tries"]
            if retry and self.written:
                retry = self.accept_ranges and not self.content_encoding
            if not retry:
                raise TransferError(kind, message)
            delay = retry_delay_ms(
                self.attempt,
                base,
                spec["retry_max_delay_ms"],
                self.retry_after_ms,
                jitter=spec["retry_jitter"],
                rng=rng,
            )
            self.retry_after_ms = None
            # `maybeRetry` prints the hint's plain text: `warn` formats with boost, not `HintFmt`.
            if self.written:
                text = f"{message}; retrying from offset {self.written} in {delay} ms (attempt {self.attempt}/{spec['tries']})"
            else:
                text = f"{message}; retrying in {delay} ms (attempt {self.attempt}/{spec['tries']})"
            log_message(LVL_WARN, warning(text))
            self.error_body = None
            time.sleep(delay / 1000)


def _download(spec: FetchSpec, url: str, sink: Callable[[bytes], object], rng: random.Random) -> None:
    s3 = None
    if url.partition("://")[0].lower() == "s3":
        url, s3 = _for_s3(spec, url)
    transfer = _Transfer(spec, url, sink, s3)
    try:
        transfer.run(rng)
    finally:
        transfer.stop()
        transfer.curl.close()


def _fetch(spec: FetchSpec, out: Path, url: str, rng: random.Random) -> None:
    """The `fetch` lambda of `builtinFetchurl`: the download, then the output in place."""
    if spec["unpack"]:
        # The NAR goes to a file first: `restorePath` pulls and curl pushes. A partial file is no output.
        nar = Path(spec["tmp_dir"]) / "fetchurl.nar"
        decompressor = lzma.LZMADecompressor() if (spec["main_url"] or "").endswith(".xz") else None
        with nar.open("wb") as file:

            def write(data: bytes) -> None:
                file.write(decompressor.decompress(data) if decompressor is not None else data)

            _download(spec, url, write, rng)
            if decompressor is not None and not decompressor.eof:
                raise ValueError("xz stream ended early")
        with nar.open("rb") as reader:
            restore_path(out, reader.read)
        nar.unlink()
    else:
        fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_CLOEXEC, 0o666)
        with open(fd, "wb", closefd=True) as file:
            try:
                _download(spec, url, file.write, rng)
            except NixError as e:
                # `writeFile(path, Source &)` (`file-system.cc:315`); `PathFmt` quotes the path.
                e.traces = (f"writing file {magenta(chr(34) + str(out) + chr(34))}", *e.traces)
                raise
    if spec["executable"]:
        out.chmod(0o755)


def run(spec: FetchSpec) -> None:
    """`builtinFetchurl` (`builtins/fetchurl.cc:10`), after `enterChroot`. Exits the process."""
    try:
        tmp = Path(spec["tmp_dir"])
        if spec["netrc_data"]:
            spec["netrc_file"] = str(tmp / "netrc")
            _write_private(Path(spec["netrc_file"]), spec["netrc_data"])
        spec["ca_file"] = str(tmp / "ca-certificates.crt")
        _write_private(Path(spec["ca_file"]), spec["ca_data"])
        if spec["out"] is None:
            raise RuntimeError("'builtin:fetchurl' requires an 'out' output")
        out = Path(spec["out"])
        if not spec["fixed_or_impure"]:
            raise RuntimeError("'builtin:fetchurl' must be a fixed-output or impure derivation")
        main_url = spec["main_url"]
        if main_url is None:
            # `std::map::at` throws `std::out_of_range`, no `nix::Error`: `runChild` writes its `what()` alone.
            os.write(2, b"map::at\n")
            os._exit(1)
        rng = random.Random()
        for url in spec["mirrors"]:
            try:
                _fetch(spec, out, url, rng)
            except (*NixError, OSError):
                continue
            os._exit(0)
        _fetch(spec, out, main_url, rng)
    except Exception as e:  # noqa: BLE001 -- `runChild` writes any `std::exception` as the build's error
        text, traces = (e.hint, e.traces) if isinstance(e, NixError) else (str(e), ())
        os.write(2, (show_error(text, traces) + "\n").encode(errors="replace"))
        os._exit(1)
    os._exit(0)


def _write_private(path: Path, data: str) -> None:
    """`writeFile(path, data, 0600)`."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_CLOEXEC, 0o600)
    with open(fd, "w", encoding="utf-8", errors="surrogateescape", closefd=True) as file:
        file.write(data)
