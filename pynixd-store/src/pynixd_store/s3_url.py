"""`ParsedS3URL` (`s3-url.cc`): an `s3://` URL, and the HTTP URL it reads from (`toHttpsUrl`, `s3-url.cc:107`)."""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from typing import Final, Literal, cast, get_args
from urllib.parse import parse_qsl, quote, urlsplit

from .hintfmt import magenta

AddressingStyle = Literal["auto", "path", "virtual"]

DEFAULT_REGION: Final = "us-east-1"
"""The region Nix signs with and builds the AWS host from when the URL names none (`s3-url.cc:111`)."""


class S3URLError(ValueError):
    """Nix's error: `str()` is the plain text, *hint* the text with each argument magenta, as `HintFmt` shows it.

    *traces* are the `addTrace` hints, outermost first.
    """

    def __init__(self, fmt: str, *args: object, traces: tuple[str, ...] = ()) -> None:
        super().__init__(fmt.format(*args))
        self.hint = fmt.format(*(magenta(arg) for arg in args))
        self.traces = traces


@dataclass(frozen=True)
class ParsedS3URL:
    bucket: str
    key: str
    """The path after the bucket, its slashes kept: Nix keeps each segment, the empty ones too."""
    profile: str | None = None
    region: str | None = None
    scheme: str | None = None
    version_id: str | None = None
    style: AddressingStyle = "auto"
    endpoint: str | None = None


def parse(url: str) -> ParsedS3URL:
    """`ParsedS3URL::parse` (`s3-url.cc:44`); `S3URLError` with Nix's text and its trace."""
    split = urlsplit(url)
    host = split.netloc.rpartition("@")[2]
    if not host.startswith("["):
        host = host.partition(":")[0]
    query: dict[str, str] = {}
    # `decodeQuery` keeps the first of a repeated parameter.
    for name, value in parse_qsl(split.query, keep_blank_values=True):
        query.setdefault(name, value)
    trace = (f"while parsing S3 URI: '{magenta(url)}'",)
    # A registered name, not an IP literal (`s3-url.cc:55`).
    if not host or host.startswith("[") or _is_ipv4(host):
        raise S3URLError("URI has a missing or invalid bucket name", traces=trace)
    if not split.path.startswith("/"):
        raise S3URLError("URI has a missing or invalid key", traces=trace)
    style = query.get("addressing-style", "auto")
    if style not in get_args(AddressingStyle):
        raise S3URLError("unknown S3 addressing style '{}', expected 'auto', 'path', or 'virtual'", style, traces=trace)
    return ParsedS3URL(
        bucket=host,
        key=split.path[1:],
        profile=query.get("profile"),
        region=query.get("region"),
        scheme=query.get("scheme"),
        version_id=query.get("versionId"),
        style=cast("AddressingStyle", style),
        endpoint=query.get("endpoint"),
    )


def _is_ipv4(host: str) -> bool:
    try:
        ipaddress.IPv4Address(host)
    except ValueError:
        return False
    return True


def object_url(url: ParsedS3URL) -> str:
    """The URL of the object *url* names. `S3URLError` with Nix's text for an address Nix refuses."""
    scheme, netloc, parts = _location(url.bucket, url.region, url.scheme, url.endpoint, url.style)
    query = "" if url.version_id is None else f"?versionId={quote(url.version_id, safe='')}"
    return f"{scheme}://{netloc}/" + "".join(f"{part}/" for part in parts) + url.key + query


def falls_back_to_path_style(url: ParsedS3URL) -> bool:
    """Whether `toHttpsUrl` warns that a dotted bucket takes path style (`s3-url.cc:138`)."""
    return "." in url.bucket and url.style == "auto" and url.endpoint is None


def https_url(
    bucket: str,
    key: str,
    *,
    region: str | None = None,
    scheme: str | None = None,
    endpoint: str | None = None,
    style: AddressingStyle = "auto",
) -> str:
    """The base URL of the cache under *key* in *bucket*, with a trailing slash. `S3URLError` with Nix's text.

    AWS itself takes virtual-hosted style by default; a custom *endpoint* takes path style unless *style* says
    `virtual`. An *endpoint* with a scheme is a whole URL, and its scheme wins over *scheme*.
    """
    scheme, netloc, parts = _location(bucket, region, scheme, endpoint, style)
    parts = [*parts, key.strip("/")]
    return f"{scheme}://{netloc}/" + "".join(f"{part}/" for part in parts if part)


def _location(
    bucket: str, region: str | None, scheme: str | None, endpoint: str | None, style: AddressingStyle
) -> tuple[str, str, list[str]]:
    """The scheme, the authority and the path segments before the key."""
    region = DEFAULT_REGION if region is None else region
    scheme = "https" if scheme is None else scheme
    if endpoint is None:
        virtual = _virtual(bucket, style, default=True)
        host = f"{bucket}.s3.{region}.amazonaws.com" if virtual else f"s3.{region}.amazonaws.com"
        return scheme, host, [] if virtual else [bucket]
    if "://" in endpoint:
        split = urlsplit(endpoint)
        virtual = _virtual(bucket, style, default=False)
        if virtual and not split.hostname:
            raise S3URLError(
                "cannot use virtual-hosted-style addressing with endpoint '{}' because it has no authority (hostname)",
                endpoint,
            )
        netloc = f"{bucket}.{split.netloc}" if virtual else split.netloc
        base = [part for part in (split.path.strip("/"),) if part]
        return split.scheme, netloc, base if virtual else [*base, bucket]
    virtual = _virtual(bucket, style, default=False)
    if virtual and not endpoint.rsplit(":", 1)[0]:
        raise S3URLError(
            "cannot use virtual-hosted-style addressing with endpoint '{}' "
            "because it has no hostname; use 'addressing-style=path' instead",
            endpoint,
        )
    return scheme, f"{bucket}.{endpoint}" if virtual else endpoint, [] if virtual else [bucket]


def _virtual(bucket: str, style: AddressingStyle, *, default: bool) -> bool:
    """`useVirtualForEndpoint` (`s3-url.cc:127`): a dotted bucket breaks the TLS wildcard of a virtual host.

    Under `auto` it takes path style, and Nix warns once (`falls_back_to_path_style`).
    """
    virtual = style != "path" if default else style == "virtual"
    if virtual and "." in bucket:
        if style == "virtual":
            raise S3URLError(
                "bucket name '{}' contains a dot, which is incompatible with "
                "virtual-hosted-style addressing (causes TLS certificate errors); "
                "use 'addressing-style=path' or 'addressing-style=auto' instead",
                bucket,
            )
        return False
    return virtual
