"""`ParsedS3URL::toHttpsUrl` (`s3-url.cc:107`): the HTTP URL an `s3://` binary cache reads from."""

from __future__ import annotations

from typing import Final, Literal
from urllib.parse import urlsplit

AddressingStyle = Literal["auto", "path", "virtual"]

DEFAULT_REGION: Final = "us-east-1"
"""The region Nix signs with and builds the AWS host from when the URL names none (`s3-url.cc:111`)."""


def https_url(
    bucket: str,
    key: str,
    *,
    region: str | None = None,
    scheme: str | None = None,
    endpoint: str | None = None,
    style: AddressingStyle = "auto",
) -> str:
    """The base URL of the cache under *key* in *bucket*, with a trailing slash. `ValueError` with Nix's text.

    AWS itself takes virtual-hosted style by default; a custom *endpoint* takes path style unless *style* says
    `virtual`. An *endpoint* with a scheme is a whole URL, and its scheme wins over *scheme*.
    """
    region = DEFAULT_REGION if region is None else region
    scheme = "https" if scheme is None else scheme
    if endpoint is None:
        virtual = _virtual(bucket, style, default=True)
        host = f"{bucket}.s3.{region}.amazonaws.com" if virtual else f"s3.{region}.amazonaws.com"
        return _join(scheme, host, "", None if virtual else bucket, key)
    if "://" in endpoint:
        split = urlsplit(endpoint)
        virtual = _virtual(bucket, style, default=False)
        if virtual and not split.hostname:
            raise ValueError(
                f"cannot use virtual-hosted-style addressing with endpoint '{endpoint}' "
                "because it has no authority (hostname)"
            )
        netloc = f"{bucket}.{split.netloc}" if virtual else split.netloc
        return _join(split.scheme, netloc, split.path, None if virtual else bucket, key)
    virtual = _virtual(bucket, style, default=False)
    if virtual and not endpoint.rsplit(":", 1)[0]:
        raise ValueError(
            f"cannot use virtual-hosted-style addressing with endpoint '{endpoint}' "
            "because it has no hostname; use 'addressing-style=path' instead"
        )
    netloc = f"{bucket}.{endpoint}" if virtual else endpoint
    return _join(scheme, netloc, "", None if virtual else bucket, key)


def _virtual(bucket: str, style: AddressingStyle, *, default: bool) -> bool:
    """`useVirtualForEndpoint` (`s3-url.cc:127`): a dotted bucket breaks the TLS wildcard of a virtual host.

    Under `auto` it takes path style, and Nix warns once; `S3BinaryCacheStore` warns.
    """
    virtual = style != "path" if default else style == "virtual"
    if virtual and "." in bucket:
        if style == "virtual":
            raise ValueError(
                f"bucket name '{bucket}' contains a dot, which is incompatible with "
                "virtual-hosted-style addressing (causes TLS certificate errors); "
                "use 'addressing-style=path' or 'addressing-style=auto' instead"
            )
        return False
    return virtual


def _join(scheme: str, netloc: str, base: str, bucket: str | None, key: str) -> str:
    parts = [part for part in (base.strip("/"), bucket, key.strip("/")) if part]
    return f"{scheme}://{netloc}/" + "".join(f"{part}/" for part in parts)
