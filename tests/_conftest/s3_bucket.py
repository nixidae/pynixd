"""An S3 server in the test process that checks every signature, for the `s3://` store tests."""

from __future__ import annotations

import json
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

import botocore.session
from moto import settings as moto_settings
from moto.core.authorization import ActionAuthenticatorMixin
from moto.server import ThreadedMotoServer

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    import pytest


@dataclass(frozen=True)
class S3Server:
    endpoint: str
    read_only: tuple[str, str]
    """The access key and secret of a user that may read every bucket and write none."""


@contextmanager
def s3_bucket(
    monkeypatch: pytest.MonkeyPatch, cache_home: Path, buckets: tuple[str, ...] = ("cache",)
) -> Iterator[S3Server]:
    """An S3 server that checks each signature, *buckets*, and keys for them in the environment.

    moto checks a signature only once its first requests are spent (`INITIAL_NO_AUTH_ACTION_COUNT`): those make
    the users and their keys. A request without a valid signature then gets 403, so a pass proves pynixd signed.

    *cache_home* holds Nix's narinfo disk cache. Nix keys it by `s3://cache` alone, whatever the endpoint
    (`http-binary-cache-store.cc:76`), so with a shared one `nix copy` takes the last test's bucket for this one
    and uploads nothing (Lillecarl/nix#406).
    """
    monkeypatch.setenv("XDG_CACHE_HOME", str(cache_home))
    monkeypatch.setattr(moto_settings, "INITIAL_NO_AUTH_ACTION_COUNT", 6)
    monkeypatch.setattr(ActionAuthenticatorMixin, "request_count", 0)
    for name, value in {
        "AWS_ACCESS_KEY_ID": "bootstrap",
        "AWS_SECRET_ACCESS_KEY": "bootstrap",
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_CONFIG_FILE": "/dev/null",
        "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
    }.items():
        monkeypatch.setenv(name, value)
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"
    # moto's accounts live in the process, past the server: each bucket starts from none.
    reset = urllib.request.Request(f"{endpoint}/moto-api/reset", method="POST")
    try:
        urllib.request.urlopen(reset).close()  # noqa: S310 -- the test's own loopback server
        iam = botocore.session.get_session().create_client("iam", endpoint_url=endpoint, region_name="us-east-1")
        keys = {}
        for user, actions in {"cache": ["s3:*"], "reader": ["s3:GetObject", "s3:ListBucket"]}.items():
            iam.create_user(UserName=user)
            statement = {"Effect": "Allow", "Action": actions, "Resource": "*"}
            iam.put_user_policy(
                UserName=user,
                PolicyName=user,
                PolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": [statement]}),
            )
            keys[user] = iam.create_access_key(UserName=user)["AccessKey"]
        key = keys["cache"]
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", key["AccessKeyId"])
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", key["SecretAccessKey"])
        s3 = botocore.session.get_session().create_client("s3", endpoint_url=endpoint, region_name="us-east-1")
        for bucket in buckets:
            s3.create_bucket(Bucket=bucket)
        yield S3Server(endpoint, (keys["reader"]["AccessKeyId"], keys["reader"]["SecretAccessKey"]))
    finally:
        urllib.request.urlopen(reset).close()  # noqa: S310 -- as above
        server.stop()
