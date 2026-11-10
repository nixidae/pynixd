"""`pynixd_store.builder.fetchurl`: the parts of `curlFileTransfer` that need no transfer, and what stays on the daemon."""

from __future__ import annotations

import random

import pycurl
import pytest

from nix_daemon_protocol.basic_derivation import BasicDerivation
from nix_daemon_protocol.derivation_output import DerivationOutput
from pynixd_store.builder import fetchurl
from pynixd_store.builder.local import _mirror_urls, unsupported
from pynixd_store.builder.settings import BuildSettings
from pynixd_store.hintfmt import magenta

HEX = "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"


def _drv(url: str = "https://example.org/f", method: str = "sha256", **env: str) -> BasicDerivation:
    out = DerivationOutput(path="/nix/store/00000000000000000000000000000000-f", method=method, hash_digest=HEX)
    return BasicDerivation(
        outputs={"out": out}, platform="builtin", builder="builtin:fetchurl", env={"url": url, **env}
    )


def _settings(**config: str) -> BuildSettings:
    return BuildSettings.from_nix_config({}, {"system": "x86_64-linux", **config})


def _reason(drv: BasicDerivation, settings: BuildSettings | None = None) -> str | None:
    return unsupported(drv, settings or _settings(), store_dir="/nix/store", relocated=False)


def test_builtin_fetchurl_builds_on_the_platform_builtin() -> None:
    """`derivation-building-goal.cc:420` exempts a builtin from the platform check."""
    assert _reason(_drv()) is None


@pytest.mark.parametrize(
    ("drv", "reason"),
    [
        (_drv("s3://bucket/f"), "builtin:fetchurl of the URL scheme 's3'"),
        (_drv("ftp://example.org/f"), "builtin:fetchurl of the URL scheme 'ftp'"),
        (_drv("https://user:secret@example.org/f"), "builtin:fetchurl of a URL with credentials"),
        (_drv(__json="{}"), "builtin:fetchurl with structured attrs"),
    ],
)
def test_what_pynixd_does_not_fetch_goes_to_the_daemon(drv: BasicDerivation, reason: str) -> None:
    assert _reason(drv) == reason


def test_another_builtin_builds_here_and_the_helper_refuses_it() -> None:
    """`runChild` (`derivation-builder.cc:1007`): the builder fails with Nix's "unsupported builtin builder"."""
    drv = BasicDerivation(outputs={"out": DerivationOutput(path="/x")}, platform="builtin", builder="builtin:other")
    assert _reason(drv) is None


def test_a_plain_output_builds_here_and_the_helper_refuses_it() -> None:
    """`builtinFetchurl` throws for a derivation neither fixed nor impure (`builtins/fetchurl.cc:29`).

    With no daemon nothing else runs it, so pynixd's helper throws in Nix's words (`fetchurl.sh:90`).
    """
    drv = BasicDerivation(
        outputs={"out": DerivationOutput(path="/nix/store/00000000000000000000000000000000-f")},
        platform="builtin",
        builder="builtin:fetchurl",
        env={"url": "https://example.org/f"},
    )
    assert _reason(drv) is None


def test_hashed_mirrors_serve_a_flat_output_only() -> None:
    """`builtins/fetchurl.cc:76`: `<mirror>/<algo>/<base16>`, with a slash added where none ends the mirror."""
    settings = _settings(**{"hashed-mirrors": "http://a/ http://b"})
    assert _mirror_urls(_drv(), settings) == [f"http://a/sha256/{HEX}", f"http://b/sha256/{HEX}"]
    assert _mirror_urls(_drv(method="r:sha256"), settings) == []


def test_a_mirror_with_a_scheme_pynixd_does_not_fetch_goes_to_the_daemon() -> None:
    assert _reason(_drv(), _settings(**{"hashed-mirrors": "s3://cache"})) == "builtin:fetchurl of the URL scheme 's3'"


def test_download_attempts_is_the_old_name_of_the_tries() -> None:
    assert _settings(**{"download-attempts": "2"}).transfer.tries == 2
    assert _settings().transfer.tries == 5


@pytest.mark.parametrize(
    ("status", "code", "kind"),
    [
        (404, pycurl.E_OK, "NotFound"),
        (410, pycurl.E_OK, "NotFound"),
        (0, pycurl.E_FILE_COULDNT_READ_FILE, "NotFound"),
        (401, pycurl.E_OK, "Unauthorized"),
        (403, pycurl.E_OK, "Forbidden"),
        (400, pycurl.E_OK, "Misc"),
        (408, pycurl.E_OK, "Transient"),
        (429, pycurl.E_OK, "Transient"),
        (500, pycurl.E_OK, "Transient"),
        (501, pycurl.E_OK, "Misc"),
        (0, pycurl.E_COULDNT_CONNECT, "Transient"),
        (0, pycurl.E_UNSUPPORTED_PROTOCOL, "Misc"),
    ],
)
def test_error_kind_follows_finish(status: int, code: int, kind: str) -> None:
    """`TransferItem::finish` (`filetransfer.cc:787`)."""
    assert fetchurl.error_kind(status, code, None) == kind


def test_a_retryable_s3_code_is_transient_even_on_403() -> None:
    body = "<Error><Code>RequestTimeTooSkewed</Code></Error>"
    assert fetchurl.error_kind(403, pycurl.E_OK, body) == "Transient"


def test_the_message_of_an_http_error_carries_a_short_body() -> None:
    """`FileTransferError` (`filetransfer.cc:1471`): every `HintFmt` argument is magenta, the empty suffix too."""
    message = fetchurl.error_message("http://h/f", 404, pycurl.E_OK, "", "no such file\n")
    assert message == (
        f"unable to download '{magenta('http://h/f')}': HTTP error {magenta(404)}{magenta('')}"
        f"\n\nresponse body:\n\n{magenta('no such file')}"
    )
    assert "response body" not in fetchurl.error_message("http://h/f", 500, pycurl.E_OK, "", "x" * 2000)


def test_the_message_of_a_curl_error_names_the_code_and_the_buffer() -> None:
    message = fetchurl.error_message("http://h/f", 0, pycurl.E_COULDNT_CONNECT, "Failed to connect", None)
    assert message == (
        f"unable to download '{magenta('http://h/f')}': {magenta('Could not connect to server')} "
        f"({magenta(7)}) {magenta('Failed to connect')}"
    )


def test_the_retry_delay_doubles_up_to_its_ceiling() -> None:
    """`computeRetryDelayMs` (`filetransfer.cc:64`) without jitter, and `Retry-After` as its floor."""
    rng = random.Random(0)
    delays = [fetchurl.retry_delay_ms(n, 100, 500, None, jitter=False, rng=rng) for n in range(1, 6)]
    assert delays == [100, 200, 400, 500, 500]
    assert fetchurl.retry_delay_ms(1, 100, 500, 3000, jitter=False, rng=rng) == 3000
    jittered = fetchurl.retry_delay_ms(3, 100, 500, 1000, jitter=True, rng=rng)
    assert 1000 <= jittered <= 1400
