"""The cold tier as a settings file spells it. nixidae/nixidae#1417."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from pynixd.config import PynixdSettings, S3BinaryCacheSpec
from pynixd.store import S3BinaryCacheStore

if TYPE_CHECKING:
    from pathlib import Path


def _cold(key: Path, **extra: object) -> dict[str, object]:
    """An R2 bucket as dynhetz's `pynixd.json` names it."""
    return {
        "type": "s3-binary-cache",
        "bucket": "dynhetz-store",
        "endpoint": "https://account.eu.r2.cloudflarestorage.com",
        "region": "auto",
        "addressing_style": "path",
        "profile": "r2",
        "secret_key": str(key),
        "gc_cold": True,
        **extra,
    }


def test_a_cold_store_parses_from_a_settings_document(tmp_path: Path) -> None:
    settings = PynixdSettings.model_validate({"stores": {"cold": _cold(tmp_path / "cold.sec")}})
    spec = settings.stores["cold"]
    assert isinstance(spec, S3BinaryCacheSpec)
    assert spec.url() == "https://account.eu.r2.cloudflarestorage.com/dynhetz-store/"
    store = spec.to_store("cold")
    assert isinstance(store, S3BinaryCacheStore)
    assert (store.gc_cold, store.region, store.profile, store.joins_every_client) == (True, "auto", "r2", True)


def test_a_cold_store_needs_a_signing_key(tmp_path: Path) -> None:
    document = _cold(tmp_path / "cold.sec")
    del document["secret_key"]
    with pytest.raises(ValidationError, match="gc_cold needs secret_key"):
        PynixdSettings.model_validate({"stores": {"cold": document}})


def test_two_cold_stores_are_refused_when_the_settings_load(tmp_path: Path) -> None:
    stores = {"a": _cold(tmp_path / "a.sec"), "b": _cold(tmp_path / "b.sec", bucket="other")}
    with pytest.raises(ValidationError, match="one gc_cold store at most, and a, b set it"):
        PynixdSettings.model_validate({"stores": stores})


def test_the_endpoint_comes_from_a_file_that_names_the_account(tmp_path: Path) -> None:
    secret = tmp_path / "endpoint"
    secret.write_text("https://account.eu.r2.cloudflarestorage.com\n")
    document = _cold(tmp_path / "cold.sec", endpoint_file=str(secret))
    del document["endpoint"]
    spec = PynixdSettings.model_validate({"stores": {"cold": document}}).stores["cold"]
    assert isinstance(spec, S3BinaryCacheSpec)
    assert "account" not in spec.model_dump_json(exclude={"settings"})
    assert spec.url() == "https://account.eu.r2.cloudflarestorage.com/dynhetz-store/"


def test_an_endpoint_and_an_endpoint_file_are_refused_together(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="endpoint_file"):
        PynixdSettings.model_validate(
            {"stores": {"cold": _cold(tmp_path / "cold.sec", endpoint_file=str(tmp_path / "endpoint"))}}
        )
