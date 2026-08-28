"""Configuration must fail at startup, not on the first request (ADR-0015)."""

from __future__ import annotations

import pytest
from pipeline import settings
from pydantic import ValidationError

# Read only what each test sets: the repo's own .env must not leak in and make a
# missing-variable test pass by accident.
NO_ENV_FILE = {"_env_file": None}


def test_missing_credentials_fail_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in (
        "S3_ACCESS_KEY",
        "S3_SECRET_KEY",
        "MINIO_ROOT_USER",
        "MINIO_ROOT_PASSWORD",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
    ):
        monkeypatch.delenv(var, raising=False)

    with pytest.raises(ValidationError) as excinfo:
        settings.S3Settings(**NO_ENV_FILE)

    # The message must name the environment variable, not the Python attribute:
    # an operator reading a crash log needs to know what to set.
    assert "S3_ACCESS_KEY" in str(excinfo.value)


def test_minio_credentials_satisfy_the_s3_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """Dev uses MINIO_ROOT_*, prod uses AWS_*; application code sees neither."""
    monkeypatch.delenv("S3_ACCESS_KEY", raising=False)
    monkeypatch.delenv("S3_SECRET_KEY", raising=False)
    monkeypatch.setenv("MINIO_ROOT_USER", "minioadmin")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "secret")

    resolved = settings.S3Settings(**NO_ENV_FILE)

    assert resolved.access_key == "minioadmin"
    assert resolved.secret_key == "secret"


def test_explicit_s3_keys_win_over_minio_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINIO_ROOT_USER", "minioadmin")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "secret")
    monkeypatch.setenv("S3_ACCESS_KEY", "prod-key")
    monkeypatch.setenv("S3_SECRET_KEY", "prod-secret")

    resolved = settings.S3Settings(**NO_ENV_FILE)

    assert resolved.access_key == "prod-key"


def test_database_url_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(ValidationError):
        settings.DatabaseSettings(**NO_ENV_FILE)


def test_notify_webhook_url_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fails fast at startup (ADR-0014) rather than every video silently
    notifying nowhere until someone happens to check."""
    monkeypatch.delenv("NOTIFY_WEBHOOK_URL", raising=False)
    with pytest.raises(ValidationError) as excinfo:
        settings.NotifySettings(**NO_ENV_FILE)

    assert "NOTIFY_WEBHOOK_URL" in str(excinfo.value)


def test_kafka_defaults_encode_the_adr_0004_stance() -> None:
    resolved = settings.KafkaSettings(**NO_ENV_FILE)

    # One record per poll: a slow transcode must never hold a batch hostage.
    assert resolved.max_poll_records == 1
    # Raised above the 5-minute default as defence in depth — the real fix is
    # pausing the partition while work runs off-thread.
    assert resolved.max_poll_interval_ms > 300_000


def test_presign_expiry_covers_a_slow_large_upload() -> None:
    resolved = settings.S3Settings(_env_file=None, access_key="k", secret_key="s")
    assert resolved.presign_put_expiry_s >= 3600


def test_public_endpoint_defaults_to_unset_not_to_the_internal_endpoint() -> None:
    """None means "same as endpoint" to ObjectStore (ADR-0006 follow-on) — it
    must not silently default to a copy of `endpoint`, which would hide a
    missing S3_PUBLIC_ENDPOINT in a containerized deployment instead of
    falling through to the correct same-host behavior for host-based dev."""
    resolved = settings.S3Settings(_env_file=None, access_key="k", secret_key="s")
    assert resolved.public_endpoint is None


def test_public_endpoint_is_read_from_its_own_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("S3_PUBLIC_ENDPOINT", "http://localhost:9000")
    resolved = settings.S3Settings(
        _env_file=None, access_key="k", secret_key="s", endpoint="http://minio:9000"
    )
    assert resolved.public_endpoint == "http://localhost:9000"
    assert resolved.endpoint == "http://minio:9000"
