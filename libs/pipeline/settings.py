"""Typed configuration, validated at import time.

12-factor and fail-fast (ADR-0015): a missing or malformed variable stops the
process at startup with the variable named, rather than surfacing as a
confusing failure on the first request hours later. Nothing outside this module
reads os.environ.
"""
from __future__ import annotations

from functools import lru_cache

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_CONFIG = SettingsConfigDict(
    env_file=".env",
    env_file_encoding="utf-8",
    extra="ignore",
    # Aliased fields stay constructible by field name, so tests and callers can
    # build settings directly without knowing the environment variable spelling.
    populate_by_name=True,
)


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(**_CONFIG, env_prefix="KAFKA_")

    bootstrap_servers: str = "localhost:29092"

    # ADR-0004: defence in depth only. The real protection against eviction is
    # pausing the partition and continuing to poll — see pipeline.consumer.
    max_poll_interval_ms: int = 600_000
    session_timeout_ms: int = 45_000
    # One record per poll so a slow item never holds a batch of others hostage.
    max_poll_records: int = 1
    auto_offset_reset: str = "earliest"


class S3Settings(BaseSettings):
    model_config = SettingsConfigDict(**_CONFIG, env_prefix="S3_")

    endpoint: str = "http://localhost:9000"
    bucket: str = "videos"
    region: str = "us-east-1"
    # No defaults: credentials must come from the environment (ADR-0015).
    access_key: str = Field(
        validation_alias=AliasChoices("S3_ACCESS_KEY", "MINIO_ROOT_USER", "AWS_ACCESS_KEY_ID")
    )
    secret_key: str = Field(
        validation_alias=AliasChoices("S3_SECRET_KEY", "MINIO_ROOT_PASSWORD",
                                      "AWS_SECRET_ACCESS_KEY")
    )
    # Long enough for a multi-GB upload on a slow connection (ADR-0006).
    presign_put_expiry_s: int = 6 * 3600
    presign_get_expiry_s: int = 900


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(**_CONFIG)

    url: str = Field(validation_alias=AliasChoices("DATABASE_URL"))
    pool_size: int = 5


class ObservabilitySettings(BaseSettings):
    model_config = SettingsConfigDict(**_CONFIG)

    service_name: str = Field(default="unknown", validation_alias=AliasChoices("SERVICE_NAME"))
    environment: str = Field(default="development", validation_alias=AliasChoices("ENVIRONMENT"))
    log_level: str = Field(default="INFO", validation_alias=AliasChoices("LOG_LEVEL"))
    otlp_endpoint: str = Field(
        default="http://localhost:4317",
        validation_alias=AliasChoices("OTEL_EXPORTER_OTLP_ENDPOINT"),
    )
    metrics_port: int = Field(default=8000, validation_alias=AliasChoices("METRICS_PORT"))
    # Sampling is a collector concern in production; locally keep everything.
    trace_sample_ratio: float = Field(
        default=1.0, validation_alias=AliasChoices("TRACE_SAMPLE_RATIO")
    )


@lru_cache(maxsize=1)
def kafka_settings() -> KafkaSettings:
    return KafkaSettings()


@lru_cache(maxsize=1)
def s3_settings() -> S3Settings:
    return S3Settings()  # type: ignore[call-arg]  # values come from the environment


@lru_cache(maxsize=1)
def database_settings() -> DatabaseSettings:
    return DatabaseSettings()  # type: ignore[call-arg]


@lru_cache(maxsize=1)
def observability_settings() -> ObservabilitySettings:
    return ObservabilitySettings()
