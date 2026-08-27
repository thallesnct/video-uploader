"""Real Kafka, Postgres and MinIO via testcontainers (ADR-0011).

Mocks cannot reproduce the behaviour this system's bugs live in — rebalances,
commit ordering, presign signatures — so the integration layer runs the real
thing, isolated per session.

The tests are deliberately synchronous and drive the app through Starlette's
TestClient. TestClient owns its event loop and runs the lifespan, so the asyncpg
engine is created and used on one loop; mixing session-scoped containers with
function-scoped async loops is the classic way to lose an afternoon here.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from collections.abc import Iterator
from typing import Any

import pytest


def _free_of_caches() -> None:
    """Settings are lru_cached; container URLs must invalidate them."""
    from pipeline import settings

    for factory in (
        settings.kafka_settings,
        settings.s3_settings,
        settings.database_settings,
        settings.observability_settings,
        settings.auth_settings,
        settings.quota_settings,
    ):
        factory.cache_clear()
    # The store caches its settings too, so it must be rebuilt against the
    # container's endpoint rather than whatever the last run configured.
    from pipeline.storage import object_store

    object_store.cache_clear()


@pytest.fixture(scope="session")
def kafka_bootstrap() -> Iterator[str]:
    from testcontainers.kafka import KafkaContainer

    with KafkaContainer() as container:
        yield container.get_bootstrap_server()


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as container:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+psycopg://"
        )


@pytest.fixture(scope="session")
def minio_endpoint() -> Iterator[tuple[str, str, str]]:
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs

    container = (
        DockerContainer("minio/minio:latest")
        .with_command("server /data")
        .with_env("MINIO_ROOT_USER", "testkey")
        .with_env("MINIO_ROOT_PASSWORD", "testsecret")
        .with_env("MINIO_API_CORS_ALLOW_ORIGIN", "http://localhost:5173")
        .with_exposed_ports(9000)
    )
    with container:
        wait_for_logs(container, "API:", timeout=60)
        endpoint = f"http://{container.get_container_host_ip()}:{container.get_exposed_port(9000)}"
        # MinIO reports ready in its logs slightly before it serves requests.
        _await_http(f"{endpoint}/minio/health/live")
        yield endpoint, "testkey", "testsecret"


def _await_http(url: str, timeout: float = 30.0) -> None:
    import urllib.error
    import urllib.request

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2):  # noqa: S310
                return
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            time.sleep(0.5)
    raise TimeoutError(f"{url} never became ready")


@pytest.fixture(scope="session")
def environment(
    kafka_bootstrap: str, postgres_url: str, minio_endpoint: tuple[str, str, str]
) -> Iterator[None]:
    endpoint, access_key, secret_key = minio_endpoint
    os.environ.update(
        {
            "KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap,
            "DATABASE_URL": postgres_url,
            "S3_ENDPOINT": endpoint,
            "S3_ACCESS_KEY": access_key,
            "S3_SECRET_KEY": secret_key,
            "S3_BUCKET": "videos",
            "OIDC_ISSUER": "http://devauth:8080",
            "OIDC_AUDIENCE": "video-pipeline",
            "SERVICE_NAME": "api-test",
            # Trace, but export nowhere: the tests assert that traceparent reaches
            # the message headers, which must not require a running collector.
            "OTEL_EXPORTER_OTLP_ENDPOINT": "",
        }
    )
    _free_of_caches()
    _create_topics(kafka_bootstrap)
    _create_bucket()
    _migrate(postgres_url)
    yield
    _free_of_caches()


def _create_topics(bootstrap: str) -> None:
    """Auto-creation is off (ADR-0002), so tests create what the registry declares."""
    from confluent_kafka.admin import AdminClient, NewTopic
    from pipeline.topics import REGISTRY

    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=30).topics)
    wanted = [
        NewTopic(plan.name, num_partitions=plan.partitions, replication_factor=1)
        for plan in REGISTRY.plan("dev")
        if plan.name not in existing
    ]
    if not wanted:
        return
    for name, future in admin.create_topics(wanted).items():
        try:
            future.result(timeout=30)
        except Exception as exc:  # already exists is fine, anything else is not
            if "already exists" not in str(exc):
                raise RuntimeError(f"could not create {name}: {exc}") from exc


def _create_bucket() -> None:
    from pipeline.storage import object_store

    store = object_store()
    try:
        store.client.create_bucket(Bucket=store.bucket)
    except Exception as exc:
        if "BucketAlreadyOwnedByYou" not in str(exc) and "BucketAlreadyExists" not in str(exc):
            raise


def _migrate(url: str) -> None:
    """Run migrations once against the container, before any test touches it.

    Invoked through the running interpreter rather than a bare `alembic`: the
    venv's bin/ is not on PATH for subprocesses, so the plain name is not found.
    """
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=False,
        env={**os.environ, "DATABASE_URL": url},
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"alembic upgrade failed:\n{result.stdout}\n{result.stderr}")


@pytest.fixture()
def client(environment: None) -> Iterator[Any]:
    from fastapi.testclient import TestClient

    from services.api.main import app

    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture(scope="session")
def mint(environment: None) -> Any:
    """Mint a real signed token for a subject, exactly as the dev issuer does."""
    from services.devauth.main import TokenRequest, issue_token

    def _mint(subject: str, **kwargs: Any) -> str:
        return str(issue_token(TokenRequest(sub=subject, **kwargs))["access_token"])

    return _mint


@pytest.fixture()
def auth(mint: Any) -> Any:
    def _auth(subject: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {mint(subject)}"}

    return _auth


@pytest.fixture(autouse=True)
def verifier_uses_the_dev_issuer_keys(environment: None, monkeypatch: Any) -> None:
    """Verify signatures against the dev issuer's JWKS without an HTTP hop.

    Real signature, issuer and audience verification — only the key *fetch* is
    short-circuited, so a token signed by another key still fails.
    """
    import jwt
    from pipeline.auth import TokenVerifier

    from services.devauth.main import jwks

    class LocalJWKClient:
        def get_signing_key_from_jwt(self, token: str) -> Any:
            header = jwt.get_unverified_header(token)
            for key in jwks()["keys"]:
                if key["kid"] == header.get("kid"):
                    return jwt.PyJWK.from_dict(key)
            raise jwt.PyJWKClientError(f"no key for kid {header.get('kid')!r}")

    original = TokenVerifier.jwk_client

    monkeypatch.setattr(TokenVerifier, "jwk_client", property(lambda self: LocalJWKClient()))
    yield
    monkeypatch.setattr(TokenVerifier, "jwk_client", original)
