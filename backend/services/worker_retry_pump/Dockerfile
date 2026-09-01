# No ffmpeg, no DB, no S3 — this only ever moves messages between topics.
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY libs ./libs
COPY services ./services
RUN uv sync --frozen --no-dev

FROM python:3.11-slim AS runtime

# Phase 12, ADR-0015 §8: pick up whatever OS-level security patches Debian
# has published since this base image tag was last built — same fix as
# api/Dockerfile's, caught by make security-verify's own Trivy scan.
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

# Same trivy scan also found HIGH CVEs in the base image's system
# pip/setuptools bootstrap tooling (unused at runtime by an
# already-built app, but still an image-scan finding worth clearing).
# /usr/local/bin/python3.11, not python3 — PATH puts the app's own
# .venv first.
RUN /usr/local/bin/python3.11 -m pip install --no-cache-dir --upgrade pip setuptools wheel

RUN useradd --system --uid 10010 --create-home appuser

WORKDIR /app
COPY --from=builder --chown=appuser:appuser /app /app
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

USER appuser

CMD ["python", "-m", "services.worker_retry_pump.main"]
