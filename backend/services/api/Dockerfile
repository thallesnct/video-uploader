# Multi-stage: dependencies resolve from the lockfile in the builder, and the
# runtime layer carries no build tooling (ADR-0015).
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Dependencies first, as their own layer: application edits must not invalidate
# the expensive install.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY libs ./libs
COPY services ./services
COPY migrations ./migrations
COPY alembic.ini ./
RUN uv sync --frozen --no-dev

FROM python:3.11-slim AS runtime

# Phase 12, ADR-0015 §8 (supply chain): pick up whatever OS-level security
# patches Debian has published since this base image tag was last built —
# caught by make security-verify's own Trivy scan finding real, fixed HIGH
# CVEs in the unpatched base (libssl3t64/openssl). Not a version bump (the
# base image tag stays python:3.11-slim, matching every other Dockerfile);
# this only pulls forward whatever the *same* Debian release has already
# fixed upstream.
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

# Same trivy scan also found two HIGH CVEs in setuptools' own *vendored*
# wheel/jaraco.context copies (usr/local/lib/python3.11/site-packages/
# setuptools/_vendor/...) — the base image's system pip/setuptools, not
# anything uv.lock or the app's own .venv controls (confirmed: the app's
# venv has neither package at all; this is the base image's pre-installed
# bootstrap tooling, unused at runtime by an already-built app, but still
# an image-scan finding worth clearing). /usr/local/bin/python3.11, not
# python3 — PATH puts the app's own .venv first.
RUN /usr/local/bin/python3.11 -m pip install --no-cache-dir --upgrade pip setuptools wheel

# Non-root, and the uid is fixed so a mounted volume's ownership is predictable.
RUN useradd --system --uid 10001 --create-home appuser

WORKDIR /app
COPY --from=builder --chown=appuser:appuser /app /app
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

USER appuser
EXPOSE 8000

# The container runs with a read-only root filesystem; anything written goes to
# the tmpfs mounted at /tmp by compose.
CMD ["uvicorn", "services.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
