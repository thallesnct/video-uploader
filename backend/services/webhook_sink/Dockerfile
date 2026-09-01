# Test double only. Never deploy this.
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
# already-built app). Upgrading in place wasn't the end of it, though:
# pip's *own* bundled vendor SBOM (pip/_vendor/bom.cdx.json) itself
# declares an outdated vendored msgpack and a stale setuptools build-
# time reference — a supply-chain lag inside pip's own release, not
# this project's uv.lock — and re-appeared as a fresh finding even
# after upgrading pip. Nothing at runtime needs this tooling at all,
# so the actual fix is to not ship it: upgrade (still needed briefly,
# to patch the base image's stale copies first), then uninstall.
# /usr/local/bin/python3.11, not python3 — PATH puts the app's own
# .venv first.
RUN /usr/local/bin/python3.11 -m pip install --no-cache-dir --upgrade pip setuptools wheel \
    && /usr/local/bin/python3.11 -m pip uninstall --yes pip setuptools wheel \
    && rm -rf /usr/local/lib/python3.11/ensurepip

RUN useradd --system --uid 10008 --create-home appuser
WORKDIR /app
COPY --from=builder --chown=appuser:appuser /app /app
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
USER appuser
EXPOSE 9100
CMD ["uvicorn", "services.webhook_sink.main:app", "--host", "0.0.0.0", "--port", "9100"]
