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
