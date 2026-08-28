# No ffmpeg needed — master.m3u8 is text, written straight to Postgres/S3.
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY libs ./libs
COPY services ./services
RUN uv sync --frozen --no-dev

FROM python:3.11-slim AS runtime

RUN useradd --system --uid 10007 --create-home appuser

WORKDIR /app
COPY --from=builder --chown=appuser:appuser /app /app
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

USER appuser

CMD ["python", "-m", "services.worker_package.main"]
