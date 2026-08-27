# Development and test identity provider only. Never deploy this.
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY libs ./libs
COPY services ./services
RUN uv sync --frozen --no-dev

FROM python:3.11-slim AS runtime
RUN useradd --system --uid 10002 --create-home appuser
WORKDIR /app
COPY --from=builder --chown=appuser:appuser /app /app
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
USER appuser
EXPOSE 8080
CMD ["uvicorn", "services.devauth.main:app", "--host", "0.0.0.0", "--port", "8080"]
