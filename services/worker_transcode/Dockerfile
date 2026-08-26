# ffmpeg lives only in media worker images (ADR-0011). Structure mirrors
# worker_probe/Dockerfile: one ffmpeg-base stage shared by the shipped image and
# the test image, so tests exercise the same binary production uses.
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY libs ./libs
COPY services ./services
RUN uv sync --frozen --no-dev

FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder-dev
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --extra dev --no-install-project
COPY libs ./libs
COPY services ./services
RUN uv sync --frozen --extra dev

FROM python:3.11-slim AS ffmpeg-base
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && ffmpeg -version | head -1
RUN useradd --system --uid 10004 --create-home appuser
WORKDIR /app
ENV PYTHONUNBUFFERED=1 SCRATCH_DIR=/scratch PATH="/app/.venv/bin:$PATH"
RUN mkdir -p /app/fixtures /scratch \
    && ffmpeg -hide_banner -loglevel error \
       -f lavfi -i testsrc=size=640x360:rate=15 \
       -f lavfi -i sine=frequency=440 \
       -t 2 -pix_fmt yuv420p -c:v libx264 -c:a aac -shortest \
       /app/fixtures/testsrc-640x360.mp4 \
    && chown -R appuser:appuser /app/fixtures /scratch

FROM ffmpeg-base AS runtime
COPY --from=builder --chown=appuser:appuser /app /app
USER appuser
CMD ["python", "-m", "services.worker_transcode.main"]

FROM ffmpeg-base AS test
COPY --from=builder-dev --chown=appuser:appuser /app /app
COPY --chown=appuser:appuser tests ./tests
USER appuser
CMD ["pytest", "tests/ffmpeg", "-q"]
