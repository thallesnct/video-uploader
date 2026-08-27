# The only images with ffmpeg are the media workers (ADR-0011). Keeping it out
# of the API image means a hostile file has nothing to reach even if one somehow
# arrives there.
#
# Stages: dependencies are resolved twice — once without dev for the shipped
# image, once with dev for the test image — while ffmpeg and the generated
# fixture are built once and shared, so tests and production run the same binary
# against the same clip.

# ---------------------------------------------------------------- dependencies
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

# ------------------------------------------------------------------ ffmpeg base
FROM python:3.11-slim AS ffmpeg-base

# Debian bookworm's ffmpeg (5.1.x), pinned to the distro rather than a static
# build so security updates arrive with the base image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && ffmpeg -version | head -1

RUN useradd --system --uid 10003 --create-home appuser
WORKDIR /app
ENV PYTHONUNBUFFERED=1 SCRATCH_DIR=/scratch PATH="/app/.venv/bin:$PATH"

# The generated clip lives in the image so the ffmpeg tests and any future e2e
# use exactly the same 2-second source. No media in git (ADR-0011).
RUN mkdir -p /app/fixtures /scratch \
    && ffmpeg -hide_banner -loglevel error \
       -f lavfi -i testsrc=size=640x360:rate=15 \
       -f lavfi -i sine=frequency=440 \
       -t 2 -pix_fmt yuv420p -c:v libx264 -c:a aac -shortest \
       /app/fixtures/testsrc-640x360.mp4 \
    && chown -R appuser:appuser /app/fixtures /scratch

# --------------------------------------------------------------------- runtime
FROM ffmpeg-base AS runtime
COPY --from=builder --chown=appuser:appuser /app /app
USER appuser
CMD ["python", "-m", "services.worker_probe.main"]

# ------------------------------------------------------------------ test image
FROM ffmpeg-base AS test
COPY --from=builder-dev --chown=appuser:appuser /app /app
COPY --chown=appuser:appuser tests ./tests
USER appuser
CMD ["pytest", "tests/ffmpeg", "-q"]
