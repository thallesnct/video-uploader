# ADR-0014: Library stack — what we depend on and why

- **Status:** Accepted
- **Date:** 2026-08-25

## Context

This is meant to be a production-grade application, not a demo. Dependency
choices are the ones hardest to reverse later: they shape the concurrency model,
the test story, the container image, and who can maintain the code. Each pick
below is weighed against the credible industry alternatives, with the
discriminator stated. Kafka clients are decided separately in ADR-0009.

## Decision

### Application core

| Concern | Choice | Alternatives weighed | Discriminator |
|---|---|---|---|
| Web framework | **FastAPI** | Litestar, Django REST, Flask, Starlette raw | Async-native (required for long-lived SSE), Pydantic-native validation, OpenAPI for free, the largest hiring pool of the async options. Litestar is arguably cleaner but a smaller ecosystem; Django/DRF is sync-first and brings an ORM and admin we do not want. |
| ASGI server | **uvicorn** (`uvloop` + `httptools`), managed by **gunicorn** with uvicorn workers in prod | granian, hypercorn, uvicorn standalone | gunicorn gives battle-tested process supervision, graceful reloads and worker recycling; uvicorn gives the fast ASGI loop. Granian is promising but young for a production dependency. |
| Config | **pydantic-settings** | dynaconf, environs, raw `os.environ` | Typed, validated-at-startup 12-factor config; fails fast on a missing env var instead of at 3 a.m. on first use. |
| Data models | **Pydantic v2** | dataclasses + cattrs, attrs, marshmallow | Already the event-envelope choice (ADR-0003); v2's Rust core makes per-message validation cheap enough to run on the hot path. |
| ORM / DB access | **SQLAlchemy 2.0** — async (`asyncpg`) in the API, sync (`psycopg[binary]`) in workers | SQLModel, Tortoise ORM, Piccolo, raw asyncpg | The only Python ORM with a serious production track record at this scale, plus it degrades gracefully to raw SQL for the `UPDATE … RETURNING` claims of ADR-0005/0013. SQLModel adds a layer over the same thing and lags SQLAlchemy releases. |
| Migrations | **Alembic** | yoyo, raw SQL + a runner, django-migrations | Autogenerate plus hand-editing is the pragmatic middle; runs as a pre-deploy job, never on app start. |
| Object store | **boto3** (workers, presigning) + **aioboto3** (API) | minio-py, s3fs, obstore | boto3 is the reference S3 implementation — correct presigning, multipart, retry/backoff config; minio-py would tie the abstraction to one server. |
| Media | **`subprocess` with explicit argv** wrapping the ffmpeg CLI; `ffprobe -print_format json` for metadata | `ffmpeg-python`, PyAV, MoviePy | Deliberate: the CLI is the interface every ffmpeg answer on the internet is written against, it is trivially reproducible by hand, and it runs **out of process** — a crash or a hang on hostile input kills a subprocess, not the worker (see ADR-0015 on ffmpeg as an attack surface). PyAV embeds libav in-process, which turns a decoder segfault into a worker crash. `ffmpeg-python` is an unmaintained DSL over the same argv we can write directly. |
| Retries (non-Kafka) | **tenacity** | backoff, hand-rolled | Declarative, testable, composable with `freezegun`. Kafka-level retries stay topic-based (ADR-0005). |
| Structured logging | **structlog** + stdlib logging bridge | loguru, python-json-logger alone | Context binding (`video_id`, `trace_id`) is the whole point; loguru fights stdlib integration and OTel log correlation. |
| Metrics | **prometheus-client** | prometheus-fastapi-instrumentator (as an add-on), statsd | The reference client. **Gotcha to design for:** under gunicorn's multiple workers it needs `PROMETHEUS_MULTIPROC_DIR` and a multiprocess collector, or `/metrics` returns whichever worker answered. Workers are single-process, so only the API needs this. |
| Tracing | **opentelemetry-sdk** + instrumentation for FastAPI, SQLAlchemy, botocore | vendor SDKs, OpenTracing | Vendor-neutral, and the Kafka header propagation of ADR-0010 is standard W3C context. |
| SSE | **sse-starlette** | hand-rolled `StreamingResponse`, `EventSourceResponse` clones | Handles the parts people get wrong: heartbeat comments, disconnect detection, correct framing. Thin enough to read end to end. |
| CLI / ops tasks | **typer** | click, argparse | Type-hint driven, same Pydantic-flavoured ergonomics; used for `topics`, `replay`, `backfill`. |
| Dep management | **uv** with a committed lockfile | Poetry, pip-tools, PDM | Fast enough to reinstall in every CI job and every Docker layer, resolver-correct, and it has become the default choice for new Python services. Poetry remains the conservative fallback if the team prefers it. |
| Lint / format / types | **ruff** (lint+format) + **mypy** (strict on `libs/pipeline`) + **pre-commit** | black+isort+flake8, pyright | Ruff replaces four tools with one fast one. Strict typing is scoped to the shared library, where a mistake reaches every service. |

### Frontend

| Concern | Choice | Alternatives weighed | Discriminator |
|---|---|---|---|
| Framework | **React 19 + TypeScript + Vite** | SvelteKit, Vue, Next.js | Largest ecosystem for the pieces we need; Vite keeps it a plain SPA — no SSR value here since the page is authenticated and live-updating. Next.js would add a server we do not need. **19, not 18:** it has been stable since Dec 2024, our whole frontend stack supports it, and starting a greenfield app on the previous major only buys a migration later. React Compiler is available but stays **opt-in** — adopt it after the UI works, never while debugging SSE re-render behaviour. |
| Server state | **TanStack Query** | Redux Toolkit Query, SWR, plain fetch | Cache invalidation on SSE events is a one-liner (`queryClient.setQueryData`), which is exactly the snapshot-plus-delta shape of ADR-0008. |
| Live updates | **native `EventSource`** | `fetch-event-source`, socket.io | Built in, auto-reconnects, sends `Last-Event-ID` for free. Swap to `@microsoft/fetch-event-source` only if auth headers on the stream become necessary (EventSource cannot set headers — until then the token goes in a short-lived query param or cookie). |
| Upload | **XHR/`fetch` PUT to the presigned URL** with progress; **Uppy** if resumable multipart UX is wanted | tus, Uppy from the start | Start with the smallest thing that shows a progress bar; Uppy earns its weight only once multi-GB resumable uploads matter. |
| Playback | **hls.js** | video.js, Shaka Player, native `<video>` | Smallest library that plays our HLS output everywhere except Safari (which plays it natively). |
| Testing | **Vitest** + **Testing Library**; **Playwright** for e2e | Jest, Cypress | Vitest shares Vite's config; Playwright handles the SSE/async-update assertions and multi-browser more reliably than Cypress. |

### Infrastructure images

Kafka in **KRaft mode** (`confluentinc/cp-kafka` or `redpanda` for a lighter dev
loop), Postgres 16, MinIO, Prometheus, Grafana, Tempo, OTel Collector,
`danielqsj/kafka-exporter`. Worker images are multi-stage: build wheels on
`python:3.11-slim`, runtime layer adds a pinned ffmpeg build, runs as non-root.

## Alternatives considered (stack-level)

- **Celery + Redis/RabbitMQ instead of Kafka-native workers.** The mainstream
  Python answer for background jobs, with mature retries and monitoring. Rejected
  for this system: it would add a second broker, and it discards the log semantics
  (replay, multiple independent consumer groups, lag as a first-class signal) that
  the whole design is built on. Noted here because it is the choice a reviewer
  will ask about.
- **A stream-processing framework (Faust, Quix Streams, Bytewax).** Rejected: our
  stages are consume→work→produce, and a framework would hide the poll-loop
  control that ADR-0004 depends on.
- **Django monolith with a task queue.** Rejected: sync-first, and the SSE
  connection model fits poorly.
- **Go or Rust for the transcode workers.** Genuinely defensible — the worker is
  mostly process supervision and I/O. Rejected to keep one language, one shared
  contracts library, and one test harness. Revisit only if per-worker overhead
  ever shows up in a profile, which it will not: ffmpeg dominates.

## Consequences

- Two async/sync worlds coexist (async API, sync workers). The boundary is
  explicit and per-service; no service mixes both.
- The `prometheus-client` multiprocess requirement must be handled in the API's
  container entrypoint or `/metrics` will quietly under-report.
- `uv` and pinned lockfiles mean dependency updates are a deliberate, reviewable
  change (Renovate/Dependabot PRs), not a rebuild-time surprise.
- Every dependency above is pinned; the image is scanned in CI (ADR-0015).
