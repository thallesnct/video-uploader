# Video Pipeline

A Kafka-driven video transcoding pipeline: upload → probe → transcode to
multiple renditions → thumbnails → HLS package, with live progress in the
browser over SSE, Prometheus/Grafana metrics, and OpenTelemetry traces.

Architecture and the reasoning behind it live in [PLAN.md](PLAN.md) and
[docs/adr/](docs/adr/README.md) — start there for *why*; this file is only
*how to run it*. Day-to-day working conventions (commit style, code layout,
non-negotiables like "video bytes never go through Kafka") are in
[AGENTS.md](AGENTS.md).

**No live deployment target.** This project is built to a genuine
production-readiness bar (ADR-0015) but is never deployed anywhere beyond
local Docker Compose — see PLAN.md's scope note.

## Quickstart

Requires Docker (with the `compose` plugin). `uv` is optional — the
`Makefile` falls back to running it in a container when it's not on the
host, so a machine with only Docker still works.

```bash
git clone <this repo>
cd message-queue
make up            # Kafka (KRaft), Postgres, MinIO — then bootstraps topics + buckets
make migrate       # apply the read-model schema
make smoke         # confirm every dependency is actually usable
```

Bring the application services up and try a real upload:

```bash
docker compose --profile app up -d --build --wait
cd frontend && npm install && npm run dev   # http://localhost:5173
```

Or drive it without the frontend, straight over HTTP (the API issues
presigned MinIO URLs — ADR-0006):

```bash
curl -s http://localhost:8000/healthz
```

Tear everything down (drops volumes):

```bash
make down
```

## Observability

```bash
make obs-up        # + Prometheus, Grafana, Tempo, OTel Collector, kafka-exporter
```

- Grafana: <http://localhost:3000> — four provisioned dashboards
  (`ops/grafana/dashboards/`), code-defined, never hand-configured in the UI.
- Prometheus: <http://localhost:9090>
- Tempo: query API on `:3200`, fed by the OTel Collector on `:4317`/`:4318`.
  One trace spans every stage a video passes through (ADR-0010).

`make obs-verify` proves this for real: dashboards provision, a real upload
produces one trace spanning every stage, and the lag panel moves.

## Everyday commands

The full list is in `Makefile` (`make help` prints it with descriptions);
the ones used constantly:

```bash
make unit          # fast, no I/O, no containers — run this constantly
make integration    # real Kafka/Postgres/MinIO via testcontainers
make e2e            # full compose stack + Playwright
make lint           # ruff + mypy + eslint + migration backward-compat check
make ci             # everything CI runs: lint, unit, integration, e2e, replay-verify
make security-verify  # image scan (Trivy), non-root/read-only rootfs, egress denied
```

`ARGS` passes straight through to pytest/Playwright:
`make integration ARGS="-k transcode"`.

## Project layout

```
backend/    Python — libs/pipeline (shared) + services/<name> (one process each)
frontend/   React + TS + Vite, its own package.json — talks to backend only over HTTP/SSE
infra/      Operator tooling: topic/bucket bootstrap, the *_verify.py gate scripts
ops/        Prometheus, Grafana dashboards (as code), Tempo, OTel Collector config
tests/e2e/  Playwright, driving the real compose stack
docs/adr/   One architectural decision per file — read before changing a design
```

`PROGRESS.md` tracks phase-by-phase status against `PLAN.md`; each phase
closes only once its gate command has actually been run and passed.
