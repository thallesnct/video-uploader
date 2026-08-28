# AGENTS.md

Working agreement for this repo. `CLAUDE.md` is a **symlink to this file** — one
document, read by every agent and every human.

Architecture lives in [PLAN.md](PLAN.md). Decisions live in
[docs/adr/](docs/adr/README.md). This file is about *how we work*, not *what we built*.

## Project in one line

Kafka-driven video transcoding pipeline in Python: upload → probe → transcode to
multiple renditions → thumbnails → HLS package → live SSE updates in the browser,
with Prometheus metrics and OpenTelemetry traces.

## Non-negotiables

1. **Read the ADR before changing a design.** If the change contradicts an ADR,
   write a new ADR that supersedes it — do not edit the old one's decision.
   Any new architectural decision → new ADR. This is the point of the repo.
2. **Update `PROGRESS.md` in the same change that lands the work.** Check the
   boxes, flip the phase state, add a line to the decision log if a choice changed.
   A phase moves to `[x]` only when its **gate command actually passed** — paste
   nothing, run it.
3. **Video bytes never go through Kafka.** Claim-check only (ADR-0001). A message
   carries identifiers and object keys.
4. **Never block the Kafka poll loop.** Long work runs off-thread with the
   partition paused (ADR-0004). A `time.sleep` or a synchronous ffmpeg call
   inside `poll()` is a bug, not a style issue.
5. **State columns belong to the projector; claim columns belong to the worker.**
   A worker that writes a status column has reintroduced the dual write ADR-0007
   exists to prevent. Classify every new column before adding it.
6. **Every consumer must be idempotent.** At-least-once delivery is a given
   (ADR-0005). Assume every message will arrive twice.
7. **Treat media input as hostile.** ffmpeg parses attacker-controlled bytes
   (ADR-0015). Never `shell=True`, never build a command from an f-string, never
   use the uploaded filename as a path — derive paths from `video_id`. Media
   workers stay non-root with no extra network egress.
8. **`/healthz` checks nothing external; `/readyz` checks dependencies.** A
   liveness probe that pings Kafka turns a broker blip into a fleet-wide restart.
9. **New dependency = a line of justification.** ADR-0014 records the stack and
   what each choice beat. Adding something outside it needs a note in the PR, or
   a new ADR if it replaces a listed choice. Pin it in the lockfile (`uv`).

## Environment constraints

- **`ffmpeg`/`ffprobe` are NOT installed on the dev machine.** They exist only in
  the worker images. Any test that shells out to them must run in Docker or be
  marked `@pytest.mark.ffmpeg` and skipped locally. Do not add a host-level
  ffmpeg dependency to a unit test.
- Python 3.11, Docker available. Kafka runs in **KRaft mode** — no ZooKeeper in
  any compose file or doc.
- Test fixture is a 2-second generated clip
  (`ffmpeg -f lavfi -i testsrc=size=640x360:rate=15 -t 2`). Do not add large
  binary media to the repo.
- **The Playwright container (`mcr.microsoft.com/playwright:*-noble`) has no
  H.264 decode.** Verified empirically:
  `MediaSource.isTypeSupported("video/mp4; codecs=\"avc1...\"")` returns
  `false`, and the image ships only `chromium`/`chromium_headless_shell`/
  `firefox`/`webkit` — no `google-chrome`. hls.js still fetches and parses
  playlists/segments fine (that's a network/parsing concern, not decode); an
  e2e spec can assert the fetch chain but never `video.currentTime`
  advancing. Don't add `channel: "chrome"` or install Google Chrome to work
  around it — that's a new dependency (non-negotiable #9) and more image
  weight, not a one-line fix.

## Commands

```bash
make up            # compose up: kafka (KRaft), postgres, minio
make obs-up        # + prometheus, grafana, tempo, otel-collector, kafka-exporter
make obs-verify    # dashboards provision, one trace spans all stages, lag panel live
make security-verify  # trivy scan, non-root + read-only rootfs, egress denied
make topics        # create/verify topics (idempotent)
make smoke         # health check every dependency
make unit          # pytest tests/unit — no I/O, no containers, fast
make integration   # pytest tests/integration — testcontainers
make e2e           # full compose + playwright
#   pass pytest/playwright args through:  make integration ARGS="-k transcode"
make lint          # ruff + mypy + eslint
make ci            # lint + unit + integration + e2e
make down
```

Run `make unit` constantly; `make integration` before declaring a stage done.

## Code layout

Backend (Python) and frontend (TS) are separate top-level trees, each with its
own manifest and dependency graph (`backend/pyproject.toml` + `uv.lock` vs.
`frontend/package.json`) — nothing under one imports from the other. What
orchestrates *both* (`docker-compose.yml`, `Makefile`, `docs/`, `tests/e2e/`,
`ops/`) stays at the true repo root, never inside either tree, because it has
to stand above the boundary it's proving.

- `backend/libs/pipeline/` — shared: event envelope, topic registry,
  producer/consumer wrappers, storage, retry/DLQ, observability bootstrap.
  **New cross-service logic goes here, not copied between services.**
- `backend/services/<name>/` — one process each; thin, using `libs/pipeline`.
- `backend/migrations/`, `backend/tests/{unit,integration,ffmpeg}/`.
- `frontend/` — React + TS + Vite. Its own `package.json`/lockfile; nothing
  here reaches into `backend/` except through the API over HTTP/SSE.
- `infra/` — Kafka topic/MinIO bootstrap scripts. Stays at root (not
  `backend/`) because it's operator tooling that runs *before* the backend's
  venv exists, but it still imports `backend/libs/pipeline/topics.json` as
  data, so it shares `backend/pyproject.toml`'s ruff config explicitly
  (`--config backend/pyproject.toml`) rather than relying on path discovery.
- `ops/` — Prometheus config, **provisioned** Grafana dashboard JSON, Tempo, OTel.
- `tests/e2e/` — Playwright, at the true root, not under either tree: it drives
  a browser against `frontend/` talking to the full `backend/` compose stack,
  so it belongs to neither side alone.

## Conventions

- Event names are `noun.past_tense` (`video.uploaded`, `rendition.completed`).
  Topic names match the event they carry.
- Every message is keyed by `video_id` — per-video ordering depends on it.
- Every event carries `event_id`, `video_id`, `occurred_at`, `schema_version`.
  Adding a field is fine; removing or retyping one requires an ADR (ADR-0003).
- Consumers ignore unknown fields (forward compatibility).
- Grafana dashboards are code in `ops/grafana/` — never "configured in the UI".
- Object keys are built by `libs/pipeline/storage.py` helpers, never f-strings
  at the call site.
- Structured JSON logs with `video_id` and `trace_id` on every line.
- Frontend styling is CSS Modules, one `Component.module.css` colocated next
  to each `Component.tsx` — never a single shared stylesheet of global class
  names. Only true cross-component concerns (body background, box-sizing,
  font stack) live in `frontend/src/global.css`. A small utility class
  (`.muted`, `.error`) duplicated across a couple of modules is fine; it's
  cheaper than a shared-but-not-quite-global module a reader has to go find.
- Every color, spacing, and radius value in a `*.module.css` file is a
  `var(--token)` from `frontend/src/global.css`'s `:root` block, never a
  hardcoded hex/rem literal — a palette change stays a one-file edit.
- A component with more than one file (a `.tsx` plus its `.module.css`, or a
  test alongside it) gets its own subfolder — `pages/UploadPage/UploadPage.tsx`
  + `UploadPage.module.css`, not two files sitting loose in `pages/`.
- Frontend API writes go through TanStack Query's `useMutation`, never a
  hand-rolled `async function` + local `useState` for pending/error tracking
  in the component. Sub-step UI state that changes *during* one mutation
  (an upload-progress percentage, a "creating…/uploading…/completing…" label)
  stays local `useState` next to it — that part isn't server-cache state and
  `useMutation` doesn't model it — but the pending/error/success outcome and
  any resulting cache invalidation (`queryClient.invalidateQueries`) belong to
  the mutation, not a `try`/`catch` in the component.

## Definition of done for a stage

- [ ] Unit tests for the pure logic
- [ ] Integration test against real Kafka/Postgres/MinIO via testcontainers
- [ ] Redelivery test — the same message twice produces one result
- [ ] Failures route to retry or DLQ with a reason, never a silent drop
- [ ] `/metrics` exposes duration + failure counters for the stage
- [ ] The stage's span joins the video's trace via the `traceparent` header
- [ ] Graceful SIGTERM handling; no work lost, no offset committed early
- [ ] Config read through the service's settings module, never `os.environ` inline
- [ ] `PROGRESS.md` updated, gate command run and green

## Git

- Conventional commits, scoped to the directory touched:
  `feat(worker-transcode): pause partition while ffmpeg runs`.
- Do not add `Co-Authored-By` or session trailers to commit messages.
- Commit or push only when asked.

### Commit granularity

**One checkbox in `PROGRESS.md` ~= one commit.** A checkbox too big for one commit
becomes two commits, never one bigger commit. A phase is *never* a single commit.

- **The ADR lands first, in its own `docs(adr):` commit**, before the code that
  implements it. If the decision changes while building, the ADR is amended or
  superseded before the implementation commit, not after.
- **Tests ship in the same commit as the code they cover.** No trailing
  "add tests for X" commits; a commit that adds behaviour without its test is
  incomplete, not staged.
- **Tick the checkbox in the same commit as the work** (non-negotiable #2), so
  `git log` and `PROGRESS.md` can never disagree.
- **Each phase closes with a `chore(progress):` commit** that flips the phase to
  `[x]` and quotes the gate output in the commit body. That commit is the proof
  the gate ran.
- Every commit leaves the tree working - `make unit` green at minimum. Being able
  to bisect is the point.
- Refactors and behaviour changes are always separate commits.
