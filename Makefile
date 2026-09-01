# Pass pytest/playwright args through:  make integration ARGS="-k transcode"
ARGS ?=
COMPOSE := docker compose

# Tests run through uv. Use it from the host when installed (fast inner loop);
# otherwise fall back to a container so a bare machine with only Docker still
# works — the constraint AGENTS.md sets for this repo.
# --directory backend, not --project: uv chdirs the whole invocation there, so
# every relative path already in pyproject.toml (testpaths, per-file-ignores,
# pythonpath) resolves exactly as it did before the backend/frontend split —
# recipes below pass backend-relative paths (tests/unit, libs/pipeline), not
# repo-root-relative ones.
UV := $(shell command -v uv 2>/dev/null)
ifdef UV
  RUN := uv run --directory backend --extra dev
else
  RUN := docker run --rm -v "$(CURDIR)":/w -w /w -e UV_CACHE_DIR=/w/.uv-cache \
         ghcr.io/astral-sh/uv:python3.11-bookworm-slim uv run --directory backend --extra dev
endif
DC_OBS  := docker compose -f docker-compose.yml -f docker-compose.obs.yml

.DEFAULT_GOAL := help
.PHONY: help up down logs ps topics buckets bootstrap smoke migrate replay \
        obs-up obs-down obs-verify replay-verify unit integration e2e lint ci security-verify

help: ## List targets
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | sort | \
		awk -F':.*?## ' '{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.env:
	@cp .env.example .env && echo "created .env from .env.example"

up: .env ## Start Kafka (KRaft), Postgres and MinIO, then bootstrap them
	$(COMPOSE) up -d --wait
	@$(MAKE) --no-print-directory bootstrap

migrate: .env ## Apply database migrations (idempotent)
	$(COMPOSE) run --rm migrate

down: ## Stop everything and drop volumes
	$(DC_OBS) down -v --remove-orphans

ps: ## Show container status
	@$(COMPOSE) ps

logs: ## Tail logs (make logs SVC=kafka)
	$(COMPOSE) logs -f $(SVC)

bootstrap: topics buckets ## Create topics and buckets (idempotent)

topics: ## Create/verify Kafka topics from infra/topics.json
	@python3 infra/bootstrap_topics.py

buckets: ## Create/verify the MinIO bucket and lifecycle rules
	@bash infra/bootstrap_minio.sh

smoke: ## Verify every dependency is actually usable (Phase 1 gate)
	@python3 infra/smoke.py

replay: ## Manual DLQ replay: make replay TOPIC=<topic>.dlq [VIDEO=<video_id>]
	@python3 infra/replay.py --topic $(TOPIC) $(if $(VIDEO),--video-id $(VIDEO))

obs-up: .env ## Start the observability stack as well
	$(DC_OBS) up -d --wait

obs-down: ## Stop the observability stack, keep core services
	$(DC_OBS) stop prometheus grafana tempo otel-collector kafka-exporter

obs-verify: up migrate obs-up ## Dashboards provision, traces span all stages, lag panel live
	$(DC_OBS) --profile app up -d --build --wait
	@$(MAKE) --no-print-directory $(E2E_FIXTURE)
	@python3 infra/obs_verify.py

# The third leg of Phase 11's gate (a DLQ replay drives a video to
# completion) isn't in tests/e2e/ — `make replay` is a host CLI action by
# ADR-0005's own design, not something the Playwright container can invoke,
# and a genuinely corrupt file could never reach completed by being
# replayed unchanged. Separate from `e2e` for the same reason obs-verify is
# separate from it: it needs `api` in its normal (non-e2e-profile)
# S3_PUBLIC_ENDPOINT, which `make e2e` only restores on its way out.
replay-verify: up migrate ## Phase 11 gate, third leg: a DLQ replay drives a video to completed
	$(COMPOSE) --profile app up -d --build --wait
	@$(MAKE) --no-print-directory $(E2E_FIXTURE)
	@python3 infra/replay_verify.py

unit: ## Fast tests, no I/O, no containers
	$(RUN) pytest tests/unit $(ARGS)

# Integration tests drive testcontainers, so they need the Docker socket and
# cannot run inside the uv container. They use host uv when present, otherwise a
# project-local venv — nothing is installed outside this directory.
# Installed from the exported lockfile, never resolved by pip. Letting pip
# re-resolve ~100 packages sends it backtracking through ancient versions for
# many minutes; uv.lock is the source of truth (ADR-0014), so we install exactly
# what it pins.
HOST_VENV := backend/.venv-host
backend/requirements-dev.txt: backend/uv.lock backend/pyproject.toml
	docker run --rm -v "$(CURDIR)":/w -w /w -e UV_CACHE_DIR=/w/.uv-cache \
	  ghcr.io/astral-sh/uv:python3.11-bookworm-slim \
	  uv export --directory backend --all-extras --no-hashes --no-emit-project \
	  --format requirements-txt -o requirements-dev.txt

$(HOST_VENV)/bin/pytest: backend/requirements-dev.txt
	python3 -m venv $(HOST_VENV)
	$(HOST_VENV)/bin/pip install -q -U pip
	$(HOST_VENV)/bin/pip install -q --no-deps -r backend/requirements-dev.txt

ifdef UV
  RUN_HOST := uv run --directory backend --extra dev
else
  # No --directory equivalent for a plain venv's python — cd there instead.
  # $(CURDIR) makes the venv path itself immune to that cd.
  RUN_HOST := cd backend && $(CURDIR)/$(HOST_VENV)/bin/python -m
endif

integration: ## Tests against real Kafka/Postgres/MinIO via testcontainers
ifndef UV
	@$(MAKE) --no-print-directory $(HOST_VENV)/bin/pytest
endif
	$(RUN_HOST) pytest tests/integration $(ARGS)

ffmpeg-tests: ## Run the ffmpeg-dependent tests inside the worker image
	docker build --target test -f backend/services/worker_probe/Dockerfile \
	  -t vp-worker-probe:test backend
	docker run --rm vp-worker-probe:test pytest tests/ffmpeg -q -p no:cacheprovider $(ARGS)

# No media in git (AGENTS.md): the fixture is the same 2-second clip already
# baked into worker-probe's image (its ffmpeg-base stage), extracted once
# rather than re-encoded — ffmpeg isn't on the host at all.
E2E_FIXTURE := tests/e2e/.fixtures/testsrc-640x360.mp4
$(E2E_FIXTURE):
	@mkdir -p $(dir $(E2E_FIXTURE))
	cid=$$(docker create video-pipeline-worker-probe); \
	docker cp $$cid:/app/fixtures/testsrc-640x360.mp4 $(E2E_FIXTURE); \
	docker rm $$cid >/dev/null

# Two waves, not one `docker compose up` for everything: api/worker-*/
# projector need topics (and a migrated schema) to exist before they can start
# without crash-looping (docker-compose.yml's comment on the `app` profile).
# `make up` brings up and bootstraps the infra tier first; only then does the
# app tier (plus the e2e-only frontend) start.
#
# Playwright itself runs containerized (Microsoft's official image, joined to
# the compose network) rather than on the host: there is no Chromium build
# for every host OS/arch combination (hit exactly this — "Playwright does not
# support chromium on mac13-arm64" — scaffolding this phase), and CI would run
# it in a container regardless, so this keeps the local and CI paths identical.
# e2e overrides api's S3_PUBLIC_ENDPOINT to minio:9000 (the browser driving
# this run is itself inside the compose network, so a presigned URL signed
# for "localhost" points nowhere reachable) and starts the e2e-only frontend
# on host port 5173. Both are cleaned up unconditionally below — a run that
# exits without doing this leaves `api` handing out presigned URLs a normal
# host-run browser can't resolve, and squats on the port `npm run dev` wants
# (found leaking into a real dev session after a `make e2e` earlier).
e2e: up migrate ## Full compose + Playwright
	S3_PUBLIC_ENDPOINT=http://minio:9000 $(COMPOSE) --profile app --profile e2e up -d --build --wait
	@$(MAKE) --no-print-directory $(E2E_FIXTURE)
	docker run --rm --network video-pipeline_default \
	  -v "$(CURDIR)/tests/e2e":/e2e -w /e2e \
	  -e E2E_BASE_URL=http://frontend:5173 \
	  mcr.microsoft.com/playwright:v1.62.1-noble sh -c "npm ci && npm test -- $(ARGS)"; \
	test_status=$$?; \
	$(COMPOSE) --profile app up -d --build --wait api; \
	$(COMPOSE) --profile app --profile e2e stop frontend; \
	exit $$test_status

# infra/ sits outside backend/ on purpose (AGENTS.md: operator tooling that
# runs before the backend's venv exists) but still wants the same lint rules —
# infra/ruff.toml extends backend/pyproject.toml's config rather than this
# target trying to point one ruff invocation at two directories with
# different per-file-ignore bases.
lint: ## ruff + mypy + eslint
	$(RUN) ruff check .
	$(RUN) ruff format --check .
	$(RUN) mypy libs/pipeline
	$(RUN) ruff check ../infra
	$(RUN) ruff format --check ../infra
	cd frontend && npm run lint

# `up migrate`, not just `up`, matching e2e/replay-verify's own precedent
# (docker-compose.yml's comment on the `app` profile): on a machine that has
# never run `make migrate` before (a fresh CI runner, not this session's own
# already-migrated dev volumes), the app-profile containers would crash-loop
# against an unmigrated schema and `--wait` below would time out instead of
# ever reaching the checks.
security-verify: up migrate ## Image scan, non-root, read-only rootfs, egress denied
	$(COMPOSE) --profile app up -d --build --wait
	@python3 infra/security_verify.py

ci: lint unit integration e2e replay-verify ## Everything CI runs
