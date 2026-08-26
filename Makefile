# Pass pytest/playwright args through:  make integration ARGS="-k transcode"
ARGS ?=
COMPOSE := docker compose

# Tests run through uv. Use it from the host when installed (fast inner loop);
# otherwise fall back to a container so a bare machine with only Docker still
# works — the constraint AGENTS.md sets for this repo.
UV := $(shell command -v uv 2>/dev/null)
ifdef UV
  RUN := uv run --extra dev
else
  RUN := docker run --rm -v "$(CURDIR)":/w -w /w -e UV_CACHE_DIR=/w/.uv-cache \
         ghcr.io/astral-sh/uv:python3.11-bookworm-slim uv run --extra dev
endif
DC_OBS  := docker compose -f docker-compose.yml -f docker-compose.obs.yml

.DEFAULT_GOAL := help
.PHONY: help up down logs ps topics buckets bootstrap smoke \
        obs-up obs-down obs-verify unit integration e2e lint ci security-verify

help: ## List targets
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | sort | \
		awk -F':.*?## ' '{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.env:
	@cp .env.example .env && echo "created .env from .env.example"

up: .env ## Start Kafka (KRaft), Postgres and MinIO, then bootstrap them
	$(COMPOSE) up -d --wait
	@$(MAKE) --no-print-directory bootstrap

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

obs-up: .env ## Start the observability stack as well
	$(DC_OBS) up -d --wait

obs-down: ## Stop the observability stack, keep core services
	$(DC_OBS) stop prometheus grafana tempo otel-collector kafka-exporter

obs-verify: ## Dashboards provision, traces span all stages, lag panel live
	@echo "not implemented until Phase 10" && exit 1

unit: ## Fast tests, no I/O, no containers
	$(RUN) pytest tests/unit $(ARGS)

# Integration tests drive testcontainers, so they need the Docker socket and
# cannot run inside the uv container. They use host uv when present, otherwise a
# project-local venv — nothing is installed outside this directory.
HOST_VENV := .venv-host
$(HOST_VENV)/bin/pytest:
	python3 -m venv $(HOST_VENV)
	$(HOST_VENV)/bin/pip install -q -U pip
	$(HOST_VENV)/bin/pip install -q -e ".[dev]"

ifdef UV
  RUN_HOST := uv run --extra dev
else
  RUN_HOST := $(HOST_VENV)/bin/python -m
endif

integration: ## Tests against real Kafka/Postgres/MinIO via testcontainers
ifndef UV
	@$(MAKE) --no-print-directory $(HOST_VENV)/bin/pytest
endif
	$(RUN_HOST) pytest tests/integration $(ARGS)

e2e: ## Full compose + Playwright
	@echo "not implemented until Phase 8" && exit 1

lint: ## ruff + mypy + eslint
	$(RUN) ruff check .
	$(RUN) ruff format --check .
	$(RUN) mypy libs/pipeline

security-verify: ## Image scan, non-root, read-only rootfs, egress denied
	@echo "not implemented until Phase 12" && exit 1

ci: lint unit integration e2e ## Everything CI runs
