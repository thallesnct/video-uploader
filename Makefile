# Pass pytest/playwright args through:  make integration ARGS="-k transcode"
ARGS ?=
COMPOSE := docker compose
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
	@echo "not implemented until Phase 2" && exit 1

integration: ## Tests against real Kafka/Postgres/MinIO via testcontainers
	@echo "not implemented until Phase 3" && exit 1

e2e: ## Full compose + Playwright
	@echo "not implemented until Phase 8" && exit 1

lint: ## ruff + mypy + eslint
	@echo "not implemented until Phase 2" && exit 1

security-verify: ## Image scan, non-root, read-only rootfs, egress denied
	@echo "not implemented until Phase 12" && exit 1

ci: lint unit integration e2e ## Everything CI runs
