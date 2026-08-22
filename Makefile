.PHONY: help bootstrap up down logs restart ps proto test test-go test-py integration-test \
        eval eval-sync seed-reddit psql trending ask clean fmt

SHELL := /bin/bash

GO_MODULES := pkg services/reddit-connector services/sink-consumer services/api-gateway services/mcp-server
PY_SERVICES := enricher analyst-agent

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ─── Stack ────────────────────────────────────────────────────────────────

bootstrap: ## First-time setup: create .env, pull images
	@test -f .env || (cp .env.example .env && echo "Created .env from .env.example")
	@echo "Edit .env with your Reddit + Anthropic credentials, then run 'make up'."
	docker compose pull

up: ## Bring up the full stack
	docker compose up -d --build
	@echo ""
	@echo "Stack starting. First boot downloads ~500MB of ML models — the"
	@echo "enricher stays unhealthy until that finishes. Follow with 'make logs'."
	@echo ""
	@echo "  API Gateway:      http://localhost:8080/v1/trending"
	@echo "  Analyst agent:    http://localhost:8100/ask"
	@echo "  MCP server:       http://localhost:8085/sse"
	@echo "  Redpanda Console: http://localhost:8090"
	@echo "  Temporal UI:      http://localhost:8233"
	@echo "  Jaeger:           http://localhost:16686"
	@echo "  Grafana:          http://localhost:3000"
	@echo "  MinIO Console:    http://localhost:9001"

down: ## Stop and remove containers
	docker compose down

restart: down up ## Full restart

ps: ## Show container status
	docker compose ps

logs: ## Tail logs from app services
	docker compose logs -f reddit-connector enricher sink-consumer api-gateway mcp-server analyst-agent

# ─── Codegen ──────────────────────────────────────────────────────────────

proto: ## Regenerate protobuf code
	buf generate

fmt: ## Format Go code
	gofmt -w pkg services tests

# ─── Tests ────────────────────────────────────────────────────────────────

test: test-go test-py ## Run all unit tests

test-go: ## Run Go unit tests
	@set -e; for m in $(GO_MODULES); do \
		echo "── $$m"; \
		(cd $$m && go build ./... && go vet ./... && go test ./...); \
	done

test-py: ## Run Python unit tests
	@set -e; for s in $(PY_SERVICES); do \
		echo "── services/$$s"; \
		(cd services/$$s && PYTHONPATH=$(CURDIR)/gen/python python -m pytest -q); \
	done

integration-test: ## Run integration tests (requires Docker)
	cd tests/integration && go test -tags=integration -timeout=20m -v ./...

# ─── Evals ────────────────────────────────────────────────────────────────

eval-sync: ## Regenerate evals/system_prompt.txt from resolve.py
	python evals/scripts/sync_system_prompt.py

eval: eval-sync ## Run the promptfoo disambiguation suite and enforce thresholds
	@test -n "$$ANTHROPIC_API_KEY" || (echo "ANTHROPIC_API_KEY is not set" && exit 1)
	cd evals && promptfoo eval --config promptfooconfig.yaml --no-cache --output results/latest.json
	python evals/scripts/check_thresholds.py evals/results/latest.json

# ─── Dev helpers ──────────────────────────────────────────────────────────

seed-reddit: ## Ingest synthetic mentions without Reddit credentials
	docker compose run --rm reddit-connector -mock

psql: ## Open a psql shell against the running database
	docker compose exec postgres psql -U gba -d gba

trending: ## Curl the trending endpoint
	@curl -s "http://localhost:8080/v1/trending?window=24h&limit=10" | python -m json.tool

ask: ## Ask the analyst agent a question: make ask Q="what's trending and why"
	@test -n "$(Q)" || (echo 'Usage: make ask Q="your question"' && exit 1)
	docker compose run --rm analyst-agent "$(Q)"

clean: ## Remove containers, volumes, and eval results
	docker compose down -v
	rm -rf evals/results/
