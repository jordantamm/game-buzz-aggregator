.PHONY: help bootstrap up down logs restart proto test integration-test seed-reddit clean

SHELL := /bin/bash

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

bootstrap: ## First-time setup: copy .env, pull images
	@test -f .env || cp .env.example .env
	@echo "Now edit .env with your Reddit credentials, then run 'make up'."
	docker compose pull

up: ## Bring up the full stack
	docker compose up -d
	@echo "Stack starting. Run 'make logs' to follow."
	@echo ""
	@echo "  Redpanda Console: http://localhost:8090"
	@echo "  Temporal UI:      http://localhost:8233"
	@echo "  Jaeger:           http://localhost:16686"
	@echo "  Grafana:          http://localhost:3000"
	@echo "  MinIO Console:    http://localhost:9001"
	@echo "  API Gateway:      http://localhost:8080"

down: ## Stop and remove containers
	docker compose down

logs: ## Tail logs from app services
	docker compose logs -f reddit-connector enricher sink-consumer api-gateway mcp-server

restart: down up ## Full restart

proto: ## Regenerate protobuf code
	buf generate

test: ## Run unit tests
	cd services/reddit-connector && go test ./...
	cd services/sink-consumer && go test ./...
	cd services/api-gateway && go test ./...
	cd services/mcp-server && go test ./...
	cd services/enricher && uv run pytest -x

integration-test: ## Run integration tests (requires Docker)
	cd tests/integration && go test -tags=integration -v ./...

seed-reddit: ## Produce synthetic mentions to mentions.raw for testing without Reddit creds
	docker compose exec redpanda rpk topic produce mentions.raw < scripts/sample-mentions.txt

clean: ## Remove all containers, volumes, and generated code
	docker compose down -v
	rm -rf gen/
