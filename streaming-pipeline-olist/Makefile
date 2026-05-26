# =============================================================================
# Real-time Streaming Pipeline — Olist :: Makefile
# =============================================================================
# Usage: make <target>.  Run `make help` to list all targets.
# =============================================================================

SHELL := /bin/bash
PYTHON ?= python
COMPOSE ?= docker compose

INFRA_SERVICES := zookeeper kafka schema-registry kafka-ui spark-master spark-worker prometheus grafana

.DEFAULT_GOAL := help
.PHONY: help setup up down up-all logs-kafka logs-spark logs-producer \
        topics schemas produce pipeline dashboard check seed \
        test-unit test-integration test-e2e test lint format clean reset

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# -----------------------------------------------------------------------------
# Bootstrapping
# -----------------------------------------------------------------------------
setup: ## Install deps, bring up infra, create topics, register schemas, seed data
	$(PYTHON) -m pip install -r requirements.txt
	$(MAKE) up
	@echo "Waiting for Kafka + Schema Registry to become healthy..."
	@sleep 25
	$(MAKE) topics
	$(MAKE) schemas
	$(MAKE) seed
	@echo "Setup complete. Run 'make up-all' to start producers + pipeline + dashboard."

# -----------------------------------------------------------------------------
# Docker lifecycle
# -----------------------------------------------------------------------------
up: ## Start core infra only (zookeeper, kafka, schema-registry, spark, monitoring)
	$(COMPOSE) up -d $(INFRA_SERVICES)

down: ## Stop all containers
	$(COMPOSE) down

up-all: ## Start everything (infra + producers + pipeline + dashboard)
	$(COMPOSE) up -d --build

# -----------------------------------------------------------------------------
# Logs
# -----------------------------------------------------------------------------
logs-kafka: ## Tail Kafka logs
	$(COMPOSE) logs -f kafka

logs-spark: ## Tail Spark pipeline logs
	$(COMPOSE) logs -f spark-pipeline

logs-producer: ## Tail orders-producer logs
	$(COMPOSE) logs -f orders-producer

# -----------------------------------------------------------------------------
# Operational scripts
# -----------------------------------------------------------------------------
topics: ## Create Kafka topics with correct configs
	$(PYTHON) scripts/create_topics.py

schemas: ## Register Avro schemas in Schema Registry
	$(PYTHON) scripts/register_schemas.py

seed: ## Prepare Olist CSVs for replay
	$(PYTHON) scripts/seed_data.py

produce: ## Run orders producer interactively
	$(PYTHON) producers/orders_producer.py

pipeline: ## Run the Spark pipeline locally
	$(PYTHON) spark_jobs/pipeline_runner.py

dashboard: ## Launch the Streamlit dashboard
	streamlit run dashboard/app.py

check: ## Health-check all pipeline components
	$(PYTHON) scripts/check_pipeline.py

# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------
test-unit: ## Run unit tests
	pytest tests/unit/ -v

test-integration: ## Run integration tests (requires docker up)
	pytest tests/integration/ -v

test-e2e: ## Run end-to-end tests (requires pipeline running)
	pytest tests/e2e/ -v

test: test-unit test-integration ## Run unit + integration tests

# -----------------------------------------------------------------------------
# Code quality
# -----------------------------------------------------------------------------
lint: ## Run black --check + flake8 + mypy
	black --check producers spark_jobs dashboard scripts tests
	flake8 producers spark_jobs dashboard scripts tests
	mypy producers spark_jobs dashboard scripts

format: ## Auto-format with black
	black producers spark_jobs dashboard scripts tests

# -----------------------------------------------------------------------------
# Cleanup
# -----------------------------------------------------------------------------
clean: ## Stop everything, remove volumes, clear checkpoints + local Delta data
	$(COMPOSE) down -v
	rm -rf /tmp/streaming-checkpoints
	rm -rf data/bronze data/silver data/gold data/reference

reset: clean setup ## Full reset: clean + setup from scratch
