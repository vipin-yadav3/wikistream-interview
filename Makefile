.PHONY: help setup up down producer pipeline test check-pg check-minio check-kafka reset clean

-include .env
export

PYTHON := python3.10
PYTEST := pytest

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ── Environment ───────────────────────────────────────────────────────────────

setup: ## First-time setup: deps, Docker, pre-download Spark JARs
	@test -f .env || (cp .env.example .env && echo "Created .env from .env.example")
	pip install -r requirements.txt
	$(MAKE) up
	@echo "Downloading Spark JARs (first time only, ~2-3 min) ..."
	$(PYTHON) scripts/prefetch_jars.py
	@echo ""
	@echo "✓ Setup complete."
	@echo "  Terminal 1: make producer   (start Wikipedia stream)"
	@echo "  Terminal 2: make pipeline   (start Spark job)"

up: ## Start Kafka, Postgres, MinIO
	docker compose up -d
	@echo "Waiting for services to be healthy ..."
	@sleep 12
	@echo "  Kafka:    localhost:9092"
	@echo "  Postgres: localhost:5432  (db=wikidb user=wiki password=wiki)"
	@echo "  MinIO:    http://localhost:9001  (minioadmin/minioadmin)"

down: ## Stop all services
	docker compose down -v

# ── Data & Pipeline ───────────────────────────────────────────────────────────

producer: ## Stream Wikipedia edits → Kafka (run in a separate terminal)
	$(PYTHON) scripts/wiki_producer.py

pipeline: ## Run the streaming pipeline (Spark job)
	$(PYTHON) -m jobs.pipeline

# ── Verification ──────────────────────────────────────────────────────────────

check-kafka: ## Tail 10 messages from wiki.recentchanges
	docker exec wiki-kafka kafka-console-consumer \
	  --bootstrap-server localhost:9092 \
	  --topic wiki.recentchanges \
	  --max-messages 10 \
	  --from-beginning

check-pg: ## Show latest rows in wiki_edit_counts
	docker exec wiki-postgres psql -U wiki -d wikidb \
	  -c "SELECT * FROM recent_stats LIMIT 15;"

check-minio: ## List Delta files in MinIO
	docker run --rm --network host minio/mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null; \
	docker run --rm --network host minio/mc ls --recursive local/wiki-stream/ 2>/dev/null || echo "No files yet."

# ── Tests ─────────────────────────────────────────────────────────────────────

test: ## Run automated unit tests (no Docker needed)
	$(PYTEST) tests/test_transforms.py -v

# ── Housekeeping ──────────────────────────────────────────────────────────────

reset: ## Wipe checkpoints + Postgres + MinIO for a fresh start
	rm -rf checkpoints/
	docker exec wiki-postgres psql -U wiki -d wikidb \
	  -c "TRUNCATE wiki_edit_counts, wiki_edit_counts_staging, bot_alerts;" 2>/dev/null || true
	@echo "Reset complete."

clean: ## Remove Python cache and temp files
	find . -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache checkpoints spark-warehouse derby.log metastore_db
