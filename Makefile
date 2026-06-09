.PHONY: help setup check-env up down producer pipeline test check-pg check-minio check-kafka reset clean

-include .env
export

# Use whatever python3 the candidate has — don't hardcode 3.10
PYTHON ?= python3
PYTEST  = $(PYTHON) -m pytest

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ── Environment ───────────────────────────────────────────────────────────────

setup: ## First-time setup: deps, Docker, pre-download Spark JARs (~3 min)
	@$(MAKE) check-env
	@test -f .env || (cp .env.example .env && echo "Created .env from .env.example")
	$(PYTHON) -m pip install -r requirements.txt
	$(MAKE) up
	@echo "Pre-downloading Spark JARs (one-time, ~3 min — looks like a hang, it's not) ..."
	$(PYTHON) scripts/prefetch_jars.py
	@echo ""
	@echo "✓ Setup complete."
	@echo "  Terminal 1: make producer   (start Wikipedia stream)"
	@echo "  Terminal 2: make pipeline   (start Spark job)"

check-env: ## Verify Python, Java, and Docker are installed correctly
	@echo "Checking prerequisites..."
	@$(PYTHON) --version 2>&1 | grep -E "Python 3\.(9|10|11|12)" > /dev/null || \
	  (echo "✗ Python 3.9+ required. Install from https://python.org" && exit 1)
	@java -version 2>&1 | grep -E "version \"(11|17)\." > /dev/null || \
	  (echo "✗ Java 11 or 17 required (Java 25+ not supported by PySpark 3.5)." && \
	   echo "  Install Java 17: https://adoptium.net/temurin/releases/?version=17" && exit 1)
	@docker info > /dev/null 2>&1 || \
	  (echo "✗ Docker not running. Start Docker Desktop." && exit 1)
	@test -f .env || \
	  (echo "✗ .env file missing. Run: cp .env.example .env" && exit 1)
	@echo "✓ Python OK  ✓ Java OK  ✓ Docker OK  ✓ .env OK"

up: ## Start Kafka, Postgres, MinIO
	docker compose up -d
	@echo "Waiting for services to be healthy ..."
	@sleep 15
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

check-kafka: ## Tail 5 messages from wiki.recentchanges
	docker exec wiki-kafka kafka-topics --bootstrap-server localhost:9092 --list
	docker exec wiki-kafka kafka-console-consumer \
	  --bootstrap-server localhost:9092 \
	  --topic wiki.recentchanges \
	  --max-messages 5 \
	  --timeout-ms 8000 \
	  --from-beginning 2>/dev/null || echo "(no messages yet — is make producer running?)"

check-pg: ## Show latest rows in wiki_edit_counts
	docker exec wiki-postgres psql -U wiki -d wikidb \
	  -c "SELECT * FROM recent_stats LIMIT 15;"

check-minio: ## List Delta files in MinIO
	@# Use host.docker.internal instead of localhost — works on macOS Docker Desktop
	docker run --rm \
	  minio/mc alias set local http://host.docker.internal:9000 minioadmin minioadmin \
	  > /dev/null 2>&1 && \
	docker run --rm \
	  minio/mc ls --recursive local/wiki-stream/ 2>/dev/null || \
	  echo "No Delta files yet — has the pipeline run for at least 30 seconds?"

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
