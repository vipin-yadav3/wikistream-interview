.PHONY: help setup up down producer pipeline test score score-auto score-branch submit check-pg check-minio check-kafka reset clean

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
	docker exec wiki-kafka kafka-console-consumer.sh \
	  --bootstrap-server localhost:9092 \
	  --topic wiki.recentchanges \
	  --max-messages 10 \
	  --from-beginning

check-pg: ## Show latest rows in wiki_edit_counts
	docker exec wiki-postgres psql -U wiki -d wikidb \
	  -c "SELECT * FROM recent_stats LIMIT 15;"

check-minio: ## List Delta files in MinIO
	docker exec wiki-minio-init mc ls --recursive local/wiki-stream/ 2>/dev/null || \
	  docker run --rm --network host minio/mc alias set local http://localhost:9000 minioadmin minioadmin \
	  && docker run --rm --network host minio/mc ls --recursive local/wiki-stream/

# ── Tests & Scoring ───────────────────────────────────────────────────────────

test: ## Run automated unit tests (no Docker needed for transform tests)
	$(PYTEST) tests/test_transforms.py -v

test-all: ## Run all tests including integration (requires Docker up)
	$(PYTEST) tests/ -v

score: ## Full interactive score (automated + manual prompts) — run at end of interview
	$(PYTHON) score.py

score-auto: ## Automated tests only — no prompts (same as GitHub Actions)
	$(PYTHON) score_auto.py

submit: ## Push your solution branch so the interviewer can score it
ifndef NAME
	$(error NAME is required. Usage: make submit NAME="Jane Doe")
endif
	@BRANCH="solution/$$(echo '$(NAME)' | tr ' ' '-' | tr '[:upper:]' '[:lower:]')" && \
	git checkout -b $$BRANCH 2>/dev/null || git checkout $$BRANCH && \
	git add jobs/pipeline.py && \
	git commit -m "Solution: $(NAME)" && \
	git push origin $$BRANCH && \
	echo "" && \
	echo "✓ Solution pushed to branch: $$BRANCH" && \
	echo "  GitHub Actions will auto-score it in ~3 min." && \
	echo "  View at: https://github.com/vipin-yadav3/wikistream-interview/actions"

# ── Interviewer: score a candidate branch locally ─────────────────────────────

score-branch: ## Score a candidate branch locally  [BRANCH=solution/jane-doe]
ifndef BRANCH
	$(error BRANCH is required. Usage: make score-branch BRANCH=solution/jane-doe)
endif
	@echo "Fetching branch $(BRANCH) ..."
	git fetch origin $(BRANCH)
	git checkout $(BRANCH)
	@CANDIDATE=$$(echo '$(BRANCH)' | sed 's|solution/||' | tr '-' ' ') && \
	echo "Scoring candidate: $$CANDIDATE" && \
	CANDIDATE_NAME="$$CANDIDATE" $(PYTHON) score_auto.py && \
	echo "" && \
	echo "Automated portion done. Run 'make score' for the full interactive score."

# ── Housekeeping ──────────────────────────────────────────────────────────────

reset: ## Wipe checkpoints + Postgres data + MinIO bucket (fresh start)
	rm -rf checkpoints/
	docker exec wiki-postgres psql -U wiki -d wikidb \
	  -c "TRUNCATE wiki_edit_counts, wiki_edit_counts_staging, bot_alerts;" 2>/dev/null || true
	docker run --rm --network host minio/mc alias set local http://localhost:9000 minioadmin minioadmin \
	  && docker run --rm --network host minio/mc rm --recursive --force local/wiki-stream/ 2>/dev/null || true
	@echo "Reset complete."

clean: ## Remove Python cache and temp files
	find . -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache checkpoints spark-warehouse derby.log metastore_db
