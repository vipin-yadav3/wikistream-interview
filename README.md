# WikiStream — Data Engineering Interview

Welcome. Read **[PROBLEM.md](PROBLEM.md)** for full instructions.

## Quick start

```bash
# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Start all services (Kafka, Postgres, MinIO)
make up

# 3. Copy environment config
cp .env.example .env

# 4. Terminal 1 — start the Wikipedia event producer
make producer

# 5. Verify events are flowing
make check-kafka

# 6. Run the pipeline (implement jobs/pipeline.py first)
make pipeline
```

## Available commands

```
make up           Start Kafka, Postgres, MinIO
make producer     Stream Wikipedia edits into Kafka
make pipeline     Run your Spark streaming pipeline
make check-pg     Show latest rows in Postgres
make check-minio  List Delta files in MinIO
make check-kafka  Tail Kafka messages
make test         Run automated unit tests
make score        Live score report
make reset        Wipe state for a fresh start
make down         Stop all services
```

## Stack

- **PySpark 3.5** — streaming engine
- **Kafka** (bitnami, KRaft mode) — event bus
- **Postgres 15** — live aggregation sink
- **MinIO** — S3-compatible object store for Delta Lake

---

*You may use AI tools freely — use them as you would on a normal working day.*
