# WikiStream Analytics Pipeline

Read **[PROBLEM.md](PROBLEM.md)** for the full problem statement.

---

## Prerequisites

Install these before starting:

| Requirement | Version | Install |
|---|---|---|
| **Python** | 3.9 or higher | https://python.org/downloads |
| **Java** | 11 or 17 only ⚠️ | https://adoptium.net/temurin/releases/?version=17 |
| **Docker Desktop** | Latest | https://docker.com/products/docker-desktop |
| **Git** | Any | https://git-scm.com |

Verify your setup:
```bash
python3 --version    # must be 3.9+
java -version        # must be 11 or 17 (Java 21/25 not supported by PySpark 3.5)
docker info          # must show server info (Docker must be running)
```

---

## Your branch

The interviewer will tell you your branch name before the interview starts.
It will be in the format `solution/<your-name>`, e.g. `solution/jane-doe`.

Check it out after cloning:

```bash
git checkout solution/your-name
```

All your work goes on this branch. Push normally as you go:

```bash
git add jobs/pipeline.py
git commit -m "wip"
git push
```

---

## Quick start

```bash
# 1. Copy environment config
cp .env.example .env

# 2. Create and activate a virtual environment (recommended)
python3 -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows

# 3. Install Python dependencies
python3 -m pip install -r requirements.txt

# 4. Start all services (Kafka, Postgres, MinIO) + pre-download Spark JARs
#    First run takes ~3 minutes to download JARs — this is expected, not a hang.
make setup

# 5. Terminal 1: start the Wikipedia event producer
make producer

# 6. Verify events are flowing (should see JSON within 5 seconds)
make check-kafka
```

> **Note:** `make setup` downloads ~500 MB of Spark JARs on the first run.
> It looks like it has stalled — it hasn't. Subsequent runs use the cache and are instant.

> **If `pip install` fails with a PEP 668 error** (common on newer macOS/Linux):
> the venv in step 2 above fixes this. Make sure you've run `source .venv/bin/activate`
> before running `pip install`.

---

## Available commands

```
make setup        Install deps, start Docker, pre-download Spark JARs
make up           Start Kafka, Postgres, MinIO
make producer     Stream Wikipedia edits into Kafka (run in second terminal)
make pipeline     Run your Spark streaming pipeline
make check-kafka  Verify Kafka is receiving events
make check-pg     Show latest rows in Postgres
make check-minio  List Delta files in MinIO
make test         Run automated unit tests against your code
make reset        Wipe state for a fresh start
make down         Stop all services
make check-env    Verify Python, Java, Docker prerequisites
```

---

## Stack

- **PySpark 3.5** — streaming engine
- **Kafka** (Confluent) — event bus on `localhost:9092`
- **Postgres 15** — live aggregation sink on `localhost:5432`
- **MinIO** — S3-compatible Delta Lake storage
- **MinIO console** — http://localhost:9001 (minioadmin / minioadmin)

---

*You may use AI tools freely — use them as you would on a normal working day.*
