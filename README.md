# WikiStream Analytics Pipeline

Read **[PROBLEM.md](PROBLEM.md)** for the full problem statement.

---

## Setup — pick the option that works for you

### Option A — VS Code devcontainer ✅ recommended

Zero manual setup. The container has the right Python and Java pre-installed.

1. Install [VS Code](https://code.visualstudio.com) and [Docker Desktop](https://docker.com/products/docker-desktop)
2. Install the **Dev Containers** extension in VS Code
3. Open the repo folder in VS Code → click **"Reopen in Container"** when prompted
4. Wait ~2 min for the container to build — everything installs automatically
5. Open the VS Code terminal and run:
   ```bash
   make producer       # Terminal 1 — start Wikipedia stream
   make check-kafka    # Verify events are flowing
   ```

That's it. Skip to **[Your branch](#your-branch)** below.

---

### Option B — Run locally (manual setup)

Only needed if you're not using VS Code or prefer to run locally.

**Prerequisites:**

| Requirement | Version | Notes |
|---|---|---|
| **Python** | 3.9 – 3.12 | Python 3.13/3.14 not yet fully supported |
| **Java** | 11 or 17 only | Java 21+ breaks PySpark 3.5 |
| **Docker Desktop** | Latest | https://docker.com/products/docker-desktop |

Install Java 17 if needed:
```bash
# macOS
brew install --cask temurin@17

# Other platforms
# https://adoptium.net/temurin/releases/?version=17
```

**Setup steps:**

```bash
# 1. Copy environment config
cp .env.example .env

# 2. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows

# 3. Install Python dependencies
python3 -m pip install -r requirements.txt

# 4. Start services + download Spark JARs (~3 min first time, not a hang)
make setup

# 5. Terminal 1: start the Wikipedia event producer
make producer

# 6. Verify events are flowing
make check-kafka
```

> **Troubleshooting:**
> - `pip install` fails with PEP 668 error → activate the venv first (`source .venv/bin/activate`)
> - `make check-env` fails on Java → install Java 17 (link above), not Java 21+
> - `make setup` looks frozen → it's downloading ~500MB of Spark JARs, wait 3 min

---

## Your branch

You will be given a repo link and a branch name before the interview.
Check it out after cloning:

```bash
git clone https://github.com/YOUR_ACCOUNT/wikistream-your-name
cd wikistream-your-name
git checkout main          # your repo has only main — no other branches
```

Commit and push your work normally as you go:

```bash
git add jobs/pipeline.py
git commit -m "wip"
git push
```

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
make check-env    Diagnose setup issues
```

---

## Stack

- **PySpark 3.5** — streaming engine
- **Kafka** (Confluent) — event bus on `localhost:9092`
- **Postgres 15** — live aggregation sink on `localhost:5432`
- **MinIO** — S3-compatible Delta Lake storage, console at http://localhost:9001

---

*You may use AI tools freely — use them as you would on a normal working day.*
