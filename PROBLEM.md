# WikiStream Analytics Pipeline

**Duration:** 60 minutes
**Stack:** Python · PySpark 3.5 · Kafka · Postgres · Delta Lake (MinIO)
**AI tools:** Allowed and encouraged — use them as you would on a normal workday.

---

## Scenario

Your team monitors Wikipedia edit activity in real-time. Wikipedia publishes
every page edit as a live event stream — your job is to build the pipeline
that ingests this stream, aggregates edit statistics by wiki, and stores the
results in two places:

- **Postgres** — live 5-minute windowed stats for the analytics dashboard
- **MinIO (Delta Lake)** — raw edit history for the data lake

A producer script is already provided that streams real Wikipedia edits into
Kafka. Your job is to build the Spark pipeline that reads and processes them.

---

## Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start all services (Kafka, Postgres, MinIO)
make up

# 3. Copy environment config
cp .env.example .env

# 4. Terminal 1: start the Wikipedia event producer
make producer

# 5. Verify events are flowing
make check-kafka
```

MinIO console (browse your Delta files): http://localhost:9001 — minioadmin/minioadmin

---

## What is already provided

| File | Status | Description |
|---|---|---|
| `scripts/wiki_producer.py` | **Pre-built** | Streams Wikipedia edits → Kafka |
| `sql/init.sql` | **Pre-built** | Postgres tables |
| `jobs/pipeline.py` | **You build this** | The Spark streaming pipeline |

---

## Kafka message shape

Every message on topic `wiki.recentchanges` is JSON with this structure:

```json
{
  "id":        12345,
  "type":      "edit",
  "wiki":      "enwiki",
  "title":     "Python (programming language)",
  "user":      "SomeEditor",
  "bot":       false,
  "timestamp": 1717600000,
  "length":    { "old": 45210, "new": 45350 },
  "meta":      { "dt": "2024-06-05T12:00:00Z" },
  "server_name": "en.wikipedia.org"
}
```

Note: ~30% of events have `type != "edit"` (new pages, log entries) — filter those out.

---

## Your tasks — implement these functions in `jobs/pipeline.py`

### Task 0 — `build_spark()`

Create the SparkSession with all required configuration:
- Delta Lake extensions
- S3A connector pointed at MinIO (path-style access — required for MinIO)

---

### Task 1 — `build_kafka_source()` + `parse_events()`

**`build_kafka_source(spark)`** — Read the raw stream from `wiki.recentchanges`.

**`parse_events(raw_df)`** — Parse JSON, produce a clean DataFrame with these columns:

| Column | Type | Source |
|---|---|---|
| `event_time` | Timestamp | `meta.dt` (ISO string → timestamp) |
| `wiki` | String | |
| `title` | String | |
| `user` | String | |
| `bot` | Boolean | |
| `bytes_old` | Long | `length.old` |
| `bytes_new` | Long | `length.new` |

Filter: keep only `type == "edit"` events.

---

### Task 2 — `build_aggregation(parsed_df)`

Compute 5-minute tumbling window statistics per wiki.

- **Watermark:** 10 minutes on `event_time`
- **Window:** 5-minute tumbling
- **Group by:** window + wiki
- **Output:** `window_start`, `window_end`, `wiki`, `edit_count`, `bot_edit_count`, `net_bytes_added`

Flatten the window struct: `window.start → window_start`, `window.end → window_end`.

---

### Task 3 — `write_to_postgres(agg_df)` + `_upsert_batch()`

Write windowed stats to Postgres using `foreachBatch`.

The table `wiki_edit_counts` has primary key `(window_start, wiki)`.
Implement an upsert so late-arriving events update the row without creating duplicates.

Pattern:
1. Write batch to `wiki_edit_counts_staging` (mode: overwrite)
2. `INSERT ... ON CONFLICT (window_start, wiki) DO UPDATE SET ...`
3. Commit and close the connection

Output mode: `update`. Checkpoint: `checkpoints/postgres`.

---

### Task 4 — `write_to_delta(parsed_df)`

Write raw events to Delta Lake on MinIO.

- Path: `s3a://wiki-stream/raw/events/`
- Format: `delta`
- Output mode: `append`
- Partition by: `wiki`
- Checkpoint: `checkpoints/delta`

Verify: `make check-minio` — you should see `_delta_log/` after ~1 minute.

---

## Running the pipeline

```bash
make pipeline
```

Verify it's working:

```bash
make check-pg      # latest rows in Postgres
make check-minio   # Delta files in MinIO
make test          # run unit tests against your code
```

---

## Tips

- Each `writeStream` needs its own unique `checkpointLocation`
- `spark.streams.awaitAnyTermination()` keeps all queries running
- `make reset` wipes state if you need a clean restart
- `make test` runs 18 unit tests against your transformation functions — use it freely
