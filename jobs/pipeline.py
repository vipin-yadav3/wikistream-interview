"""
WikiStream Analytics Pipeline
==============================

You are building a real-time analytics pipeline that ingests Wikipedia page
edits from Kafka and materialises two outputs:

  1. Postgres  — 5-minute windowed edit statistics per wiki (live aggregates)
  2. MinIO     — raw edit events in Delta Lake format (queryable data lake)

The Wikipedia producer is already running (make producer).
Verify events are flowing:  make check-kafka

Run this pipeline:  make pipeline

── What you implement ─────────────────────────────────────────────────────────

  build_spark()           Task 0  SparkSession with Delta + S3A + Kafka + JDBC
  build_kafka_source()    Task 1a Read raw stream from Kafka
  parse_events()          Task 1b Parse JSON, derive columns, filter edits
  build_aggregation()     Task 2  Windowed stats with watermark
  write_to_postgres()     Task 3  foreachBatch upsert → Postgres
  write_to_delta()        Task 4  Append raw events → Delta on MinIO

  (Curveball added at 40 min — see CURVEBALL.md when revealed)
"""

import os
import psycopg2

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, LongType, StringType, StructField, StructType,
)

# ── Connection config (loaded from .env) ─────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP",  "localhost:9092")
KAFKA_TOPIC     = "wiki.recentchanges"
PG_URL          = os.getenv("POSTGRES_URL",      "jdbc:postgresql://localhost:5432/wikidb")
PG_PROPS        = {"user": "wiki", "password": "wiki", "driver": "org.postgresql.Driver"}
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",    "http://localhost:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY",  "minioadmin")
DELTA_PATH      = "s3a://wiki-stream/raw/events/"
CHECKPOINT_DIR  = "checkpoints"

# ── Kafka message schema ──────────────────────────────────────────────────────
# Every message published by wiki_producer.py has this shape.
EDIT_SCHEMA = StructType([
    StructField("id",        LongType(),   True),
    StructField("type",      StringType(), True),
    StructField("wiki",      StringType(), True),
    StructField("title",     StringType(), True),
    StructField("user",      StringType(), True),
    StructField("bot",       BooleanType(), True),
    StructField("timestamp", LongType(),   True),
    StructField("length", StructType([
        StructField("old", LongType(), True),
        StructField("new", LongType(), True),
    ]), True),
    StructField("meta", StructType([
        StructField("dt", StringType(), True),   # ISO-8601 event timestamp
    ]), True),
    StructField("server_name", StringType(), True),
    StructField("ingested_at", StringType(), True),
])


# ── Task 0 — SparkSession ─────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    """
    Create and configure the SparkSession.

    Required configuration
    ----------------------
    The PYSPARK_SUBMIT_ARGS env var (set in .env) downloads these packages:
      • spark-sql-kafka-0-10  — Kafka source/sink
      • delta-spark           — Delta Lake format
      • hadoop-aws            — S3A filesystem
      • aws-java-sdk-bundle   — AWS SDK (needed by S3A even for MinIO)
      • postgresql            — JDBC driver

    You must also set these configs on the SparkSession builder:

    Delta Lake extensions (required to use format("delta")):
      spark.sql.extensions
          → io.delta.sql.DeltaSparkSessionExtension
      spark.sql.catalog.spark_catalog
          → org.apache.spark.sql.delta.catalog.DeltaCatalog

    S3A / MinIO settings (required to read/write s3a:// paths):
      spark.hadoop.fs.s3a.endpoint          → MINIO_ENDPOINT
      spark.hadoop.fs.s3a.access.key        → MINIO_ACCESS
      spark.hadoop.fs.s3a.secret.key        → MINIO_SECRET
      spark.hadoop.fs.s3a.path.style.access → "true"
      spark.hadoop.fs.s3a.impl              → org.apache.hadoop.fs.s3a.S3AFileSystem

    NOTE: path.style.access = true is required for MinIO.
    AWS S3 uses virtual-hosted style (bucket.s3.amazonaws.com);
    MinIO uses path style (localhost:9000/bucket). Without this you get 403.
    """
    # TODO
    pass


# ── Task 1a — Kafka source ────────────────────────────────────────────────────

def build_kafka_source(spark: SparkSession) -> DataFrame:
    """
    Return a raw streaming DataFrame from Kafka.

    Requirements
    ------------
    - Subscribe to KAFKA_TOPIC
    - startingOffsets: "latest"  (do not replay historical messages on restart)
    - Set failOnDataLoss to False (topic may not exist yet if producer is slow)

    The returned DataFrame has Kafka metadata columns:
    key, value (bytes), topic, partition, offset, timestamp, timestampType
    You do NOT parse the value here — that is parse_events()'s job.
    """
    # TODO
    pass


# ── Task 1b — Parse events ────────────────────────────────────────────────────

def parse_events(raw_df: DataFrame) -> DataFrame:
    """
    Parse the Kafka value column and produce a clean events DataFrame.

    Steps
    -----
    1. Cast value (bytes) to string, then parse as JSON using EDIT_SCHEMA.
       HINT: F.from_json(F.col("value").cast("string"), EDIT_SCHEMA)

    2. Expand the parsed struct to top-level columns.

    3. Derive event_time (TimestampType) from meta.dt:
       HINT: F.to_timestamp(F.col("meta.dt"))
       Do NOT use the unix `timestamp` field — meta.dt is the authoritative
       business time. Using timestamp (unix epoch) directly would lose timezone
       info and produce wrong window assignments.

    4. Filter: keep only rows where type == "edit".
       (~30% of Wikipedia events are new-page, log, or categorize events.)

    5. Select and name the final output columns:
         event_time  TimestampType  ← from meta.dt
         wiki        StringType
         title       StringType
         user        StringType
         bot         BooleanType
         bytes_old   LongType       ← from length.old
         bytes_new   LongType       ← from length.new

    Returns: streaming DataFrame with exactly those 7 columns.
    """
    # TODO
    pass


# ── Task 2 — Windowed aggregation ─────────────────────────────────────────────

def build_aggregation(parsed_df: DataFrame) -> DataFrame:
    """
    Compute 5-minute tumbling window statistics per wiki.

    Requirements
    ------------
    Watermark (MUST come before groupBy):
      .withWatermark("event_time", "10 minutes")

    Window:
      F.window("event_time", "5 minutes")   — tumbling, no slide offset

    Group by: window + wiki

    Aggregations:
      edit_count      = count(*)
      bot_edit_count  = sum of bot cast to int  (True=1, False=0)
      net_bytes_added = sum(bytes_new - bytes_old)

    Flatten the window struct in the output:
      window.start → window_start
      window.end   → window_end

    Output columns: window_start, window_end, wiki,
                    edit_count, bot_edit_count, net_bytes_added

    ── Why watermark MUST come before groupBy ──────────────────────────────
    Spark uses the watermark to decide when it is safe to evict window state
    from memory. If placed after the aggregation, Spark has no way to track
    the event time of individual rows in the aggregated result — it silently
    ignores the watermark and your state grows unbounded.
    """
    # TODO
    pass


# ── Task 3 — Write to Postgres via foreachBatch ───────────────────────────────

def _upsert_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch handler: write one micro-batch of windowed stats to Postgres.

    Why foreachBatch and not a direct JDBC streaming sink?
    -------------------------------------------------------
    Spark's built-in JDBC sink only supports `append` mode. Our aggregation
    uses `update` mode (windows are updated as late events arrive). foreachBatch
    gives us a static DataFrame per micro-batch where we can use any JDBC
    write semantics we choose.

    Implementation pattern (upsert via staging table)
    --------------------------------------------------
    Step 1: Write the batch to wiki_edit_counts_staging using mode="overwrite".
            Overwrite = idempotent: if Spark replays this batch, we overwrite
            the same staging rows rather than appending duplicates.

    Step 2: Open a psycopg2 connection and execute:
            INSERT INTO wiki_edit_counts (...)
            SELECT ... FROM wiki_edit_counts_staging
            ON CONFLICT (window_start, wiki) DO UPDATE SET
                edit_count      = EXCLUDED.edit_count,
                bot_edit_count  = EXCLUDED.bot_edit_count,
                net_bytes_added = EXCLUDED.net_bytes_added,
                last_updated    = NOW();

    Step 3: Commit and close the connection.

    This ensures that if the job crashes and Spark re-processes a batch,
    the Postgres row is overwritten with the same values — not duplicated.

    NOTE: psycopg2 DSN: postgresql://wiki:wiki@localhost:5432/wikidb
    """
    if batch_df.isEmpty():
        return

    # TODO: Step 1 — write to staging with mode="overwrite"

    # TODO: Step 2 — execute UPSERT via psycopg2

    # TODO: Step 3 — commit + close


def write_to_postgres(agg_df: DataFrame):
    """
    Wire foreachBatch to the aggregated stream.

    Requirements:
    - outputMode: "update"  (windows are updated as more events arrive)
    - trigger: processingTime="30 seconds"
    - checkpointLocation: checkpoints/postgres
    """
    # TODO
    pass


# ── Task 4 — Write raw events to Delta Lake on MinIO ─────────────────────────

def write_to_delta(parsed_df: DataFrame):
    """
    Append raw edit events to Delta Lake stored in MinIO.

    Requirements
    ------------
    - format: "delta"  (not "parquet" — Delta adds the transaction log)
    - path: DELTA_PATH  (s3a://wiki-stream/raw/events/)
    - outputMode: "append"
    - partitionBy: "wiki"   (correct cardinality: ~20 wikis vs millions of titles)
    - trigger: processingTime="30 seconds"
    - checkpointLocation: checkpoints/delta  ← DIFFERENT from postgres checkpoint

    Why partition by wiki?
    ----------------------
    Analytics queries almost always filter by wiki first (e.g. "show me enwiki
    stats"). Partitioning aligns with this access pattern, so Spark only reads
    the relevant partition directories — partition pruning.

    Why Delta over plain Parquet?
    -----------------------------
    Delta adds a transaction log (_delta_log/) that provides:
    - ACID writes: concurrent writers don't corrupt each other
    - Schema enforcement: rejects rows that don't match the table schema
    - Time travel: query data as it was at a previous point in time
    - Compaction: OPTIMIZE command merges small files from micro-batches

    Verify after ~1 min:  make check-minio
    """
    # TODO
    pass


# ── Curveball additions (revealed at 40 min) ──────────────────────────────────
# Do not implement until CURVEBALL.md is revealed.

def write_to_kafka_sink(parsed_df: DataFrame):
    """
    Curveball Part B: Filter enwiki edits and publish to wiki.en-only topic.
    Implement when instructed.
    """
    pass


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw    = build_kafka_source(spark)
    parsed = parse_events(raw)
    agg    = build_aggregation(parsed)

    q1 = write_to_postgres(agg)
    q2 = write_to_delta(parsed)
    # q3 = write_to_kafka_sink(parsed)   # ← uncomment at curveball

    print("\nPipeline running.")
    print("  Verify Postgres:  make check-pg")
    print("  Verify MinIO:     make check-minio")
    print("  Ctrl-C to stop.\n")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
