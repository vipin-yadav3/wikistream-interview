"""
SOLUTION — WikiStream Analytics Pipeline (Tasks 1-4 + Curveball)
For interviewer reference only.
"""

import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, LongType, StringType, StructField, StructType,
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP",  "localhost:9092")
KAFKA_TOPIC     = "wiki.recentchanges"
PG_URL          = os.getenv("POSTGRES_URL",      "jdbc:postgresql://localhost:5432/wikidb")
PG_PROPS        = {"user": "wiki", "password": "wiki", "driver": "org.postgresql.Driver"}
PG_DSN          = "postgresql://wiki:wiki@localhost:5432/wikidb"
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",    "http://localhost:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY",  "minioadmin")
DELTA_PATH      = "s3a://wiki-stream/raw/events/"
CHECKPOINT_DIR  = "checkpoints"

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
        StructField("dt", StringType(), True),
    ]), True),
    StructField("server_name", StringType(), True),
    StructField("ingested_at", StringType(), True),
])


# ── Task 0 ────────────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("wiki-stream")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # S3A / MinIO — path.style.access is critical for MinIO
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# ── Task 1a ───────────────────────────────────────────────────────────────────

def build_kafka_source(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


# ── Task 1b ───────────────────────────────────────────────────────────────────

def parse_events(raw_df: DataFrame) -> DataFrame:
    parsed = (
        raw_df
        .select(F.from_json(F.col("value").cast("string"), EDIT_SCHEMA).alias("e"))
        .select("e.*")
    )
    return (
        parsed
        .filter(F.col("type") == "edit")
        .select(
            F.to_timestamp(F.col("meta.dt")).alias("event_time"),
            F.col("wiki"),
            F.col("title"),
            F.col("user"),
            F.col("bot"),
            F.col("length.old").alias("bytes_old"),
            F.col("length.new").alias("bytes_new"),
        )
    )


# ── Task 2 ────────────────────────────────────────────────────────────────────

def build_aggregation(parsed_df: DataFrame) -> DataFrame:
    return (
        parsed_df
        .withWatermark("event_time", "10 minutes")          # BEFORE groupBy
        .groupBy(F.window("event_time", "5 minutes"), "wiki")
        .agg(
            F.count("*").alias("edit_count"),
            F.sum(F.col("bot").cast("int")).alias("bot_edit_count"),
            F.sum(F.col("bytes_new") - F.col("bytes_old")).alias("net_bytes_added"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "wiki", "edit_count", "bot_edit_count", "net_bytes_added",
        )
    )


# ── Task 3 ────────────────────────────────────────────────────────────────────

def _upsert_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return

    # Step 1: overwrite staging — idempotent if batch replays
    batch_df.write.jdbc(
        url=PG_URL, table="wiki_edit_counts_staging",
        mode="overwrite", properties=PG_PROPS,
    )

    # Step 2: merge staging → main table
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO wiki_edit_counts
                    (window_start, window_end, wiki,
                     edit_count, bot_edit_count, net_bytes_added, last_updated)
                SELECT
                    window_start, window_end, wiki,
                    edit_count, bot_edit_count, net_bytes_added, NOW()
                FROM wiki_edit_counts_staging
                ON CONFLICT (window_start, wiki) DO UPDATE SET
                    edit_count      = EXCLUDED.edit_count,
                    bot_edit_count  = EXCLUDED.bot_edit_count,
                    net_bytes_added = EXCLUDED.net_bytes_added,
                    last_updated    = NOW()
            """)
        conn.commit()
    finally:
        conn.close()


def write_to_postgres(agg_df: DataFrame):
    return (
        agg_df.writeStream
        .outputMode("update")
        .foreachBatch(_upsert_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/postgres")
        .trigger(processingTime="30 seconds")
        .start()
    )


# ── Task 4 ────────────────────────────────────────────────────────────────────

def write_to_delta(parsed_df: DataFrame):
    return (
        parsed_df
        .withWatermark("event_time", "30 seconds")
        .writeStream
        .outputMode("append")
        .format("delta")
        .partitionBy("wiki")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/delta")
        .trigger(processingTime="30 seconds")
        .start(DELTA_PATH)
    )


# ── Curveball A: idempotency ─────────────────────────────────────────────────
# _upsert_batch already uses mode="overwrite" on staging → already idempotent.
# If the candidate used mode="append", they need to change it here.


# ── Curveball B: enwiki → wiki.en-only Kafka topic ───────────────────────────

def write_to_kafka_sink(parsed_df: DataFrame):
    """
    Filter enwiki edits and publish key fields to wiki.en-only.

    Key design decisions:
    - Independent writeStream on parsed_df (no restart needed)
    - Own checkpoint dir (checkpoints/kafka-sink)
    - Serialize value as JSON using to_json(struct(...))
    - Key = wiki (consistent with producer keying)
    """
    en_edits = (
        parsed_df
        .filter(F.col("wiki") == "enwiki")
        .select(
            F.col("wiki").cast("string").alias("key"),
            F.to_json(F.struct(
                F.col("title"),
                F.col("user"),
                F.col("event_time"),
                (F.col("bytes_new") - F.col("bytes_old")).alias("net_bytes"),
            )).alias("value"),
        )
    )
    return (
        en_edits.writeStream
        .outputMode("append")
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", "wiki.en-only")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/kafka-sink")
        .trigger(processingTime="30 seconds")
        .start()
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw    = build_kafka_source(spark)
    parsed = parse_events(raw)
    agg    = build_aggregation(parsed)

    q1 = write_to_postgres(agg)
    q2 = write_to_delta(parsed)
    q3 = write_to_kafka_sink(parsed)   # curveball

    print("Pipeline running. Ctrl-C to stop.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
