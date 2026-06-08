"""
Pre-downloads Spark JARs so the first `make pipeline` run isn't slow.
Run automatically by `make setup`.
"""

import os
import subprocess
import sys

os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    "--packages "
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "io.delta:delta-spark_2.12:3.2.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "org.postgresql:postgresql:42.7.3 "
    "pyspark-shell"
)

from pyspark.sql import SparkSession

print("Downloading Spark JARs (this runs once and caches locally) …")
spark = (
    SparkSession.builder
    .appName("jar-prefetch")
    .master("local[1]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.stop()
print("✓ JARs ready.")
