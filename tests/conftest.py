"""
Pytest configuration for WikiStream tests.

The SparkSession here is configured with Delta Lake but WITHOUT S3A,
so tests run offline (no MinIO required for unit tests).
Integration tests that need Postgres/MinIO are marked @pytest.mark.integration.
"""

import os
import pytest

os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    "--packages "
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "io.delta:delta-spark_2.12:3.2.0,"
    "org.postgresql:postgresql:42.7.3 "
    "pyspark-shell"
)

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("wikistream-tests")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
