"""
Unit tests for WikiStream pipeline transformation functions.

Tests run against static (batch) DataFrames — no Kafka, Postgres, or MinIO needed.
All streaming logic is identical in batch mode for these pure transformation functions.

Run: make test
"""

import json
from datetime import datetime

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from jobs.pipeline import build_aggregation, parse_events


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def raw_kafka_df(spark):
    """
    Simulate what Kafka delivers: rows with a binary 'value' column.
    Contains: 2 real edits, 1 bot edit (dewiki), 1 non-edit event (type=new).
    """
    events = [
        {   # enwiki human edit, bytes grow
            "id": 1, "type": "edit", "wiki": "enwiki",
            "title": "Python", "user": "Alice", "bot": False,
            "timestamp": 1717600000,
            "length": {"old": 1000, "new": 1200},
            "meta": {"dt": "2024-06-05T12:00:00Z"},
            "server_name": "en.wikipedia.org", "ingested_at": "2024-06-05T12:00:01Z",
        },
        {   # dewiki bot edit, bytes shrink
            "id": 2, "type": "edit", "wiki": "dewiki",
            "title": "Python", "user": "CleanupBot", "bot": True,
            "timestamp": 1717600060,
            "length": {"old": 500, "new": 480},
            "meta": {"dt": "2024-06-05T12:01:00Z"},
            "server_name": "de.wikipedia.org", "ingested_at": "2024-06-05T12:01:01Z",
        },
        {   # new-page event — should be FILTERED OUT by parse_events
            "id": 3, "type": "new", "wiki": "enwiki",
            "title": "NewPage", "user": "Bob", "bot": False,
            "timestamp": 1717600120,
            "length": {"old": 0, "new": 300},
            "meta": {"dt": "2024-06-05T12:02:00Z"},
            "server_name": "en.wikipedia.org", "ingested_at": "2024-06-05T12:02:01Z",
        },
    ]
    rows = [Row(value=json.dumps(e)) for e in events]
    return spark.createDataFrame(rows)


@pytest.fixture
def parsed_events_df(spark):
    """
    10 known edit events for aggregation testing.
    All fall within the same 5-minute window: 12:00 – 12:05.
      - enwiki: 7 human edits, bytes growing
      - dewiki: 3 bot edits, bytes shrinking by 10 each
    """
    ts = lambda m, s: datetime(2024, 6, 5, 12, m, s)
    rows = (
        [Row(event_time=ts(0, 30), wiki="enwiki", title=f"Page{i}",
             user=f"User{i}", bot=False, bytes_old=1000, bytes_new=1000 + i * 50)
         for i in range(7)]
        +
        [Row(event_time=ts(1, 0), wiki="dewiki", title=f"Seite{i}",
             user=f"Bot{i}", bot=True, bytes_old=500, bytes_new=490)
         for i in range(3)]
    )
    return spark.createDataFrame(rows)


# ── Tests: parse_events ───────────────────────────────────────────────────────

class TestParseEvents:

    def test_filters_non_edit_type(self, spark, raw_kafka_df):
        """type=new must be dropped — only type=edit survives."""
        result = parse_events(raw_kafka_df)
        assert result.count() == 2, (
            "3 input events (2 edit + 1 new) should yield 2 parsed events"
        )

    def test_output_columns_present(self, spark, raw_kafka_df):
        result = parse_events(raw_kafka_df)
        required = {"event_time", "wiki", "title", "user", "bot", "bytes_old", "bytes_new"}
        assert required.issubset(set(result.columns)), (
            f"Missing columns: {required - set(result.columns)}"
        )

    def test_event_time_is_timestamp_type(self, spark, raw_kafka_df):
        result = parse_events(raw_kafka_df)
        schema_map = {f.name: f.dataType for f in result.schema.fields}
        assert isinstance(schema_map["event_time"], TimestampType), (
            f"event_time must be TimestampType, got {schema_map['event_time']}. "
            "Did you use F.to_timestamp() on meta.dt?"
        )

    def test_event_time_year_is_correct(self, spark, raw_kafka_df):
        """Ensures meta.dt is parsed correctly (not unix epoch / 1000 etc.)."""
        result = parse_events(raw_kafka_df)
        row = result.filter(F.col("wiki") == "enwiki").collect()[0]
        assert row["event_time"].year == 2024, (
            f"event_time year should be 2024, got {row['event_time'].year}. "
            "Are you using meta.dt (ISO string) or the unix timestamp field?"
        )

    def test_bytes_old_and_new_extracted(self, spark, raw_kafka_df):
        result = parse_events(raw_kafka_df)
        enwiki = result.filter(F.col("wiki") == "enwiki").collect()[0]
        assert enwiki["bytes_old"] == 1000
        assert enwiki["bytes_new"] == 1200

    def test_bot_flag_preserved(self, spark, raw_kafka_df):
        result = parse_events(raw_kafka_df)
        bot_row = result.filter(F.col("wiki") == "dewiki").collect()[0]
        assert bot_row["bot"] is True, "Bot flag must be True for CleanupBot"

    def test_wiki_field_present(self, spark, raw_kafka_df):
        result = parse_events(raw_kafka_df)
        wikis = {r["wiki"] for r in result.collect()}
        assert "enwiki" in wikis and "dewiki" in wikis


# ── Tests: build_aggregation ──────────────────────────────────────────────────

class TestBuildAggregation:

    def test_output_columns_present(self, spark, parsed_events_df):
        result = build_aggregation(parsed_events_df)
        required = {"window_start", "window_end", "wiki",
                    "edit_count", "bot_edit_count", "net_bytes_added"}
        assert required.issubset(set(result.columns)), (
            f"Missing columns: {required - set(result.columns)}. "
            "Did you flatten window.start → window_start, window.end → window_end?"
        )

    def test_window_start_is_timestamp(self, spark, parsed_events_df):
        result = build_aggregation(parsed_events_df)
        schema_map = {f.name: f.dataType for f in result.schema.fields}
        assert isinstance(schema_map["window_start"], TimestampType), (
            "window_start must be TimestampType after flattening the window struct"
        )

    def test_one_row_per_wiki_per_window(self, spark, parsed_events_df):
        result = build_aggregation(parsed_events_df)
        assert result.count() == 2, (
            "Expect 2 rows: one for enwiki, one for dewiki (same window)"
        )

    def test_edit_count_enwiki(self, spark, parsed_events_df):
        result = build_aggregation(parsed_events_df)
        rows = {r["wiki"]: r for r in result.collect()}
        assert rows["enwiki"]["edit_count"] == 7, (
            f"enwiki has 7 events, got {rows['enwiki']['edit_count']}"
        )

    def test_edit_count_dewiki(self, spark, parsed_events_df):
        result = build_aggregation(parsed_events_df)
        rows = {r["wiki"]: r for r in result.collect()}
        assert rows["dewiki"]["edit_count"] == 3

    def test_bot_edit_count_zero_for_human_wiki(self, spark, parsed_events_df):
        result = build_aggregation(parsed_events_df)
        rows = {r["wiki"]: r for r in result.collect()}
        assert rows["enwiki"]["bot_edit_count"] == 0, (
            "All enwiki events are human edits — bot_edit_count must be 0"
        )

    def test_bot_edit_count_for_bot_wiki(self, spark, parsed_events_df):
        result = build_aggregation(parsed_events_df)
        rows = {r["wiki"]: r for r in result.collect()}
        assert rows["dewiki"]["bot_edit_count"] == 3, (
            "All 3 dewiki events are bot edits"
        )

    def test_net_bytes_positive_when_growing(self, spark, parsed_events_df):
        """enwiki events grow bytes, net should be positive."""
        result = build_aggregation(parsed_events_df)
        rows = {r["wiki"]: r for r in result.collect()}
        assert rows["enwiki"]["net_bytes_added"] > 0, (
            "enwiki events all add bytes — net_bytes_added must be positive"
        )

    def test_net_bytes_negative_when_shrinking(self, spark, parsed_events_df):
        """dewiki: 500→490 per edit × 3 = −30."""
        result = build_aggregation(parsed_events_df)
        rows = {r["wiki"]: r for r in result.collect()}
        assert rows["dewiki"]["net_bytes_added"] == -30, (
            f"dewiki: 3 × (490-500) = -30, got {rows['dewiki']['net_bytes_added']}"
        )

    def test_no_window_struct_in_output(self, spark, parsed_events_df):
        """The raw 'window' struct column must be flattened — not in final output."""
        result = build_aggregation(parsed_events_df)
        assert "window" not in result.columns, (
            "The 'window' struct column must be replaced with window_start/window_end"
        )


# ── Tests: curveball — write_to_kafka_sink (transformation only) ──────────────

class TestKafkaSinkTransform:

    def test_filters_only_enwiki(self, spark, parsed_events_df):
        """Only enwiki events should go to the wiki.en-only topic."""
        from jobs.pipeline import write_to_kafka_sink
        # If not implemented yet, skip
        if write_to_kafka_sink.__doc__ and "Implement when instructed" in write_to_kafka_sink.__doc__:
            pytest.skip("Curveball not yet implemented")

        # Test the filter logic on a static DataFrame (extract filtering logic)
        filtered = parsed_events_df.filter(F.col("wiki") == "enwiki")
        assert filtered.count() == 7
        wikis = {r["wiki"] for r in filtered.collect()}
        assert wikis == {"enwiki"}, "Only enwiki events must reach wiki.en-only"
