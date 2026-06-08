"""
Wikipedia Recent Changes → Kafka Producer
==========================================
PRE-BUILT — you do not need to modify this script.

Data source: https://stream.wikimedia.org/v2/stream/recentchange
  - Real 24/7 SSE stream, no authentication required
  - ~50-200 events/minute depending on time of day
  - Events include page edits, new pages, log entries, etc.

This script:
  1. Connects to the Wikipedia SSE endpoint
  2. Filters for 'edit' and 'new' event types
  3. Enriches with an ingestion timestamp
  4. Publishes to Kafka topic: wiki.recentchanges
     Key = wiki name (ensures all events for a wiki go to the same partition)

Usage:  make producer
"""

import json
import os
import time
from datetime import datetime, timezone

import requests
import sseclient
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC           = "wiki.recentchanges"
SSE_URL         = "https://stream.wikimedia.org/v2/stream/recentchange"
ALLOWED_TYPES   = {"edit", "new"}


def wait_for_kafka(bootstrap: str, retries: int = 15, delay: float = 5.0) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            print(f"✓ Connected to Kafka at {bootstrap}")
            return producer
        except NoBrokersAvailable:
            print(f"  Kafka not ready (attempt {attempt}/{retries}) — retrying in {delay}s …")
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka at {bootstrap}")


def stream_edits(producer: KafkaProducer) -> None:
    print(f"Connecting to Wikipedia SSE stream …")
    resp = requests.get(SSE_URL, stream=True, timeout=30,
                        headers={"Accept": "text/event-stream"})
    resp.raise_for_status()
    client = sseclient.SSEClient(resp)

    sent = skipped = 0
    for event in client.events():
        if not event.data:
            continue
        try:
            data = json.loads(event.data)
        except json.JSONDecodeError:
            continue

        if data.get("type") not in ALLOWED_TYPES:
            skipped += 1
            continue

        # Normalise to a flat, predictable shape for Spark to parse
        msg = {
            "id":          data.get("id"),
            "type":        data.get("type", ""),
            "wiki":        data.get("wiki", ""),
            "title":       data.get("title", ""),
            "user":        data.get("user", ""),
            "bot":         bool(data.get("bot", False)),
            "timestamp":   data.get("timestamp", 0),
            "length": {
                "old": (data.get("length") or {}).get("old") or 0,
                "new": (data.get("length") or {}).get("new") or 0,
            },
            "meta": {
                "dt": (data.get("meta") or {}).get("dt") or "",
            },
            "server_name": data.get("server_name", ""),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

        producer.send(TOPIC, key=msg["wiki"], value=msg)
        sent += 1
        if sent % 200 == 0:
            print(f"  … {sent:,} events sent  |  {skipped:,} non-edit events skipped")


def run() -> None:
    producer = wait_for_kafka(KAFKA_BOOTSTRAP)
    print(f"Streaming Wikipedia edits → topic '{TOPIC}'  (Ctrl-C to stop)\n")
    while True:
        try:
            stream_edits(producer)
        except KeyboardInterrupt:
            print("\nStopping producer.")
            break
        except Exception as e:
            print(f"Stream error: {e} — reconnecting in 5s …")
            time.sleep(5)
    producer.flush()


if __name__ == "__main__":
    run()
