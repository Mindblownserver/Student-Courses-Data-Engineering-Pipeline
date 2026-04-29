#!/usr/bin/env python3
"""Streaming consumer: Kafka topic -> Bronze stream JSONL files."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = os.getenv("COURSES_TOPIC", "courses.events")
GROUP_ID = os.getenv("COURSES_CONSUMER_GROUP", "courses-events-bronze")
BRONZE_STREAM_ROOT = Path(
    os.getenv("BRONZE_STREAM_PATH", "/data/bronze_stream/courses_events")
)


# Instantiates Kafka Consumer
def _consumer() -> KafkaConsumer:
    for attempt in range(1, 31):
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP,
                group_id=GROUP_ID,
                enable_auto_commit=True,
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
        except Exception as exc:
            print(f"Kafka not ready (attempt {attempt}/30): {exc}", flush=True)
            time.sleep(2)
    raise RuntimeError("Kafka broker not reachable after retries")


def _target_file(now: datetime) -> Path:
    path = (
        BRONZE_STREAM_ROOT / f"{now.year:04d}" / f"{now.month:02d}" / f"{now.day:02d}"
    )
    path.mkdir(parents=True, exist_ok=True)
    return path / "events.jsonl"


def main():
    consumer = _consumer()
    for msg in consumer:
        now = datetime.now(timezone.utc)
        event = msg.value
        event["_ingested_at"] = now.isoformat().replace("+00:00", "Z")
        event["_topic"] = TOPIC
        event["_partition"] = msg.partition
        event["_offset"] = msg.offset
        target = _target_file(now)
        with target.open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(event, ensure_ascii=True) + "\n")
        print(f"Stored event offset={msg.offset} at {target}", flush=True)


if __name__ == "__main__":
    main()
