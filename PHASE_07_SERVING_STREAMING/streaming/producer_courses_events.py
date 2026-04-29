#!/usr/bin/env python3
"""Streaming producer: External Courses API -> Kafka topic.

Modes:
- mock: generate synthetic events
- live: fetch from external courses API and publish events
"""

from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from pymongo import MongoClient
from bson import ObjectId


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = os.getenv("COURSES_TOPIC", "courses.events")
MODE = os.getenv("COURSES_API_MODE", "mongo_poll").strip().lower()
POLL_SECONDS = 10
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = "courses"
CHECKPOINT_PATH = "/data/bronze_stream/courses_events/checkpoint6.txt"
BOOT_BATCH_SIZE = 500
POLL_BATCH_SIZE = 50


# Instantiates Kafka Producer
def _producer() -> KafkaProducer:
    for attempt in range(1, 31):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=True).encode(
                    "utf-8"
                ),
                acks="all",
                retries=5,
            )
        except Exception as exc:
            print(f"Kafka not ready (attempt {attempt}/30): {exc}", flush=True)
            time.sleep(2)
    raise RuntimeError("Kafka broker not reachable after retries")


# Scapegoat if the kafka bit fails me TT
def _mock_event() -> dict:
    course_names = [
        "Advanced MongoDB Techniques",
        "Data Engineering Fundamentals",
        "Spark for Analytics",
        "Python for Data Pipelines",
    ]
    # Keep mock IDs in Mongo ObjectId-like format for consistency.
    course_id = str(ObjectId())
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "course_id": course_id,
        "course_name": random.choice(course_names),
        "event_type": random.choice(["create", "update"]),
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


# In case we have dedicated REST Api for streaming...which we don't :(
# def _fetch_live_events() -> list[dict]:
#     if not API_URL:
#         return []
#     headers = {"Authorization": f"Bearer {API_KEY}"} if API_KEY else {}
#     response = requests.get(API_URL, headers=headers, timeout=20)
#     response.raise_for_status()
#     payload = response.json()
#     rows = payload.get("events", payload if isinstance(payload, list) else [])
#     out = []
#     for row in rows:
#         out.append(
#             {
#                 "event_id": str(row.get("event_id") or uuid.uuid4()),
#                 "event_time": row.get("event_time")
#                 or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
#                 "course_id": str(row.get("course_id") or row.get("_id") or ""),
#                 "course_name": row.get("course_name") or row.get("name"),
#                 "event_type": row.get("event_type") or "upsert",
#                 "created_at": row.get("created_at") or row.get("createdAt"),
#                 "updated_at": row.get("updated_at") or row.get("updatedAt"),
#             }
#         )
#     return out


def _load_checkpoint() -> str | None:
    if not os.path.exists(CHECKPOINT_PATH):
        return None
    with open(CHECKPOINT_PATH, "r", encoding="utf-8") as fp:
        raw = fp.read().strip()
    return raw or None


def _save_checkpoint(value: str) -> None:
    os.makedirs(os.path.dirname(CHECKPOINT_PATH), exist_ok=True)
    with open(CHECKPOINT_PATH, "w", encoding="utf-8") as fp:
        fp.write(value)


def _poll_mongo_courses() -> list[dict]:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        coll = client[MONGO_DB][MONGO_COLLECTION]
        last_id = _load_checkpoint()
        query = {}
        if last_id and ObjectId.is_valid(last_id):
            query = {"_id": {"$gt": ObjectId(last_id)}}

        docs = list(coll.find(query).sort("_id", 1).limit(POLL_BATCH_SIZE))
        if not docs:
            return []

        events = []
        for doc in docs:
            events.append(
                {
                    "event_id": str(uuid.uuid4()),
                    "event_time": datetime.now(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z"),
                    "course_id": str(doc.get("_id") or ""),
                    "course_name": doc.get("title"),
                    "event_type": "upsert",
                    "created_at": str(
                        doc.get("created_at") or doc.get("createdAt") or ""
                    ),
                    "updated_at": str(
                        doc.get("updated_at") or doc.get("updatedAt") or ""
                    ),
                }
            )

        last_doc_id = str(docs[-1].get("_id"))
        if last_doc_id:
            _save_checkpoint(last_doc_id)

        return events
    finally:
        client.close()


def _publish_events(producer: KafkaProducer, events: list[dict]) -> None:
    if not events:
        return
    for event in events:
        producer.send(TOPIC, event)
    producer.flush()


def _bootstrap_mongo_courses(producer: KafkaProducer) -> int:
    """One-time catch-up at process start using ObjectId watermark."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    published_total = 0
    try:
        coll = client[MONGO_DB][MONGO_COLLECTION]

        while True:
            last_id = _load_checkpoint()
            print("The last id is", last_id)
            query = {}
            if last_id and ObjectId.is_valid(last_id):
                query = {"_id": {"$gt": ObjectId(last_id)}}

            docs = list(coll.find(query).sort("_id", 1).limit(BOOT_BATCH_SIZE))
            if not docs:
                break

            events = []
            for doc in docs:
                events.append(
                    {
                        "event_id": str(uuid.uuid4()),
                        "event_time": datetime.now(timezone.utc)
                        .isoformat()
                        .replace("+00:00", "Z"),
                        "course_id": str(doc.get("_id") or ""),
                        "course_name": doc.get("course_name"),
                        "event_type": "upsert",
                        "created_at": str(
                            doc.get("created_at") or doc.get("createdAt") or ""
                        ),
                        "updated_at": str(
                            doc.get("updated_at") or doc.get("updatedAt") or ""
                        ),
                    }
                )

            _publish_events(producer, events)
            published_total += len(events)

            last_doc_id = str(docs[-1].get("_id"))
            if last_doc_id:
                _save_checkpoint(last_doc_id)

            if len(docs) < BOOT_BATCH_SIZE:
                break
    finally:
        client.close()

    return published_total


def main() -> None:
    producer = _producer()

    if MODE == "mongo_poll":
        boot_published = _bootstrap_mongo_courses(producer)
        print(
            f"Bootstrap catch-up published {boot_published} event(s) to {TOPIC}",
            flush=True,
        )

    while True:
        events: list[dict] = []
        if MODE == "mongo_poll":  # Checking records in DB
            events = _poll_mongo_courses()
            if not events:
                time.sleep(POLL_SECONDS)
                continue
        elif MODE == "mock":  # Mocking getting records (unit testing)
            events = [_mock_event()]

        else:
            raise ValueError(f"Unsupported COURSES_API_MODE: {MODE}")

        _publish_events(producer, events)
        print(f"Published {len(events)} event(s) to {TOPIC}", flush=True)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
