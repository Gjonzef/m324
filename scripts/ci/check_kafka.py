from __future__ import annotations

import os
import sys

from confluent_kafka.admin import AdminClient


def _topics_from_env() -> list[str]:
    topics_raw = os.getenv("KAFKA_CDC_TOPICS", "")
    return [topic.strip() for topic in topics_raw.split(",") if topic.strip()]


def main() -> int:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not bootstrap:
        print("KAFKA_BOOTSTRAP_SERVERS is required", file=sys.stderr)
        return 2

    client = AdminClient({"bootstrap.servers": bootstrap})
    metadata = client.list_topics(timeout=10)
    if metadata is None:
        print("Kafka metadata request returned None", file=sys.stderr)
        return 1

    broker_ids = sorted(metadata.brokers.keys())
    print(f"Kafka reachable. brokers={broker_ids}")

    expected_topics = _topics_from_env()
    if expected_topics:
        missing = [topic for topic in expected_topics if topic not in metadata.topics]
        if missing:
            print(
                "Kafka reachable but expected topics are missing: " + ", ".join(missing),
                file=sys.stderr,
            )
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
