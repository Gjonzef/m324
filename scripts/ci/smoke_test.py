from __future__ import annotations

import json
import os
import sys
import time

from clickhouse_connect import get_client
from confluent_kafka import Producer

from ledger_listener.configuration import Settings


def _require_int_env(name: str) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        raise ValueError(f"{name} is required for smoke test")
    return int(raw)


def _first_topic(settings: Settings) -> str:
    if settings.kafka_cdc_topics:
        return settings.kafka_cdc_topics[0]
    raise ValueError("KAFKA_CDC_TOPICS must contain at least one topic")


def _build_event(settings: Settings, subscription_id: int, end_user_id: int, product_id: int) -> dict[str, object]:
    now_ms = int(time.time() * 1000)
    now_us = int(time.time() * 1_000_000)
    return {
        "payload": {
            "before": None,
            "after": {
                "id": subscription_id,
                "end_user_id": end_user_id,
                "product_id": product_id,
                "created_at": str(now_us),
                "updated_at": str(now_us),
                "start_datetime": str(now_us),
                "end_datetime": str(now_us),
            },
            "source": {
                "name": settings.debezium_connector_name,
                "db": settings.source_database_name,
                "table": settings.subscription_table_name,
                "file": f"smoke-{subscription_id}",
                "pos": 1,
                "row": 1,
                "ts_ms": now_ms,
            },
            "op": "c",
            "ts_ms": now_ms,
        }
    }


def _produce_message(bootstrap_servers: str, topic: str, payload: dict[str, object]) -> None:
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    body = json.dumps(payload).encode("utf-8")
    producer.produce(topic=topic, value=body)
    producer.flush(10)


def _wait_for_clickhouse_row(settings: Settings, subscription_id: int, timeout_seconds: int) -> bool:
    client = get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_username,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
        secure=settings.clickhouse_secure,
    )

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        result = client.query(
            f"""
            SELECT count()
            FROM {settings.clickhouse_database}.billing_ledger
            WHERE subscription_id = %(subscription_id)s
              AND source_table = %(source_table)s
              AND billing_type = 'setup'
            """,
            parameters={
                "subscription_id": subscription_id,
                "source_table": settings.subscription_table_name,
            },
        )
        if result.result_rows and int(result.result_rows[0][0]) > 0:
            return True
        time.sleep(2)
    return False


def main() -> int:
    settings = Settings.from_env()

    bootstrap = settings.kafka_bootstrap_servers
    topic = os.getenv("SMOKE_TOPIC", "").strip() or _first_topic(settings)
    wait_seconds = int(os.getenv("SMOKE_WAIT_SECONDS", "60"))

    subscription_id = int(os.getenv("SMOKE_SUBSCRIPTION_ID", str(int(time.time()))))
    end_user_id = _require_int_env("SMOKE_END_USER_ID")
    product_id = _require_int_env("SMOKE_PRODUCT_ID")

    event_payload = _build_event(
        settings=settings,
        subscription_id=subscription_id,
        end_user_id=end_user_id,
        product_id=product_id,
    )

    print(f"Producing synthetic event to topic={topic} subscription_id={subscription_id}")
    _produce_message(bootstrap_servers=bootstrap, topic=topic, payload=event_payload)

    if not _wait_for_clickhouse_row(settings, subscription_id=subscription_id, timeout_seconds=wait_seconds):
        print(
            f"Smoke test failed: no ClickHouse row found for subscription_id={subscription_id}",
            file=sys.stderr,
        )
        return 1

    print("Smoke test passed: row found in ClickHouse")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Smoke test failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
