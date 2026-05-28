from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ledger_listener.configuration import Settings  # noqa: E402
from ledger_listener.events.models import KafkaMetadata  # noqa: E402


@pytest.fixture
def metadata() -> KafkaMetadata:
    return KafkaMetadata(topic="billing.cdc.subscription", partition=0, offset=42)


@pytest.fixture
def settings() -> Settings:
    return Settings(
        kafka_bootstrap_servers="localhost:9092",
        kafka_cdc_topics=("billing.cdc.subscription",),
        kafka_group_id="ledger-listener-cdc",
        kafka_auto_offset_reset="earliest",
        kafka_allow_wildcard_topics=False,
        listener_poll_timeout_seconds=0.01,
        clickhouse_host="localhost",
        clickhouse_port=8123,
        clickhouse_username="default",
        clickhouse_password="secret",
        clickhouse_database="billing",
        clickhouse_secure=False,
        clickhouse_recreate_ledger_table=False,
        debezium_connector_name="telephony_dev",
        source_database_name="telephony_dev",
        subscription_table_name="subscription",
        monthly_billing_topic="monthly_billing",
        mariadb_host="localhost",
        mariadb_port=3306,
        mariadb_database="telephony_dev",
        mariadb_user="ledger",
        mariadb_password="secret",
        mariadb_connect_timeout_seconds=5,
        mariadb_charset="utf8mb4",
        mariadb_collation="utf8mb4_general_ci",
        default_currency="CHF",
        log_level="INFO",
    )


@pytest.fixture
def debezium_create_event() -> dict:
    return {
        "payload": {
            "before": None,
            "after": {
                "id": 1001,
                "end_user_id": 501,
                "product_id": 10,
                "created_at": "1711929600000000",
                "updated_at": "1711929600000000",
                "start_datetime": "1711929600000000",
                "end_datetime": "1714521600000000",
            },
            "op": "c",
            "source": {
                "name": "telephony_dev",
                "db": "telephony_dev",
                "table": "subscription",
                "file": "mysql-bin.000123",
                "pos": 456,
                "row": 1,
                "ts_ms": 1711929600000,
            },
            "ts_ms": 1711929600500,
        }
    }


@pytest.fixture
def debezium_update_event() -> dict:
    return {
        "payload": {
            "before": {
                "id": 1001,
                "product_id": 10,
                "updated_at": "1711929600000000",
            },
            "after": {
                "id": 1001,
                "product_id": 11,
                "updated_at": "1712016000000000",
            },
            "op": "u",
            "source": {
                "name": "telephony_dev",
                "db": "telephony_dev",
                "table": "subscription",
                "file": "mysql-bin.000123",
                "pos": 789,
                "row": 2,
                "ts_ms": 1712016000000,
            },
            "ts_ms": 1712016000100,
        }
    }


@pytest.fixture
def debezium_delete_event() -> dict:
    return {
        "payload": {
            "before": {
                "id": 1001,
                "end_user_id": 501,
                "product_id": 10,
                "created_at": "1711929600000000",
                "updated_at": "1712016000000000",
                "start_datetime": "1711929600000000",
                "end_datetime": "1714521600000000",
            },
            "after": None,
            "op": "d",
            "source": {
                "name": "telephony_dev",
                "db": "telephony_dev",
                "table": "subscription",
                "file": "mysql-bin.000123",
                "pos": 999,
                "row": 3,
                "ts_ms": 1712102400000,
            },
            "ts_ms": 1712102400200,
        }
    }


@pytest.fixture
def monthly_payload() -> dict:
    return {
        "payload": {
            "event_id": "evt-monthly-001",
            "position_id": "pos-monthly-001",
            "correlation_id": "",
            "corrects_ledger_entry_id": "",
            "billing_type": "monthly",
            "subscription_id": 1001,
            "user_id": 501,
            "product_id": 10,
            "provider_id": 7,
            "amount": 1990,
            "amount_unit": 100,
            "billing_period_start": "2026-04-01",
            "billing_period_end": "2026-04-30",
            "billing_period_label": "2026-04",
            "event_timestamp": "2026-04-01T00:00:00Z",
            "source_table": "billing_ledger",
            "created_by": "pytest",
            "comment": "monthly booking",
            "monthly_details": {
                "monthly_price_chf": "19.90",
                "service_period_start": "2026-04-01",
                "service_period_end": "2026-04-30",
                "quantity": 1,
            },
        }
    }
