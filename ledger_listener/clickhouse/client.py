from datetime import date
from decimal import Decimal
from typing import Any

import clickhouse_connect

from ledger_listener.configuration import Settings
from ledger_listener.events.models import LedgerEntry


class ClickHouseLedgerClient:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_username,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
            secure=settings.clickhouse_secure,
        )

    def ensure_schema(self) -> None:
        self._client.command(f"CREATE DATABASE IF NOT EXISTS {self._settings.clickhouse_database}")
        if self._ledger_table_exists() and not self._ledger_schema_matches():
            if self._settings.clickhouse_recreate_ledger_table:
                self._client.command(f"DROP TABLE {self._settings.clickhouse_database}.billing_ledger")
            else:
                raise RuntimeError(
                    "Existing billing_ledger schema is incompatible with the real table definition. "
                    "Set CLICKHOUSE_RECREATE_LEDGER_TABLE=true for one startup to recreate it."
                )
        self._client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {self._settings.clickhouse_database}.billing_ledger
            (
                ledger_entry_id String DEFAULT toString(generateUUIDv4()),
                event_id String,
                position_id String,
                correlation_id String DEFAULT '',
                corrects_ledger_entry_id String DEFAULT '',
                debezium_op Enum8('c' = 1, 'u' = 2, 'd' = 3, 'r' = 4),
                source_table String,
                event_timestamp DateTime('UTC'),
                processed_at DateTime('UTC') DEFAULT now(),
                billing_type Enum8('monthly' = 1, 'prorata' = 2, 'setup' = 3, 'correction' = 4),
                subscription_id UInt32,
                user_id UInt32,
                product_id UInt32,
                provider_id UInt32,
                amount Int64,
                amount_unit UInt32,
                billing_period_start Date,
                billing_period_end Date,
                billing_period_label String,
                created_by String,
                comment Nullable(String)
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(billing_period_start)
            ORDER BY (billing_period_start, subscription_id, position_id, processed_at)
            """
        )
        self._client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {self._settings.clickhouse_database}.billing_monthly_details
            (
                ledger_entry_id String,
                monthly_price_chf Decimal(10, 2),
                service_period_start Date,
                service_period_end Date,
                quantity UInt32 DEFAULT 1,
                created_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree
            ORDER BY (ledger_entry_id)
            """
        )
        self._client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {self._settings.clickhouse_database}.billing_prorata_details
            (
                ledger_entry_id String,
                full_period_start Date,
                full_period_end Date,
                active_period_start Date,
                active_period_end Date,
                prorata_days UInt32,
                full_period_days UInt32,
                full_amount_chf Decimal(10, 2),
                prorata_factor Decimal(8, 4),
                created_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree
            ORDER BY (ledger_entry_id)
            """
        )

    def _ledger_table_exists(self) -> bool:
        result = self._client.query(
            f"EXISTS TABLE {self._settings.clickhouse_database}.billing_ledger"
        )
        return bool(result.result_rows and int(result.result_rows[0][0]) == 1)

    def _ledger_schema_matches(self) -> bool:
        result = self._client.query(
            f"DESCRIBE TABLE {self._settings.clickhouse_database}.billing_ledger"
        )
        existing_types = {str(row[0]): str(row[1]) for row in result.result_rows}
        required = {
            "ledger_entry_id",
            "event_id",
            "position_id",
            "correlation_id",
            "corrects_ledger_entry_id",
            "debezium_op",
            "source_table",
            "event_timestamp",
            "processed_at",
            "billing_type",
            "subscription_id",
            "user_id",
            "product_id",
            "provider_id",
            "amount",
            "amount_unit",
            "billing_period_start",
            "billing_period_end",
            "billing_period_label",
            "created_by",
            "comment",
        }
        if not required.issubset(set(existing_types)):
            return False
        return existing_types.get("amount_unit") == "UInt32"

    def position_exists(self, position_id: str) -> bool:
        result = self._client.query(
            f"SELECT count() FROM {self._settings.clickhouse_database}.billing_ledger WHERE position_id = %(position_id)s",
            parameters={"position_id": position_id},
        )
        return bool(result.result_rows and result.result_rows[0][0] > 0)

    def insert_ledger_entry(self, entry: LedgerEntry) -> None:
        data: list[list[Any]] = [
            [
                entry.ledger_entry_id,
                entry.event_id,
                entry.position_id,
                entry.correlation_id,
                entry.corrects_ledger_entry_id,
                entry.debezium_op,
                entry.source_table,
                entry.event_timestamp,
                entry.processed_at,
                entry.subscription_id,
                entry.product_id,
                entry.billing_type,
                entry.user_id,
                entry.provider_id,
                entry.amount,
                entry.amount_unit,
                entry.billing_period_start,
                entry.billing_period_end,
                entry.billing_period_label,
                entry.created_by,
                entry.comment,
            ]
        ]

        self._client.insert(
            table=f"{self._settings.clickhouse_database}.billing_ledger",
            data=data,
            column_names=[
                "ledger_entry_id",
                "event_id",
                "position_id",
                "correlation_id",
                "corrects_ledger_entry_id",
                "debezium_op",
                "source_table",
                "event_timestamp",
                "processed_at",
                "subscription_id",
                "product_id",
                "billing_type",
                "user_id",
                "provider_id",
                "amount",
                "amount_unit",
                "billing_period_start",
                "billing_period_end",
                "billing_period_label",
                "created_by",
                "comment",
            ],
        )

    def insert_monthly_details(
        self,
        *,
        ledger_entry_id: str,
        monthly_price_chf: Decimal,
        service_period_start: date,
        service_period_end: date,
        quantity: int,
    ) -> None:
        self._client.insert(
            table=f"{self._settings.clickhouse_database}.billing_monthly_details",
            data=[
                [
                    ledger_entry_id,
                    monthly_price_chf,
                    service_period_start,
                    service_period_end,
                    quantity,
                ]
            ],
            column_names=[
                "ledger_entry_id",
                "monthly_price_chf",
                "service_period_start",
                "service_period_end",
                "quantity",
            ],
        )

    def insert_prorata_details(
        self,
        *,
        ledger_entry_id: str,
        full_period_start: date,
        full_period_end: date,
        active_period_start: date,
        active_period_end: date,
        prorata_days: int,
        full_period_days: int,
        full_amount_chf: Decimal,
        prorata_factor: Decimal,
    ) -> None:
        self._client.insert(
            table=f"{self._settings.clickhouse_database}.billing_prorata_details",
            data=[
                [
                    ledger_entry_id,
                    full_period_start,
                    full_period_end,
                    active_period_start,
                    active_period_end,
                    prorata_days,
                    full_period_days,
                    full_amount_chf,
                    prorata_factor,
                ]
            ],
            column_names=[
                "ledger_entry_id",
                "full_period_start",
                "full_period_end",
                "active_period_start",
                "active_period_end",
                "prorata_days",
                "full_period_days",
                "full_amount_chf",
                "prorata_factor",
            ],
        )
