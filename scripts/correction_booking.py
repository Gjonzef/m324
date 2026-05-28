from __future__ import annotations

import argparse
import hashlib
import json
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

import clickhouse_connect
from confluent_kafka import Producer

from ledger_listener.configuration import Settings


@dataclass(frozen=True)
class LedgerRow:
    ledger_entry_id: str
    source_table: str
    event_timestamp: datetime
    billing_type: str
    subscription_id: int
    user_id: int
    product_id: int
    provider_id: int
    amount: int
    amount_unit: int
    billing_period_start: str
    billing_period_end: str
    billing_period_label: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create correction bookings from an existing billing_ledger row "
            "(reverse entry + optional replacement entry)."
        )
    )
    parser.add_argument("--ledger-entry-id", required=True, help="Original ledger_entry_id to correct")
    parser.add_argument(
        "--replacement-amount-chf",
        type=Decimal,
        default=None,
        help="Optional replacement amount in CHF (for example 12.50)",
    )
    parser.add_argument(
        "--replacement-amount-minor",
        type=int,
        default=None,
        help="Optional replacement amount in minor units (for amount_unit=100, 1250 means CHF 12.50)",
    )
    parser.add_argument(
        "--replacement-billing-type",
        choices=["monthly", "prorata", "setup", "correction"],
        default=None,
        help="Optional replacement billing_type. Default: billing_type of original entry",
    )
    parser.add_argument(
        "--skip-replacement",
        action="store_true",
        help="Only create reversing correction entry, without replacement entry",
    )
    parser.add_argument("--created-by", default="ledger-correction-script")
    parser.add_argument("--comment", default="manual correction")
    parser.add_argument("--topic", default=None, help="Kafka topic override (default: MONTHLY_BILLING_TOPIC)")
    parser.add_argument(
        "--send",
        action="store_true",
        help="Send to Kafka. If omitted, script prints payloads only (dry-run)",
    )
    return parser.parse_args()


def _fetch_original(settings: Settings, ledger_entry_id: str) -> LedgerRow:
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_username,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
        secure=settings.clickhouse_secure,
    )

    result = client.query(
        f"""
        SELECT
            ledger_entry_id,
            source_table,
            event_timestamp,
            billing_type,
            subscription_id,
            user_id,
            product_id,
            provider_id,
            amount,
            amount_unit,
            billing_period_start,
            billing_period_end,
            billing_period_label
        FROM {settings.clickhouse_database}.billing_ledger
        WHERE ledger_entry_id = %(ledger_entry_id)s
        ORDER BY processed_at DESC
        LIMIT 1
        """,
        parameters={"ledger_entry_id": ledger_entry_id},
    )

    if not result.result_rows:
        raise ValueError(f"No billing_ledger row found for ledger_entry_id={ledger_entry_id}")

    row = result.result_rows[0]
    return LedgerRow(
        ledger_entry_id=str(row[0]),
        source_table=str(row[1]),
        event_timestamp=row[2],
        billing_type=str(row[3]),
        subscription_id=int(row[4]),
        user_id=int(row[5]),
        product_id=int(row[6]),
        provider_id=int(row[7]),
        amount=int(row[8]),
        amount_unit=int(row[9]),
        billing_period_start=row[10].isoformat(),
        billing_period_end=row[11].isoformat(),
        billing_period_label=str(row[12]),
    )


def _to_minor_units(chf_amount: Decimal, amount_unit: int) -> int:
    quantized = (chf_amount * Decimal(amount_unit)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(quantized)


def _position_id(material: str) -> str:
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _build_entry_payload(
    *,
    original: LedgerRow,
    now: datetime,
    event_id: str,
    position_id: str,
    correlation_id: str,
    corrects_ledger_entry_id: str,
    billing_type: str,
    amount: int,
    created_by: str,
    comment: str,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "position_id": position_id,
        "correlation_id": correlation_id,
        "corrects_ledger_entry_id": corrects_ledger_entry_id,
        "billing_type": billing_type,
        "subscription_id": original.subscription_id,
        "user_id": original.user_id,
        "product_id": original.product_id,
        "provider_id": original.provider_id,
        "amount": amount,
        "amount_unit": original.amount_unit,
        "billing_period_start": original.billing_period_start,
        "billing_period_end": original.billing_period_end,
        "billing_period_label": original.billing_period_label,
        "event_timestamp": now.isoformat(),
        "source_table": original.source_table,
        "created_by": created_by,
        "comment": comment,
    }


def _produce(topic: str, bootstrap_servers: str, payloads: list[dict[str, Any]]) -> None:
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    for payload in payloads:
        body = json.dumps({"payload": payload}).encode("utf-8")
        producer.produce(topic=topic, value=body)
    producer.flush(10)


def main() -> int:
    args = _parse_args()
    settings = Settings.from_env()

    if args.replacement_amount_chf is not None and args.replacement_amount_minor is not None:
        raise ValueError("Use only one of --replacement-amount-chf or --replacement-amount-minor")

    original = _fetch_original(settings, args.ledger_entry_id)
    now = datetime.now(timezone.utc)
    correction_batch_id = str(uuid.uuid4())

    reverse_event_id = f"correction:{correction_batch_id}:reverse"
    reverse_position_id = _position_id(f"correction|{original.ledger_entry_id}|reverse|{correction_batch_id}")
    reverse_payload = _build_entry_payload(
        original=original,
        now=now,
        event_id=reverse_event_id,
        position_id=reverse_position_id,
        correlation_id=correction_batch_id,
        corrects_ledger_entry_id=original.ledger_entry_id,
        billing_type="correction",
        amount=-original.amount,
        created_by=args.created_by,
        comment=f"{args.comment} (reverse)",
    )

    payloads: list[dict[str, Any]] = [reverse_payload]

    if not args.skip_replacement:
        if args.replacement_amount_minor is not None:
            replacement_amount = int(args.replacement_amount_minor)
        elif args.replacement_amount_chf is not None:
            replacement_amount = _to_minor_units(args.replacement_amount_chf, original.amount_unit)
        else:
            replacement_amount = original.amount

        replacement_billing_type = args.replacement_billing_type or original.billing_type
        replace_event_id = f"correction:{correction_batch_id}:replacement"
        replace_position_id = _position_id(f"correction|{original.ledger_entry_id}|replacement|{correction_batch_id}")
        replace_payload = _build_entry_payload(
            original=original,
            now=now,
            event_id=replace_event_id,
            position_id=replace_position_id,
            correlation_id=correction_batch_id,
            corrects_ledger_entry_id=original.ledger_entry_id,
            billing_type=replacement_billing_type,
            amount=replacement_amount,
            created_by=args.created_by,
            comment=f"{args.comment} (replacement)",
        )
        payloads.append(replace_payload)

    mode = "SEND" if args.send else "DRY-RUN"
    print(f"Mode: {mode}")
    print(f"Original ledger_entry_id: {original.ledger_entry_id}")
    print(f"Correction batch_id: {correction_batch_id}")
    print("Generated payloads:")
    print(json.dumps(payloads, indent=2))

    if args.send:
        topic = args.topic or settings.monthly_billing_topic
        _produce(topic=topic, bootstrap_servers=settings.kafka_bootstrap_servers, payloads=payloads)
        print(f"Sent {len(payloads)} message(s) to topic={topic}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Correction script failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
