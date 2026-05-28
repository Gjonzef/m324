import json
from datetime import datetime, timezone

from ledger_listener.debezium.parser import parse_debezium_event
from scripts.correction_booking import LedgerRow, _build_entry_payload


def test_tc03_delete_event_to_correction_booking_payload(metadata, debezium_delete_event) -> None:
    # Arrange
    event = parse_debezium_event(json.dumps(debezium_delete_event), metadata)
    correlation_id = "corr-delete-202604"
    original = LedgerRow(
        ledger_entry_id="ledger-0001",
        source_table="subscription",
        event_timestamp=event.event_ts,
        billing_type="monthly",
        subscription_id=1001,
        user_id=501,
        product_id=10,
        provider_id=7,
        amount=1990,
        amount_unit=100,
        billing_period_start="2026-04-01",
        billing_period_end="2026-04-30",
        billing_period_label="2026-04",
    )

    # Act
    correction_payload = _build_entry_payload(
        original=original,
        now=datetime(2026, 4, 30, tzinfo=timezone.utc),
        event_id="correction:corr-delete-202604:reverse",
        position_id="pos-correction-1",
        correlation_id=correlation_id,
        corrects_ledger_entry_id=original.ledger_entry_id,
        billing_type="correction",
        amount=-original.amount,
        created_by="pytest",
        comment="reverse from delete",
    )

    # Assert
    assert event.op == "d"
    assert correction_payload["amount"] < 0
    assert correction_payload["correlation_id"] == correlation_id
