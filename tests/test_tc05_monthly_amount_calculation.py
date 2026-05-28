from datetime import date, datetime, timezone
from decimal import Decimal

from ledger_listener.events.models import LedgerEntry
from ledger_listener.processor.worker import _build_monthly_details


def test_tc05_monthly_amount_calculation_equals_monthly_price() -> None:
    # Arrange
    entry = LedgerEntry(
        ledger_entry_id="ledger-monthly-1",
        event_id="evt-monthly-1",
        position_id="pos-monthly-1",
        correlation_id="",
        corrects_ledger_entry_id="",
        debezium_op="c",
        source_table="billing_ledger",
        event_timestamp=datetime(2026, 4, 1, tzinfo=timezone.utc),
        processed_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
        billing_type="monthly",
        subscription_id=1001,
        user_id=501,
        product_id=10,
        provider_id=7,
        amount=1990,
        amount_unit=100,
        billing_period_start=date(2026, 4, 1),
        billing_period_end=date(2026, 4, 30),
        billing_period_label="2026-04",
        created_by="pytest",
        comment=None,
    )

    # Act
    details = _build_monthly_details(entry, payload={})
    amount_chf = Decimal(entry.amount) / Decimal(entry.amount_unit)

    # Assert
    assert details["monthly_price_chf"] == Decimal("19.90")
    assert amount_chf == details["monthly_price_chf"]
