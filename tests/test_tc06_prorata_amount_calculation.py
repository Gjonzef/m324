from datetime import date, datetime, timezone
from decimal import Decimal

from ledger_listener.events.models import LedgerEntry
from ledger_listener.processor.worker import _build_prorata_details


def test_tc06_prorata_amount_for_april_30_days() -> None:
    # Arrange
    entry = LedgerEntry(
        ledger_entry_id="ledger-prorata-1",
        event_id="evt-prorata-1",
        position_id="pos-prorata-1",
        correlation_id="",
        corrects_ledger_entry_id="",
        debezium_op="c",
        source_table="billing_ledger",
        event_timestamp=datetime(2026, 4, 1, tzinfo=timezone.utc),
        processed_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
        billing_type="prorata",
        subscription_id=1002,
        user_id=502,
        product_id=11,
        provider_id=7,
        amount=3000,
        amount_unit=100,
        billing_period_start=date(2026, 4, 1),
        billing_period_end=date(2026, 4, 30),
        billing_period_label="2026-04",
        created_by="pytest",
        comment=None,
    )
    payload = {
        "prorata_details": {
            "full_period_start": "2026-04-01",
            "full_period_end": "2026-04-30",
            "active_period_start": "2026-04-01",
            "active_period_end": "2026-04-10",
        }
    }

    # Act
    details = _build_prorata_details(entry, payload)
    expected = (details["full_amount_chf"] / Decimal(details["full_period_days"])) * Decimal(details["prorata_days"])

    # Assert
    assert details["full_period_days"] == 30
    assert details["prorata_days"] == 10
    assert expected.quantize(Decimal("0.01")) == Decimal("10.00")
