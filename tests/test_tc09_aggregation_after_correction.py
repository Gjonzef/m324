import json
from decimal import Decimal
from unittest.mock import MagicMock

from ledger_listener.events.models import KafkaMetadata, LedgerEntry
from ledger_listener.processor.worker import EventWorker


def test_tc09_aggregation_net_total_zero_after_correction(settings, monthly_payload) -> None:
    # Arrange
    clickhouse = MagicMock()
    mariadb = MagicMock()
    worker = EventWorker(settings=settings, clickhouse=clickhouse, mariadb=mariadb)

    inserted_entries: list[LedgerEntry] = []

    def _insert(entry: LedgerEntry) -> None:
        inserted_entries.append(entry)

    clickhouse.insert_ledger_entry.side_effect = _insert
    clickhouse.position_exists.side_effect = [False, False]

    correction_payload = {
        "payload": {
            **monthly_payload["payload"],
            "event_id": "evt-correction-001",
            "position_id": "pos-correction-001",
            "correlation_id": "corr-001",
            "corrects_ledger_entry_id": "ledger-original-001",
            "billing_type": "correction",
            "amount": -monthly_payload["payload"]["amount"],
            "comment": "reverse",
        }
    }
    monthly_metadata = KafkaMetadata(topic=settings.monthly_billing_topic, partition=0, offset=1)

    # Act
    worker.process_cdc_payload(json.dumps(monthly_payload), monthly_metadata)
    worker.process_cdc_payload(json.dumps(correction_payload), monthly_metadata)
    net_total = sum(entry.amount for entry in inserted_entries)

    # Assert
    assert len(inserted_entries) == 2
    assert net_total == 0
    assert Decimal(net_total) / Decimal(100) == Decimal("0.00")
