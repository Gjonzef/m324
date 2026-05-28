import json
from unittest.mock import MagicMock

from ledger_listener.events.models import KafkaMetadata, LedgerEntry
from ledger_listener.processor.worker import EventWorker


def test_tc08_idempotency_duplicate_event_count_one(settings, monthly_payload) -> None:
    # Arrange
    clickhouse = MagicMock()
    mariadb = MagicMock()
    worker = EventWorker(settings=settings, clickhouse=clickhouse, mariadb=mariadb)

    inserted_entries: list[LedgerEntry] = []

    def _insert(entry: LedgerEntry) -> None:
        inserted_entries.append(entry)

    clickhouse.insert_ledger_entry.side_effect = _insert
    clickhouse.position_exists.side_effect = [False, True]

    raw = json.dumps(monthly_payload)
    monthly_metadata = KafkaMetadata(topic=settings.monthly_billing_topic, partition=0, offset=1)

    # Act
    first = worker.process_cdc_payload(raw, monthly_metadata)
    second = worker.process_cdc_payload(raw, monthly_metadata)

    # Assert
    assert first.wrote_clickhouse is True
    assert second.duplicate is True
    assert len(inserted_entries) == 1
