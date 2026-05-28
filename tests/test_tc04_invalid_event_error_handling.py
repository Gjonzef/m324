import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


def test_tc04_invalid_event_missing_required_fields_logged_and_consumer_continues(settings, caplog) -> None:
    # Arrange
    caplog.set_level(logging.ERROR)
    consumer_module = pytest.importorskip("ledger_listener.kafka.consumer")

    bad_message = SimpleNamespace(
        topic=lambda: "billing.cdc.subscription",
        partition=lambda: 0,
        offset=lambda: 10,
        value=lambda: b'{"payload": {"op": "c", "after": {"id": 1}}}',
        error=lambda: None,
    )
    good_message = SimpleNamespace(
        topic=lambda: "billing.cdc.subscription",
        partition=lambda: 0,
        offset=lambda: 11,
        value=lambda: b"{}",
        error=lambda: None,
    )

    fake_consumer = MagicMock()
    fake_consumer.poll.side_effect = [bad_message, good_message, KeyboardInterrupt]
    fake_consumer.subscribe.return_value = None

    worker = MagicMock()
    worker.process_cdc_payload.side_effect = [
        ValueError("missing required fields"),
        SimpleNamespace(wrote_clickhouse=False, duplicate=False, should_commit=True),
    ]

    def monkeypatched_consumer_class(*_args, **_kwargs):
        return fake_consumer

    setattr(consumer_module, "Consumer", monkeypatched_consumer_class)

    cdc_consumer = consumer_module.CDCConsumer(settings=settings, worker=worker)

    # Act
    with pytest.raises(KeyboardInterrupt):
        cdc_consumer.run_forever()

    # Assert
    assert worker.process_cdc_payload.call_count == 2
    assert fake_consumer.commit.call_count == 1
    assert "Failed to process CDC message" in caplog.text
