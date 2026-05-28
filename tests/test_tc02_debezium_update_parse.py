import json

from ledger_listener.debezium.parser import parse_debezium_event


def test_tc02_parse_debezium_update_event_before_and_after(metadata, debezium_update_event) -> None:
    # Arrange
    raw = json.dumps(debezium_update_event)

    # Act
    event = parse_debezium_event(raw, metadata)

    # Assert
    assert event.op == "u"
    assert event.before is not None
    assert event.after is not None
    assert event.before["product_id"] == 10
    assert event.after["product_id"] == 11
