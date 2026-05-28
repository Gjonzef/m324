import json

from ledger_listener.debezium.parser import parse_debezium_event


def test_tc01_parse_debezium_create_event(metadata, debezium_create_event) -> None:
    # Arrange
    raw = json.dumps(debezium_create_event)

    # Act
    event = parse_debezium_event(raw, metadata)

    # Assert
    assert event.op == "c"
    assert event.before is None
    assert event.after is not None
    assert event.after["id"] == 1001
    assert event.source_name == "telephony_dev"
    assert event.source_table == "subscription"
    assert event.source_file == "mysql-bin.000123"
    assert event.source_pos == 456
    assert event.source_row == 1
