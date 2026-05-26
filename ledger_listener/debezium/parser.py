import json
from datetime import datetime, timezone
from typing import Any

from ledger_listener.events.models import DebeziumEvent, KafkaMetadata


def _ts_ms_to_datetime(ts_ms: int | None) -> datetime:
    if ts_ms is None:
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)


def parse_debezium_event(raw_payload: bytes | str, metadata: KafkaMetadata) -> DebeziumEvent:
    payload_str = raw_payload.decode("utf-8") if isinstance(raw_payload, bytes) else raw_payload
    document: dict[str, Any] = json.loads(payload_str)
    envelope: dict[str, Any] = document.get("payload") if isinstance(document.get("payload"), dict) else document

    op = envelope.get("op")
    if op not in {"c", "u", "d", "r"}:
        raise ValueError(f"Unsupported Debezium op: {op!r}")

    before = envelope.get("before")
    after = envelope.get("after")
    if op == "d" and not isinstance(before, dict):
        raise ValueError("Delete event must provide 'before'")
    if op in {"c", "u", "r"} and not isinstance(after, dict):
        raise ValueError("Create/Update/Read event must provide 'after'")

    source = envelope.get("source") if isinstance(envelope.get("source"), dict) else {}
    source_ts_ms = source.get("ts_ms") if isinstance(source, dict) else None
    event_ts_ms = envelope.get("ts_ms")
    # Canonical event timestamp per project rule: source.ts_ms
    ts_ms = source_ts_ms if source_ts_ms is not None else event_ts_ms

    return DebeziumEvent(
        op=op,
        before=before if isinstance(before, dict) else None,
        after=after if isinstance(after, dict) else None,
        source_name=source.get("name") if isinstance(source, dict) else None,
        source_file=source.get("file") if isinstance(source, dict) else None,
        source_pos=source.get("pos") if isinstance(source.get("pos"), int) else None,
        source_row=source.get("row") if isinstance(source.get("row"), int) else None,
        source_ts_ms=source_ts_ms if isinstance(source_ts_ms, int) else None,
        source_table=source.get("table") if isinstance(source, dict) else None,
        source_db=source.get("db") if isinstance(source, dict) else None,
        event_ts=_ts_ms_to_datetime(ts_ms if isinstance(ts_ms, int) else None),
        raw_payload=payload_str,
        metadata=metadata,
    )
