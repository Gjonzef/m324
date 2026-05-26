from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Literal

DebeziumOp = Literal["c", "u", "d", "r"]
BillingType = Literal["monthly", "prorata", "setup", "correction"]
AmountSkaling = Literal[100, 1_000_000]


@dataclass(frozen=True)
class KafkaMetadata:
    topic: str
    partition: int
    offset: int


@dataclass(frozen=True)
class DebeziumEvent:
    op: DebeziumOp
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    source_name: str | None
    source_file: str | None
    source_pos: int | None
    source_row: int | None
    source_ts_ms: int | None
    source_table: str | None
    source_db: str | None
    event_ts: datetime
    raw_payload: str
    metadata: KafkaMetadata

    @property
    def payload(self) -> dict[str, Any] | None:
        if self.op == "d":
            return self.before
        return self.after


@dataclass(frozen=True)
class BillingComputation:
    billing_type: BillingType
    amount_minor: int
    amount_unit: AmountSkaling
    billing_period_start: date
    billing_period_end: date
    billing_period_label: str
    correlation_id: str | None


@dataclass(frozen=True)
class LedgerEntry:
    ledger_entry_id: str
    event_id: str
    position_id: str
    correlation_id: str
    corrects_ledger_entry_id: str
    debezium_op: DebeziumOp
    source_table: str
    event_timestamp: datetime
    processed_at: datetime
    billing_type: BillingType
    subscription_id: int
    user_id: int
    product_id: int
    provider_id: int
    amount: int
    amount_unit: AmountSkaling
    billing_period_start: date
    billing_period_end: date
    billing_period_label: str
    created_by: str
    comment: str | None
