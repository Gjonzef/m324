from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
import logging
import uuid

from ledger_listener.clickhouse.client import ClickHouseLedgerClient
from ledger_listener.configuration import Settings
from ledger_listener.debezium.parser import parse_debezium_event
from ledger_listener.events.models import DebeziumEvent, KafkaMetadata, LedgerEntry
from ledger_listener.mariadb.client import MariaDbReadClient
from ledger_listener.processor.billing import compute_number_assignment_setup_billing, compute_setup_billing

logger = logging.getLogger(__name__)
ALLOWED_MONTHLY_BILLING_TYPES = {"monthly", "prorata", "correction"}


@dataclass(frozen=True)
class ProcessResult:
    wrote_clickhouse: bool
    duplicate: bool
    should_commit: bool


class EventWorker:
    def __init__(self, settings: Settings, clickhouse: ClickHouseLedgerClient, mariadb: MariaDbReadClient):
        self._settings = settings
        self._clickhouse = clickhouse
        self._mariadb = mariadb

    def process_cdc_payload(self, raw_payload: bytes | str, metadata: KafkaMetadata) -> ProcessResult:
        if metadata.topic == self._settings.monthly_billing_topic:
            return self._process_monthly_billing_payload(raw_payload)

        event = parse_debezium_event(raw_payload, metadata)
        payload = event.payload
        if payload is None:
            raise ValueError("Debezium payload is empty")

        if event.source_name and event.source_name != self._settings.debezium_connector_name:
            logger.info("Skipping event from connector=%s", event.source_name)
            return ProcessResult(wrote_clickhouse=False, duplicate=False, should_commit=True)

        if event.source_db and event.source_db != self._settings.source_database_name:
            logger.info("Skipping event from source_db=%s", event.source_db)
            return ProcessResult(wrote_clickhouse=False, duplicate=False, should_commit=True)

        if event.source_table == self._settings.subscription_table_name:
            if event.op != "c":
                logger.info("Skipping non-create subscription event op=%s", event.op)
                return ProcessResult(wrote_clickhouse=False, duplicate=False, should_commit=True)
            return self._process_subscription_create_event(event=event, payload=payload, metadata=metadata)

        if event.source_table == "number":
            if event.op != "u":
                logger.info("Skipping non-update number event op=%s", event.op)
                return ProcessResult(wrote_clickhouse=False, duplicate=False, should_commit=True)
            return self._process_number_assignment_event(event=event, metadata=metadata)

        logger.info("Skipping table=%s (no billing rule configured)", event.source_table)
        return ProcessResult(wrote_clickhouse=False, duplicate=False, should_commit=True)

    def _process_subscription_create_event(
        self,
        event: DebeziumEvent,
        payload: dict,
        metadata: KafkaMetadata,
    ) -> ProcessResult:
        _validate_subscription_create_payload(payload)

        subscription_id = int(payload.get("id"))
        user_id = int(payload.get("end_user_id"))
        product_id = int(payload.get("product_id"))

        product = self._mariadb.fetch_product_pricing(product_id)
        setup_context = self._mariadb.fetch_subscription_setup_context(subscription_id)
        provider_id = self._mariadb.fetch_end_user_provider_id(user_id)
        computation = compute_setup_billing(
            event=event,
            setup_costs=product.setup_costs,
            setup_fees=product.setup_fees,
            setup_context=setup_context,
        )

        return self._insert_setup_entry(
            event=event,
            metadata=metadata,
            subscription_id=subscription_id,
            user_id=user_id,
            product_id=product.product_id,
            provider_id=provider_id,
            amount_minor=computation.amount_minor,
            amount_unit=computation.amount_unit,
            billing_period_start=computation.billing_period_start,
            billing_period_end=computation.billing_period_end,
            billing_period_label=computation.billing_period_label,
        )

    def _process_number_assignment_event(self, event: DebeziumEvent, metadata: KafkaMetadata) -> ProcessResult:
        before = event.before if isinstance(event.before, dict) else {}
        after = event.after if isinstance(event.after, dict) else {}

        before_subscription_id = _as_nullable_int(before.get("subscription_id"))
        after_subscription_id = _as_nullable_int(after.get("subscription_id"))
        if before_subscription_id is not None or after_subscription_id is None:
            logger.info(
                "Skipping number update without NULL->assigned transition. before=%s after=%s",
                before_subscription_id,
                after_subscription_id,
            )
            return ProcessResult(wrote_clickhouse=False, duplicate=False, should_commit=True)

        subscription_info = self._mariadb.fetch_subscription_billing_info(after_subscription_id)
        product = self._mariadb.fetch_product_pricing(subscription_info.product_id)
        provider_id = self._mariadb.fetch_end_user_provider_id(subscription_info.end_user_id)

        is_own_number = _as_int("is_own_number", after.get("is_own_number", 0)) == 1
        premium_number_class = _as_nullable_str(after.get("premium_number_class"))
        national_number = _as_nullable_str(after.get("national_number"))
        porting_connection_type = None
        if not is_own_number and national_number:
            porting_connection_type = self._mariadb.fetch_latest_porting_connection_type_for_number(
                subscription_id=after_subscription_id,
                national_number=national_number,
            )

        computation = compute_number_assignment_setup_billing(
            event=event,
            setup_fees=product.setup_fees,
            is_own_number=is_own_number,
            premium_number_class=premium_number_class,
            porting_connection_type=porting_connection_type,
        )
        number_id = _as_int("id", after.get("id"))

        # Assumption: keep existing position hash logic for now.
        position_material = (
            f"{after_subscription_id}|{number_id}|{computation.billing_period_start.isoformat()}|"
            f"{computation.billing_type}"
        )
        number_position_id = hashlib.sha256(position_material.encode("utf-8")).hexdigest()

        return self._insert_setup_entry(
            event=event,
            metadata=metadata,
            subscription_id=after_subscription_id,
            user_id=subscription_info.end_user_id,
            product_id=product.product_id,
            provider_id=provider_id,
            amount_minor=computation.amount_minor,
            amount_unit=computation.amount_unit,
            billing_period_start=computation.billing_period_start,
            billing_period_end=computation.billing_period_end,
            billing_period_label=computation.billing_period_label,
            position_id_override=number_position_id,
        )

    def _insert_setup_entry(
        self,
        event: DebeziumEvent,
        metadata: KafkaMetadata,
        subscription_id: int,
        user_id: int,
        product_id: int,
        provider_id: int,
        amount_minor: int,
        amount_unit: int,
        billing_period_start: date,
        billing_period_end: date,
        billing_period_label: str,
        position_id_override: str | None = None,
    ) -> ProcessResult:
        if position_id_override:
            position_id = position_id_override
        else:
            position_material = f"{subscription_id}|{billing_period_start.isoformat()}|setup"
            position_id = hashlib.sha256(position_material.encode("utf-8")).hexdigest()

        if event.source_file and event.source_pos is not None and event.source_row is not None:
            event_id = f"{event.source_file}:{event.source_pos}:{event.source_row}"
        else:
            event_id = f"{metadata.topic}:{metadata.partition}:{metadata.offset}"

        if self._clickhouse.position_exists(position_id):
            logger.info("Duplicate event skipped. position_id=%s", position_id)
            return ProcessResult(wrote_clickhouse=False, duplicate=True, should_commit=True)

        source_table = event.source_table or self._settings.subscription_table_name
        entry = LedgerEntry(
            ledger_entry_id=str(uuid.uuid4()),
            event_id=event_id,
            position_id=position_id,
            correlation_id="",
            corrects_ledger_entry_id="",
            debezium_op=event.op,
            source_table=source_table,
            event_timestamp=event.event_ts,
            processed_at=datetime.now(timezone.utc),
            billing_type="setup",
            subscription_id=subscription_id,
            user_id=user_id,
            product_id=product_id,
            provider_id=provider_id,
            amount=amount_minor,
            amount_unit=amount_unit,
            billing_period_start=billing_period_start,
            billing_period_end=billing_period_end,
            billing_period_label=billing_period_label,
            created_by="ledger-system",
            comment=None,
        )

        self._clickhouse.insert_ledger_entry(entry)
        logger.info(
            "Inserted ledger entry. subscription_id=%s position_id=%s type=%s amount=%s",
            entry.subscription_id,
            entry.position_id,
            entry.billing_type,
            entry.amount,
        )
        return ProcessResult(wrote_clickhouse=True, duplicate=False, should_commit=True)

    def _process_monthly_billing_payload(self, raw_payload: bytes | str) -> ProcessResult:
        payload_str = raw_payload.decode("utf-8") if isinstance(raw_payload, bytes) else raw_payload
        document = json.loads(payload_str)
        payload = document.get("payload") if isinstance(document.get("payload"), dict) else document
        if not isinstance(payload, dict):
            raise ValueError("Monthly billing payload must be a JSON object")

        _validate_monthly_billing_payload(payload)

        position_id = _as_non_empty_str("position_id", payload.get("position_id"))
        if self._clickhouse.position_exists(position_id):
            logger.info("Duplicate monthly billing event skipped. position_id=%s", position_id)
            return ProcessResult(wrote_clickhouse=False, duplicate=True, should_commit=True)

        entry = LedgerEntry(
            ledger_entry_id=str(uuid.uuid4()),
            event_id=_as_non_empty_str("event_id", payload.get("event_id")),
            position_id=position_id,
            correlation_id=_as_optional_str(payload.get("correlation_id")),
            corrects_ledger_entry_id=_as_optional_str(payload.get("corrects_ledger_entry_id")),
            debezium_op="c",
            source_table=_as_non_empty_str("source_table", payload.get("source_table")),
            event_timestamp=_as_iso_datetime("event_timestamp", payload.get("event_timestamp")),
            processed_at=datetime.now(timezone.utc),
            billing_type=_as_non_empty_str("billing_type", payload.get("billing_type")),
            subscription_id=_as_int("subscription_id", payload.get("subscription_id")),
            user_id=_as_int("user_id", payload.get("user_id")),
            product_id=_as_int("product_id", payload.get("product_id")),
            provider_id=_as_int("provider_id", payload.get("provider_id")),
            amount=_as_int("amount", payload.get("amount")),
            amount_unit=_parse_amount_unit(payload.get("amount_unit")),
            billing_period_start=_as_iso_date("billing_period_start", payload.get("billing_period_start")),
            billing_period_end=_as_iso_date("billing_period_end", payload.get("billing_period_end")),
            billing_period_label=_as_non_empty_str("billing_period_label", payload.get("billing_period_label")),
            created_by=_as_non_empty_str("created_by", payload.get("created_by")),
            comment=_as_nullable_str(payload.get("comment")),
        )

        if entry.billing_period_end < entry.billing_period_start:
            raise ValueError("billing_period_end must be greater than or equal to billing_period_start")

        self._clickhouse.insert_ledger_entry(entry)
        self._insert_details_for_monthly_or_prorata(entry, payload)
        logger.info(
            "Inserted monthly/prorata billing entry. subscription_id=%s position_id=%s billing_type=%s amount=%s",
            entry.subscription_id,
            entry.position_id,
            entry.billing_type,
            entry.amount,
        )
        return ProcessResult(wrote_clickhouse=True, duplicate=False, should_commit=True)

    def _insert_details_for_monthly_or_prorata(self, entry: LedgerEntry, payload: dict) -> None:
        try:
            if entry.billing_type == "monthly":
                details = _build_monthly_details(entry, payload)
                self._clickhouse.insert_monthly_details(
                    ledger_entry_id=entry.ledger_entry_id,
                    monthly_price_chf=details["monthly_price_chf"],
                    service_period_start=details["service_period_start"],
                    service_period_end=details["service_period_end"],
                    quantity=details["quantity"],
                )
                return

            if entry.billing_type == "prorata":
                details = _build_prorata_details(entry, payload)
                self._clickhouse.insert_prorata_details(
                    ledger_entry_id=entry.ledger_entry_id,
                    full_period_start=details["full_period_start"],
                    full_period_end=details["full_period_end"],
                    active_period_start=details["active_period_start"],
                    active_period_end=details["active_period_end"],
                    prorata_days=details["prorata_days"],
                    full_period_days=details["full_period_days"],
                    full_amount_chf=details["full_amount_chf"],
                    prorata_factor=details["prorata_factor"],
                )
        except Exception:
            # Ledger write remains primary; details are best effort for now.
            logger.exception(
                "Failed to insert billing details. ledger_entry_id=%s billing_type=%s",
                entry.ledger_entry_id,
                entry.billing_type,
            )


def _validate_subscription_create_payload(payload: dict) -> None:
    required_fields = ("id", "end_user_id", "product_id", "created_at", "updated_at", "start_datetime")
    missing = [field for field in required_fields if payload.get(field) in (None, "")]
    if missing:
        raise ValueError(f"Missing required subscription create fields: {', '.join(missing)}")

    _as_int("id", payload["id"])
    _as_int("product_id", payload["product_id"])
    _as_microsecond_string("created_at", payload["created_at"])
    _as_microsecond_string("updated_at", payload["updated_at"])
    _as_microsecond_string("start_datetime", payload["start_datetime"])
    if payload.get("end_datetime") not in (None, ""):
        _as_microsecond_string("end_datetime", payload["end_datetime"])


def _validate_monthly_billing_payload(payload: dict) -> None:
    required_fields = (
        "event_id",
        "position_id",
        "billing_type",
        "subscription_id",
        "user_id",
        "product_id",
        "provider_id",
        "amount",
        "amount_unit",
        "billing_period_start",
        "billing_period_end",
        "billing_period_label",
        "event_timestamp",
        "source_table",
        "created_by",
    )
    missing = [field for field in required_fields if payload.get(field) in (None, "")]
    if missing:
        raise ValueError(f"Missing required monthly billing fields: {', '.join(missing)}")

    billing_type = _as_non_empty_str("billing_type", payload["billing_type"])
    if billing_type not in ALLOWED_MONTHLY_BILLING_TYPES:
        allowed = ", ".join(sorted(ALLOWED_MONTHLY_BILLING_TYPES))
        raise ValueError(f"Unsupported monthly billing_type: {billing_type!r}. Allowed values: {allowed}")


def _build_monthly_details(entry: LedgerEntry, payload: dict) -> dict[str, object]:
    details = payload.get("monthly_details") if isinstance(payload.get("monthly_details"), dict) else payload
    service_period_start = _as_iso_date(
        "service_period_start",
        details.get("service_period_start", entry.billing_period_start.isoformat()),
    )
    service_period_end = _as_iso_date(
        "service_period_end",
        details.get("service_period_end", entry.billing_period_end.isoformat()),
    )
    if service_period_end < service_period_start:
        raise ValueError("service_period_end must be greater than or equal to service_period_start")

    monthly_price_chf = _as_decimal_2(
        "monthly_price_chf",
        details.get("monthly_price_chf", _details_amount_chf(entry.amount, entry.amount_unit)),
    )
    quantity = _as_uint32("quantity", details.get("quantity", 1))
    return {
        "monthly_price_chf": monthly_price_chf,
        "service_period_start": service_period_start,
        "service_period_end": service_period_end,
        "quantity": quantity,
    }


def _build_prorata_details(entry: LedgerEntry, payload: dict) -> dict[str, object]:
    details = payload.get("prorata_details") if isinstance(payload.get("prorata_details"), dict) else payload

    full_period_start = _as_iso_date(
        "full_period_start",
        details.get("full_period_start", entry.billing_period_start.isoformat()),
    )
    full_period_end = _as_iso_date(
        "full_period_end",
        details.get("full_period_end", entry.billing_period_end.isoformat()),
    )
    active_period_start = _as_iso_date(
        "active_period_start",
        details.get("active_period_start", entry.billing_period_start.isoformat()),
    )
    active_period_end = _as_iso_date(
        "active_period_end",
        details.get("active_period_end", entry.billing_period_end.isoformat()),
    )
    if full_period_end < full_period_start:
        raise ValueError("full_period_end must be greater than or equal to full_period_start")
    if active_period_end < active_period_start:
        raise ValueError("active_period_end must be greater than or equal to active_period_start")

    full_period_days_default = (full_period_end - full_period_start).days + 1
    prorata_days_default = (active_period_end - active_period_start).days + 1
    full_period_days = _as_uint32("full_period_days", details.get("full_period_days", full_period_days_default))
    prorata_days = _as_uint32("prorata_days", details.get("prorata_days", prorata_days_default))
    if full_period_days == 0:
        raise ValueError("full_period_days must be greater than 0")

    full_amount_chf = _as_decimal_2(
        "full_amount_chf",
        details.get("full_amount_chf", _details_amount_chf(entry.amount, entry.amount_unit)),
    )
    prorata_factor = _as_decimal_4(
        "prorata_factor",
        details.get("prorata_factor", Decimal(prorata_days) / Decimal(full_period_days)),
    )
    return {
        "full_period_start": full_period_start,
        "full_period_end": full_period_end,
        "active_period_start": active_period_start,
        "active_period_end": active_period_end,
        "prorata_days": prorata_days,
        "full_period_days": full_period_days,
        "full_amount_chf": full_amount_chf,
        "prorata_factor": prorata_factor,
    }


def _as_int(name: str, value: object) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Field '{name}' must be an integer-like value") from exc


def _as_nullable_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    return int(str(value))


def _as_uint32(name: str, value: object) -> int:
    parsed = _as_int(name, value)
    if parsed < 0 or parsed > 4_294_967_295:
        raise ValueError(f"Field '{name}' must be between 0 and 4294967295")
    return parsed


def _as_microsecond_string(name: str, value: object) -> datetime:
    raw = str(value)
    if not raw.isdigit():
        raise ValueError(f"Field '{name}' must be a numeric microsecond timestamp string")
    micros = int(raw)
    return datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc)


def _as_non_empty_str(name: str, value: object) -> str:
    raw = str(value).strip() if value is not None else ""
    if not raw:
        raise ValueError(f"Field '{name}' must be a non-empty string")
    return raw


def _as_optional_str(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _as_nullable_str(value: object) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    return raw if raw else None


def _as_iso_date(name: str, value: object) -> date:
    raw = _as_non_empty_str(name, value)
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"Field '{name}' must be an ISO date (YYYY-MM-DD)") from exc


def _as_iso_datetime(name: str, value: object) -> datetime:
    raw = _as_non_empty_str(name, value)
    normalized = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Field '{name}' must be an ISO-8601 datetime") from exc

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_amount_unit(value: object) -> int:
    if isinstance(value, str):
        unit = value.strip().lower()
        if unit == "unit":
            return 100
        if unit in {"microunit", "micro_unit"}:
            return 1_000_000
    parsed = _as_int("amount_unit", value)
    if parsed not in {100, 1_000_000}:
        raise ValueError("Field 'amount_unit' must be 'unit', 'microunit', 100 or 1000000")
    return parsed


def _as_decimal_2(name: str, value: object) -> Decimal:
    return _as_decimal(name, value, places=2)


def _as_decimal_4(name: str, value: object) -> Decimal:
    return _as_decimal(name, value, places=4)


def _as_decimal(name: str, value: object, places: int) -> Decimal:
    quantize = Decimal("1").scaleb(-places)
    try:
        parsed = Decimal(str(value)).quantize(quantize, rounding=ROUND_HALF_UP)
    except Exception as exc:
        raise ValueError(f"Field '{name}' must be a decimal-like value") from exc
    return parsed


def _details_amount_chf(amount: int, amount_unit: int) -> Decimal:
    return (Decimal(amount) / Decimal(amount_unit)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
