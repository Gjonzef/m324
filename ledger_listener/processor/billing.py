from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

from ledger_listener.events.models import BillingComputation, DebeziumEvent
from ledger_listener.mariadb.client import ProductSetupFees, SubscriptionSetupContext

RAPPE_FACTOR = Decimal("100")

PORTING_COST_FIELD_BY_CONNECTION_TYPE = {
    "DDI": "porting_costs_ddi",
    "MOBILE": "porting_costs_mobile",
    "MOBILE_PRE_PAID": "porting_costs_mobile_prepaid",
    "PSTN_ISDN": "porting_costs_isdn",
}

PREMIUM_COST_FIELD_BY_CLASS = {
    "C": "premium_number_cost_c",
    "C+": "premium_number_cost_c_plus",
    "C++": "premium_number_cost_c_plus_plus",
    "D": "premium_number_cost_d",
    "D+": "premium_number_cost_d_plus",
    "E": "premium_number_cost_e",
    "E+": "premium_number_cost_e_plus",
    "E++": "premium_number_cost_e_plus_plus",
}


def _to_decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _period_from_created_at(payload_created_at: object, fallback_ts: datetime) -> date:
    if payload_created_at is None:
        return date(fallback_ts.year, fallback_ts.month, 1)
    micros = int(str(payload_created_at))
    ts = datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc)
    return date(ts.year, ts.month, 1)


def compute_setup_billing(
    event: DebeziumEvent,
    setup_costs: Decimal,
    setup_fees: ProductSetupFees | None = None,
    setup_context: SubscriptionSetupContext | None = None,
) -> BillingComputation:
    payload = event.payload
    if payload is None:
        raise ValueError("Missing Debezium payload")

    if event.op != "c":
        raise ValueError(f"Unsupported op for setup billing: {event.op}")

    start, end, label = _setup_period_for_event(payload=payload, fallback=event.event_ts)

    effective_setup_costs = _to_decimal(setup_costs)
    if setup_fees is not None and setup_context is not None:
        effective_setup_costs = _compute_complex_setup_costs(setup_fees, setup_context)

    amount_minor = int((effective_setup_costs * RAPPE_FACTOR).quantize(Decimal("1"), rounding=ROUND_HALF_UP))

    return BillingComputation(
        billing_type="setup",
        amount_minor=amount_minor,
        amount_unit=100,
        billing_period_start=start,
        billing_period_end=end,
        billing_period_label=label,
        correlation_id=None,
    )


def compute_number_assignment_setup_billing(
    event: DebeziumEvent,
    setup_fees: ProductSetupFees,
    is_own_number: bool,
    premium_number_class: str | None,
    porting_connection_type: str | None,
) -> BillingComputation:
    payload = event.payload
    if payload is None:
        raise ValueError("Missing Debezium payload")

    amount = Decimal("0")
    if is_own_number:
        fee_field = PREMIUM_COST_FIELD_BY_CLASS.get((premium_number_class or "").upper())
        if fee_field is not None:
            amount += _to_decimal(getattr(setup_fees, fee_field))
    else:
        fee_field = PORTING_COST_FIELD_BY_CONNECTION_TYPE.get((porting_connection_type or "").upper())
        if fee_field is not None:
            amount += _to_decimal(getattr(setup_fees, fee_field))

    start, end, label = _setup_period_for_event(payload=payload, fallback=event.event_ts)
    amount_minor = int((amount * RAPPE_FACTOR).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    return BillingComputation(
        billing_type="setup",
        amount_minor=amount_minor,
        amount_unit=100,
        billing_period_start=start,
        billing_period_end=end,
        billing_period_label=label,
        correlation_id=None,
    )


def _compute_complex_setup_costs(setup_fees: ProductSetupFees, setup_context: SubscriptionSetupContext) -> Decimal:
    total = Decimal("0")

    for connection_type in setup_context.connection_types:
        fee_field = PORTING_COST_FIELD_BY_CONNECTION_TYPE.get(connection_type)
        if fee_field is None:
            continue
        total += _to_decimal(getattr(setup_fees, fee_field))

    for number in setup_context.numbers:
        if not number.is_own_number:
            # Ported numbers are priced via number_porting costs, not premium own-number pricing.
            continue
        fee_field = PREMIUM_COST_FIELD_BY_CLASS.get((number.premium_number_class or "").upper())
        if fee_field is None:
            continue
        total += _to_decimal(getattr(setup_fees, fee_field))

    for block in setup_context.blocks:
        if block.block_size <= 0:
            continue
        total += _to_decimal(setup_fees.number_block_setup_costs.get(block.block_size, Decimal("0")))

    return total


def _setup_period_for_event(payload: dict, fallback: datetime) -> tuple[date, date, str]:
    start = _date_from_microseconds(
        payload.get("start_datetime", payload.get("updated_at", payload.get("created_at"))),
        fallback=fallback,
    )
    end = _date_from_microseconds(payload.get("end_datetime"), fallback=fallback)
    if end < start:
        end = start
    label = f"{start.year:04d}-{start.month:02d}"
    return start, end, label


def _date_from_microseconds(value: object, fallback: datetime) -> date:
    if value in (None, ""):
        return date(fallback.year, fallback.month, fallback.day)
    micros = int(str(value))
    ts = datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc)
    return date(ts.year, ts.month, ts.day)
