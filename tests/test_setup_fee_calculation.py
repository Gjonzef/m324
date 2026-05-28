from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from ledger_listener.events.models import DebeziumEvent, KafkaMetadata
from ledger_listener.mariadb.client import (
    ProductSetupFees,
    SubscriptionBlock,
    SubscriptionNumber,
    SubscriptionSetupContext,
)
from ledger_listener.processor.billing import compute_setup_billing
from ledger_listener.processor.billing import compute_number_assignment_setup_billing


def _create_subscription_event() -> DebeziumEvent:
    return DebeziumEvent(
        op="c",
        before=None,
        after={
            "id": 1001,
            "end_user_id": 501,
            "product_id": 10,
            "created_at": "1711929600000000",
            "updated_at": "1711929600000000",
            "start_datetime": "1711929600000000",
            "end_datetime": "1714521600000000",
        },
        source_name="telephony_dev",
        source_file="mysql-bin.000123",
        source_pos=456,
        source_row=1,
        source_ts_ms=1711929600000,
        source_table="subscription",
        source_db="telephony_dev",
        event_ts=datetime(2026, 4, 1, tzinfo=timezone.utc),
        raw_payload="{}",
        metadata=KafkaMetadata(topic="billing.cdc.subscription", partition=0, offset=42),
    )


def test_setup_fee_complex_calculation_maps_porting_premium_and_blocks() -> None:
    event = _create_subscription_event()
    product_setup_fees = ProductSetupFees(
        porting_costs_ddi=Decimal("10"),
        porting_costs_ina=Decimal("3"),
        porting_costs_isdn=Decimal("40"),
        porting_costs_mobile=Decimal("20"),
        porting_costs_mobile_prepaid=Decimal("30"),
        premium_number_cost_c=Decimal("7"),
        premium_number_cost_c_plus=Decimal("8"),
        premium_number_cost_c_plus_plus=Decimal("9"),
        premium_number_cost_d=Decimal("0"),
        premium_number_cost_d_plus=Decimal("0"),
        premium_number_cost_e=Decimal("0"),
        premium_number_cost_e_plus=Decimal("0"),
        premium_number_cost_e_plus_plus=Decimal("5"),
        number_block_setup_costs={10: Decimal("25"), 100: Decimal("60")},
    )
    setup_context = SubscriptionSetupContext(
        connection_types=["DDI", "MOBILE", "MOBILE_PRE_PAID", "UNKNOWN"],
        numbers=[
            SubscriptionNumber(number_id=1, is_own_number=True, premium_number_class="C"),
            SubscriptionNumber(number_id=2, is_own_number=True, premium_number_class="E++"),
            SubscriptionNumber(number_id=3, is_own_number=False, premium_number_class="C++"),
            SubscriptionNumber(number_id=4, is_own_number=True, premium_number_class=None),
        ],
        blocks=[
            SubscriptionBlock(block_id=100, block_size=10),
            SubscriptionBlock(block_id=101, block_size=10),
            SubscriptionBlock(block_id=102, block_size=100),
        ],
    )

    computation = compute_setup_billing(
        event=event,
        setup_costs=Decimal("999"),
        setup_fees=product_setup_fees,
        setup_context=setup_context,
    )

    # porting: 10 + 20 + 30, premium own numbers: 7 + 5, blocks: 25 + 25 + 60
    assert computation.amount_minor == 18_200


def test_setup_fee_fallback_to_legacy_setup_cost_when_no_context() -> None:
    event = _create_subscription_event()

    computation = compute_setup_billing(
        event=event,
        setup_costs=Decimal("19.90"),
        setup_fees=None,
        setup_context=None,
    )

    assert computation.amount_minor == 1_990


def test_number_assignment_setup_uses_porting_fee_for_non_own_number() -> None:
    event = _create_subscription_event()
    product_setup_fees = ProductSetupFees(
        porting_costs_ddi=Decimal("10"),
        porting_costs_ina=Decimal("3"),
        porting_costs_isdn=Decimal("40"),
        porting_costs_mobile=Decimal("20"),
        porting_costs_mobile_prepaid=Decimal("30"),
        premium_number_cost_c=Decimal("7"),
        premium_number_cost_c_plus=Decimal("8"),
        premium_number_cost_c_plus_plus=Decimal("9"),
        premium_number_cost_d=Decimal("0"),
        premium_number_cost_d_plus=Decimal("0"),
        premium_number_cost_e=Decimal("0"),
        premium_number_cost_e_plus=Decimal("0"),
        premium_number_cost_e_plus_plus=Decimal("5"),
        number_block_setup_costs={},
    )

    computation = compute_number_assignment_setup_billing(
        event=event,
        setup_fees=product_setup_fees,
        is_own_number=False,
        premium_number_class="C++",
        porting_connection_type="MOBILE_PRE_PAID",
    )

    assert computation.amount_minor == 3_000
