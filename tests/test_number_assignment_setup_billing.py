import json
from dataclasses import dataclass
from decimal import Decimal
from unittest.mock import MagicMock

from ledger_listener.mariadb.client import ProductPricing, ProductSetupFees
from ledger_listener.processor.worker import EventWorker


@dataclass(frozen=True)
class _SubscriptionBillingInfoStub:
    subscription_id: int
    end_user_id: int
    product_id: int


def _number_update_payload(before_subscription_id, after_subscription_id, is_own_number, national_number) -> dict:
    return {
        "payload": {
            "before": {
                "id": 777,
                "subscription_id": before_subscription_id,
                "is_own_number": is_own_number,
                "premium_number_class": "C",
                "national_number": national_number,
            },
            "after": {
                "id": 777,
                "subscription_id": after_subscription_id,
                "is_own_number": is_own_number,
                "premium_number_class": "C",
                "national_number": national_number,
                "updated_at": "1712016000000000",
            },
            "op": "u",
            "source": {
                "name": "telephony_dev",
                "db": "telephony_dev",
                "table": "number",
                "file": "mysql-bin.000123",
                "pos": 789,
                "row": 2,
                "ts_ms": 1712016000000,
            },
            "ts_ms": 1712016000100,
        }
    }


def _product_pricing_with_setup_fees() -> ProductPricing:
    return ProductPricing(
        product_id=10,
        name="test",
        type="voice",
        setup_costs=Decimal("0"),
        monthly_costs=Decimal("0"),
        setup_fees=ProductSetupFees(
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
        ),
    )


def test_number_update_null_to_assigned_uses_porting_fee(settings, metadata) -> None:
    clickhouse = MagicMock()
    mariadb = MagicMock()
    worker = EventWorker(settings=settings, clickhouse=clickhouse, mariadb=mariadb)

    inserted_entries = []

    def _insert(entry):
        inserted_entries.append(entry)

    clickhouse.position_exists.return_value = False
    clickhouse.insert_ledger_entry.side_effect = _insert
    mariadb.fetch_subscription_billing_info.return_value = _SubscriptionBillingInfoStub(
        subscription_id=1001,
        end_user_id=501,
        product_id=10,
    )
    mariadb.fetch_product_pricing.return_value = _product_pricing_with_setup_fees()
    mariadb.fetch_end_user_provider_id.return_value = 7
    mariadb.fetch_latest_porting_connection_type_for_number.return_value = "MOBILE"

    payload = _number_update_payload(
        before_subscription_id=None,
        after_subscription_id=1001,
        is_own_number=0,
        national_number="41441234567",
    )

    result = worker.process_cdc_payload(json.dumps(payload), metadata)

    assert result.wrote_clickhouse is True
    assert len(inserted_entries) == 1
    assert inserted_entries[0].amount == 2_000
    mariadb.fetch_latest_porting_connection_type_for_number.assert_called_once_with(
        subscription_id=1001,
        national_number="41441234567",
    )


def test_number_update_without_null_to_assigned_transition_is_skipped(settings, metadata) -> None:
    clickhouse = MagicMock()
    mariadb = MagicMock()
    worker = EventWorker(settings=settings, clickhouse=clickhouse, mariadb=mariadb)

    payload = _number_update_payload(
        before_subscription_id=1001,
        after_subscription_id=1001,
        is_own_number=0,
        national_number="41441234567",
    )

    result = worker.process_cdc_payload(json.dumps(payload), metadata)

    assert result.wrote_clickhouse is False
    assert result.duplicate is False
    mariadb.fetch_subscription_billing_info.assert_not_called()
    clickhouse.insert_ledger_entry.assert_not_called()
