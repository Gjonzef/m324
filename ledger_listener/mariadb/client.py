from dataclasses import dataclass, field
from decimal import Decimal
import json
from typing import Any

from ledger_listener.configuration import Settings


@dataclass(frozen=True)
class ProductSetupFees:
    porting_costs_ddi: Decimal
    porting_costs_ina: Decimal
    porting_costs_isdn: Decimal
    porting_costs_mobile: Decimal
    porting_costs_mobile_prepaid: Decimal
    premium_number_cost_c: Decimal
    premium_number_cost_c_plus: Decimal
    premium_number_cost_c_plus_plus: Decimal
    premium_number_cost_d: Decimal
    premium_number_cost_d_plus: Decimal
    premium_number_cost_e: Decimal
    premium_number_cost_e_plus: Decimal
    premium_number_cost_e_plus_plus: Decimal
    number_block_setup_costs: dict[int, Decimal] = field(default_factory=dict)


@dataclass(frozen=True)
class ProductPricing:
    product_id: int
    name: str
    type: str
    setup_costs: Decimal
    monthly_costs: Decimal
    setup_fees: ProductSetupFees


@dataclass(frozen=True)
class SubscriptionNumber:
    number_id: int
    is_own_number: bool
    premium_number_class: str | None


@dataclass(frozen=True)
class SubscriptionBlock:
    block_id: int
    block_size: int


@dataclass(frozen=True)
class SubscriptionSetupContext:
    connection_types: list[str]
    numbers: list[SubscriptionNumber]
    blocks: list[SubscriptionBlock]


@dataclass(frozen=True)
class SubscriptionBillingInfo:
    subscription_id: int
    end_user_id: int
    product_id: int


class MariaDbReadClient:
    def __init__(self, settings: Settings):
        self._settings = settings

    def fetch_product_pricing(self, product_id: int) -> ProductPricing:
        conn = self._connect()
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        name,
                        type,
                        setup_costs,
                        monthly_costs,
                        porting_costs_ddi,
                        porting_costs_ina,
                        porting_costs_isdn,
                        porting_costs_mobile,
                        porting_costs_mobile_prepaid,
                        premium_number_cost_c,
                        premium_number_cost_c_plus,
                        premium_number_cost_c_plus_plus,
                        premium_number_cost_d,
                        premium_number_cost_d_plus,
                        premium_number_cost_e,
                        premium_number_cost_e_plus,
                        premium_number_cost_e_plus_plus,
                        number_block_costs
                    FROM product
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (product_id,),
                )
                row = cur.fetchone()
        finally:
            conn.close()

        if not row:
            raise ValueError(f"Product not found for id={product_id}")

        return ProductPricing(
            product_id=int(row["id"]),
            name=str(row.get("name") or ""),
            type=str(row.get("type") or ""),
            setup_costs=_to_decimal(row.get("setup_costs")),
            monthly_costs=_to_decimal(row.get("monthly_costs")),
            setup_fees=ProductSetupFees(
                porting_costs_ddi=_to_decimal(row.get("porting_costs_ddi")),
                porting_costs_ina=_to_decimal(row.get("porting_costs_ina")),
                porting_costs_isdn=_to_decimal(row.get("porting_costs_isdn")),
                porting_costs_mobile=_to_decimal(row.get("porting_costs_mobile")),
                porting_costs_mobile_prepaid=_to_decimal(row.get("porting_costs_mobile_prepaid")),
                premium_number_cost_c=_to_decimal(row.get("premium_number_cost_c")),
                premium_number_cost_c_plus=_to_decimal(row.get("premium_number_cost_c_plus")),
                premium_number_cost_c_plus_plus=_to_decimal(row.get("premium_number_cost_c_plus_plus")),
                premium_number_cost_d=_to_decimal(row.get("premium_number_cost_d")),
                premium_number_cost_d_plus=_to_decimal(row.get("premium_number_cost_d_plus")),
                premium_number_cost_e=_to_decimal(row.get("premium_number_cost_e")),
                premium_number_cost_e_plus=_to_decimal(row.get("premium_number_cost_e_plus")),
                premium_number_cost_e_plus_plus=_to_decimal(row.get("premium_number_cost_e_plus_plus")),
                number_block_setup_costs=_parse_number_block_costs(row.get("number_block_costs")),
            ),
        )

    def fetch_subscription_setup_context(self, subscription_id: int) -> SubscriptionSetupContext:
        conn = self._connect()
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute(
                    """
                    SELECT connection_type
                    FROM number_porting
                    WHERE subscription_id = %s
                    """,
                    (subscription_id,),
                )
                connection_type_rows = cur.fetchall()

                cur.execute(
                    """
                    SELECT id, is_own_number, premium_number_class
                    FROM `number`
                    WHERE subscription_id = %s
                    """,
                    (subscription_id,),
                )
                number_rows = cur.fetchall()

                cur.execute(
                    """
                    SELECT id, block_size
                    FROM number_block
                    WHERE subscription_id = %s
                    """,
                    (subscription_id,),
                )
                block_rows = cur.fetchall()
        finally:
            conn.close()

        connection_types = [str(row.get("connection_type") or "").strip().upper() for row in connection_type_rows]

        numbers = [
            SubscriptionNumber(
                number_id=int(row["id"]),
                is_own_number=_to_bool_flag(row.get("is_own_number")),
                premium_number_class=_normalize_premium_class(row.get("premium_number_class")),
            )
            for row in number_rows
            if row.get("id") is not None
        ]

        blocks = [
            SubscriptionBlock(
                block_id=int(row["id"]),
                block_size=int(str(row.get("block_size") or "0")),
            )
            for row in block_rows
            if row.get("id") is not None and row.get("block_size") not in (None, "")
        ]

        return SubscriptionSetupContext(connection_types=connection_types, numbers=numbers, blocks=blocks)

    def fetch_subscription_billing_info(self, subscription_id: int) -> SubscriptionBillingInfo:
        conn = self._connect()
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute(
                    """
                    SELECT id, end_user_id, product_id
                    FROM subscription
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (subscription_id,),
                )
                row = cur.fetchone()
        finally:
            conn.close()

        if not row:
            raise ValueError(f"subscription not found for id={subscription_id}")
        if row.get("end_user_id") is None:
            raise ValueError(f"subscription.end_user_id not found for subscription_id={subscription_id}")
        if row.get("product_id") is None:
            raise ValueError(f"subscription.product_id not found for subscription_id={subscription_id}")

        return SubscriptionBillingInfo(
            subscription_id=int(row["id"]),
            end_user_id=int(row["end_user_id"]),
            product_id=int(row["product_id"]),
        )

    def fetch_latest_porting_connection_type_for_number(
        self,
        subscription_id: int,
        national_number: str,
    ) -> str | None:
        conn = self._connect()
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute(
                    """
                    SELECT np.connection_type
                    FROM number_porting_subscriber_number npsn
                    JOIN number_porting np ON np.id = npsn.porting_id
                    WHERE np.subscription_id = %s
                      AND npsn.subscriber_number = %s
                    ORDER BY npsn.id DESC
                    LIMIT 1
                    """,
                    (subscription_id, national_number),
                )
                row = cur.fetchone()
        finally:
            conn.close()

        if not row or row.get("connection_type") in (None, ""):
            return None
        return str(row.get("connection_type")).strip().upper()

    def fetch_end_user_provider_id(self, end_user_id: int) -> int:
        conn = self._connect()
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute(
                    """
                    SELECT provider_id
                    FROM end_user
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (end_user_id,),
                )
                row = cur.fetchone()
        finally:
            conn.close()

        if not row or row.get("provider_id") is None:
            raise ValueError(f"end_user.provider_id not found for end_user_id={end_user_id}")
        return int(row["provider_id"])

    def _connect(self):
        import mysql.connector

        return mysql.connector.connect(
            host=self._settings.mariadb_host,
            port=self._settings.mariadb_port,
            user=self._settings.mariadb_user,
            password=self._settings.mariadb_password,
            database=self._settings.mariadb_database,
            connection_timeout=self._settings.mariadb_connect_timeout_seconds,
            charset=self._settings.mariadb_charset,
            collation=self._settings.mariadb_collation,
            use_unicode=True,
            autocommit=True,
        )


def _to_decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def _to_bool_flag(value: Any) -> bool:
    if value in (None, ""):
        return False
    return int(str(value)) == 1


def _normalize_premium_class(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def _parse_number_block_costs(value: Any) -> dict[int, Decimal]:
    if value in (None, ""):
        return {}

    payload: Any = value
    if isinstance(value, str):
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            return {}

    if not isinstance(payload, list):
        return {}

    parsed: dict[int, Decimal] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue

        block_size = item.get("block_size")
        setup_costs = item.get("setup_costs")
        if block_size in (None, "") or setup_costs in (None, ""):
            continue

        try:
            parsed[int(str(block_size))] = _to_decimal(setup_costs)
        except (TypeError, ValueError):
            continue

    return parsed
