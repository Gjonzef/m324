from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import clickhouse_connect  # noqa: E402

from ledger_listener.configuration import Settings  # noqa: E402


@dataclass(frozen=True)
class InvoiceLedgerRow:
    ledger_entry_id: str
    billing_type: str
    subscription_id: int
    user_id: int
    product_id: int
    provider_id: int
    amount: int
    amount_unit: int
    billing_period_start: date
    billing_period_end: date
    billing_period_label: str
    comment: str | None
    monthly_details: dict[str, Any] | None = None
    prorata_details: dict[str, Any] | None = None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Demo invoice run: aggregate billing_ledger rows into invoice JSON documents."
    )
    parser.add_argument(
        "--period",
        default=_current_period_label(),
        help="Billing period label to invoice, for example 2026-04. Default: current month.",
    )
    parser.add_argument("--provider-id", type=int, default=None, help="Optional provider filter")
    parser.add_argument("--user-id", type=int, default=None, help="Optional end-user filter")
    parser.add_argument(
        "--demo-data",
        action="store_true",
        help="Use built-in demo rows instead of reading ClickHouse.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional JSON output file. If omitted, invoices are printed to stdout.",
    )
    return parser.parse_args()


def _current_period_label() -> str:
    today = date.today()
    return f"{today.year:04d}-{today.month:02d}"


def _fetch_ledger_rows(
    settings: Settings,
    period: str,
    provider_id: int | None,
    user_id: int | None,
) -> list[InvoiceLedgerRow]:
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_username,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
        secure=settings.clickhouse_secure,
    )

    where = ["l.billing_period_label = %(period)s"]
    parameters: dict[str, object] = {"period": period}
    if provider_id is not None:
        where.append("l.provider_id = %(provider_id)s")
        parameters["provider_id"] = provider_id
    if user_id is not None:
        where.append("l.user_id = %(user_id)s")
        parameters["user_id"] = user_id

    result = client.query(
        f"""
        SELECT
            l.ledger_entry_id,
            l.billing_type,
            l.subscription_id,
            l.user_id,
            l.product_id,
            l.provider_id,
            l.amount,
            l.amount_unit,
            l.billing_period_start,
            l.billing_period_end,
            l.billing_period_label,
            l.comment,
            m.monthly_price_chf,
            m.service_period_start,
            m.service_period_end,
            m.quantity,
            p.full_period_start,
            p.full_period_end,
            p.active_period_start,
            p.active_period_end,
            p.prorata_days,
            p.full_period_days,
            p.full_amount_chf,
            p.prorata_factor
        FROM {settings.clickhouse_database}.billing_ledger AS l
        LEFT JOIN {settings.clickhouse_database}.billing_monthly_details AS m
            ON l.ledger_entry_id = m.ledger_entry_id
        LEFT JOIN {settings.clickhouse_database}.billing_prorata_details AS p
            ON l.ledger_entry_id = p.ledger_entry_id
        WHERE {" AND ".join(where)}
        ORDER BY l.provider_id, l.user_id, l.billing_period_start, l.subscription_id, l.billing_type, l.processed_at
        """,
        parameters=parameters,
    )
    return [_row_from_clickhouse(row) for row in result.result_rows]


def _row_from_clickhouse(row: tuple[Any, ...]) -> InvoiceLedgerRow:
    return InvoiceLedgerRow(
        ledger_entry_id=str(row[0]),
        billing_type=str(row[1]),
        subscription_id=int(row[2]),
        user_id=int(row[3]),
        product_id=int(row[4]),
        provider_id=int(row[5]),
        amount=int(row[6]),
        amount_unit=int(row[7]),
        billing_period_start=row[8],
        billing_period_end=row[9],
        billing_period_label=str(row[10]),
        comment=row[11],
        monthly_details=_monthly_details_from_row(row) if str(row[1]) == "monthly" else None,
        prorata_details=_prorata_details_from_row(row) if str(row[1]) == "prorata" else None,
    )


def _monthly_details_from_row(row: tuple[Any, ...]) -> dict[str, Any] | None:
    if row[12] is None:
        return None
    return {
        "monthly_price_chf": _money(Decimal(str(row[12]))),
        "service_period_start": row[13].isoformat(),
        "service_period_end": row[14].isoformat(),
        "quantity": int(row[15]),
    }


def _prorata_details_from_row(row: tuple[Any, ...]) -> dict[str, Any] | None:
    if row[16] is None:
        return None
    full_amount_chf = Decimal(str(row[22]))
    prorata_factor = Decimal(str(row[23]))
    expected_amount_chf = full_amount_chf * prorata_factor
    return {
        "full_period_start": row[16].isoformat(),
        "full_period_end": row[17].isoformat(),
        "active_period_start": row[18].isoformat(),
        "active_period_end": row[19].isoformat(),
        "prorata_days": int(row[20]),
        "full_period_days": int(row[21]),
        "full_amount_chf": _money(full_amount_chf),
        "prorata_factor": str(prorata_factor),
        "expected_amount_chf": _money(expected_amount_chf),
    }


def _demo_rows(period: str, provider_id: int | None, user_id: int | None) -> list[InvoiceLedgerRow]:
    year, month = _parse_period(period)
    period_start = date(year, month, 1)
    period_end = _last_day_of_month(period_start)
    rows = [
        InvoiceLedgerRow(
            ledger_entry_id="demo-ledger-001",
            billing_type="setup",
            subscription_id=1001,
            user_id=501,
            product_id=10,
            provider_id=7,
            amount=2500,
            amount_unit=100,
            billing_period_start=period_start,
            billing_period_end=period_start,
            billing_period_label=period,
            comment="Demo setup booking",
        ),
        InvoiceLedgerRow(
            ledger_entry_id="demo-ledger-002",
            billing_type="monthly",
            subscription_id=1001,
            user_id=501,
            product_id=10,
            provider_id=7,
            amount=1990,
            amount_unit=100,
            billing_period_start=period_start,
            billing_period_end=period_end,
            billing_period_label=period,
            comment="Demo monthly booking",
            monthly_details={
                "monthly_price_chf": "19.90",
                "service_period_start": period_start.isoformat(),
                "service_period_end": period_end.isoformat(),
                "quantity": 1,
            },
        ),
        InvoiceLedgerRow(
            ledger_entry_id="demo-ledger-003",
            billing_type="prorata",
            subscription_id=1002,
            user_id=502,
            product_id=11,
            provider_id=7,
            amount=995,
            amount_unit=100,
            billing_period_start=period_start,
            billing_period_end=date(year, month, 15),
            billing_period_label=period,
            comment="Demo prorata booking",
            prorata_details={
                "full_period_start": period_start.isoformat(),
                "full_period_end": period_end.isoformat(),
                "active_period_start": period_start.isoformat(),
                "active_period_end": date(year, month, 15).isoformat(),
                "prorata_days": 15,
                "full_period_days": period_end.day,
                "full_amount_chf": "19.90",
                "prorata_factor": "0.5000",
                "expected_amount_chf": "9.95",
            },
        ),
    ]
    return [
        row
        for row in rows
        if (provider_id is None or row.provider_id == provider_id)
        and (user_id is None or row.user_id == user_id)
    ]


def _parse_period(period: str) -> tuple[int, int]:
    try:
        raw_year, raw_month = period.split("-", maxsplit=1)
        year = int(raw_year)
        month = int(raw_month)
    except ValueError as exc:
        raise ValueError("Period must use YYYY-MM format") from exc
    if month < 1 or month > 12:
        raise ValueError("Period month must be between 01 and 12")
    return year, month


def _last_day_of_month(first_day: date) -> date:
    if first_day.month == 12:
        next_month = date(first_day.year + 1, 1, 1)
    else:
        next_month = date(first_day.year, first_day.month + 1, 1)
    return date.fromordinal(next_month.toordinal() - 1)


def _build_invoices(rows: list[InvoiceLedgerRow], currency: str, created_at: datetime) -> list[dict[str, Any]]:
    grouped: dict[tuple[int, int, str], list[InvoiceLedgerRow]] = defaultdict(list)
    for row in rows:
        grouped[(row.provider_id, row.user_id, row.billing_period_label)].append(row)

    invoices: list[dict[str, Any]] = []
    for (provider_id, user_id, period), group_rows in sorted(grouped.items()):
        invoice_lines = [_build_invoice_line(row, currency) for row in group_rows]
        total_amount_chf = sum((Decimal(str(line["amount_chf"])) for line in invoice_lines), Decimal("0.00"))
        invoices.append(
            {
                "invoice_id": f"demo-invoice-{period}-{provider_id}-{user_id}",
                "invoice_type": "demo",
                "status": "generated",
                "period": period,
                "provider_id": provider_id,
                "user_id": user_id,
                "currency": currency,
                "created_at": created_at.isoformat(),
                "line_count": len(invoice_lines),
                "total_amount_chf": _money(total_amount_chf),
                "lines": invoice_lines,
            }
        )
    return invoices


def _build_invoice_line(row: InvoiceLedgerRow, currency: str) -> dict[str, Any]:
    amount_chf = Decimal(row.amount) / Decimal(row.amount_unit)
    line = {
        "ledger_entry_id": row.ledger_entry_id,
        "billing_type": row.billing_type,
        "subscription_id": row.subscription_id,
        "product_id": row.product_id,
        "service_period_start": row.billing_period_start.isoformat(),
        "service_period_end": row.billing_period_end.isoformat(),
        "amount": row.amount,
        "amount_unit": row.amount_unit,
        "amount_chf": _money(amount_chf),
        "currency": currency,
        "comment": row.comment,
    }
    if row.monthly_details is not None:
        line["monthly_details"] = row.monthly_details
    if row.prorata_details is not None:
        line["prorata_details"] = row.prorata_details
        expected_amount = Decimal(str(row.prorata_details["expected_amount_chf"]))
        line["amount_validation"] = {
            "expected_amount_chf": _money(expected_amount),
            "ledger_amount_chf": _money(amount_chf),
            "matches": _money(expected_amount) == _money(amount_chf),
        }
    return line


def _money(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _write_or_print(document: dict[str, Any], output: Path | None) -> None:
    body = json.dumps(document, indent=2)
    if output is None:
        print(body)
        return
    output.write_text(body + "\n", encoding="utf-8")
    print(f"Wrote demo invoice run to {output}")


def main() -> int:
    args = _parse_args()
    _parse_period(args.period)
    settings = Settings.from_env()
    created_at = datetime.now(timezone.utc)

    if args.demo_data:
        rows = _demo_rows(args.period, args.provider_id, args.user_id)
        source = "demo-data"
    else:
        rows = _fetch_ledger_rows(settings, args.period, args.provider_id, args.user_id)
        source = "clickhouse"

    invoices = _build_invoices(rows, currency=settings.default_currency, created_at=created_at)
    document = {
        "run_type": "demo_invoice_run",
        "source": source,
        "period": args.period,
        "created_at": created_at.isoformat(),
        "invoice_count": len(invoices),
        "ledger_row_count": len(rows),
        "invoices": invoices,
    }
    _write_or_print(document, args.output)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Invoice demo run failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
