from __future__ import annotations


# NOTE: Placeholder adapter for TC-10.
# Replace this with your production invoice-freeze function once available.
def run_invoice_freeze(rows: list[dict], already_frozen_position_ids: set[str]) -> dict:
    frozen_ids: set[str] = set(already_frozen_position_ids)
    inserted: list[dict] = []

    for row in rows:
        position_id = row["position_id"]
        if position_id in frozen_ids:
            continue
        frozen_ids.add(position_id)
        inserted.append(
            {
                **row,
                "state": "frozen",
                "invoice_reference": f"INV-{row['billing_period_label']}-{row['provider_id']}-{row['user_id']}",
            }
        )

    return {"inserted": inserted, "frozen_ids": frozen_ids}
