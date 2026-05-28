from tests.helpers import run_invoice_freeze


def test_tc10_invoice_run_freezes_state_sets_reference_second_run_empty() -> None:
    # Arrange
    rows = [
        {
            "position_id": "pos-monthly-001",
            "billing_period_label": "2026-04",
            "provider_id": 7,
            "user_id": 501,
            "amount": 1990,
            "amount_unit": 100,
        }
    ]

    # Act
    first_run = run_invoice_freeze(rows=rows, already_frozen_position_ids=set())
    second_run = run_invoice_freeze(rows=rows, already_frozen_position_ids=first_run["frozen_ids"])

    # Assert
    assert len(first_run["inserted"]) == 1
    assert first_run["inserted"][0]["state"] == "frozen"
    assert first_run["inserted"][0]["invoice_reference"].startswith("INV-2026-04-")
    assert second_run["inserted"] == []
