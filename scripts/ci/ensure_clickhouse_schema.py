from ledger_listener.clickhouse.client import ClickHouseLedgerClient
from ledger_listener.configuration import Settings


def main() -> int:
    settings = Settings.from_env()
    client = ClickHouseLedgerClient(settings)
    client.ensure_schema()
    print("ClickHouse schema is ready")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
