from dataclasses import dataclass
import os


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    kafka_cdc_topics: tuple[str, ...]
    kafka_group_id: str
    kafka_auto_offset_reset: str
    kafka_allow_wildcard_topics: bool
    listener_poll_timeout_seconds: float
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_username: str
    clickhouse_password: str
    clickhouse_database: str
    clickhouse_secure: bool
    clickhouse_recreate_ledger_table: bool
    debezium_connector_name: str
    source_database_name: str
    subscription_table_name: str
    monthly_billing_topic: str
    mariadb_host: str
    mariadb_port: int
    mariadb_database: str
    mariadb_user: str
    mariadb_password: str
    mariadb_connect_timeout_seconds: int
    mariadb_charset: str
    mariadb_collation: str
    default_currency: str
    log_level: str

    @classmethod
    def from_env(cls) -> "Settings":
        topics_raw = os.getenv("KAFKA_CDC_TOPICS", os.getenv("KAFKA_CDC_TOPIC", "billing.cdc.subscription"))
        topics = tuple(t.strip() for t in topics_raw.split(",") if t.strip())
        if not topics:
            topics = ("billing.cdc.subscription",)
        return cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_cdc_topics=topics,
            kafka_group_id=os.getenv("KAFKA_GROUP_ID", "ledger-listener-cdc"),
            kafka_auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            kafka_allow_wildcard_topics=_as_bool(os.getenv("KAFKA_ALLOW_WILDCARD_TOPICS"), default=False),
            listener_poll_timeout_seconds=float(os.getenv("LISTENER_POLL_TIMEOUT_SECONDS", "1.0")),
            clickhouse_host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            clickhouse_port=int(os.getenv("CLICKHOUSE_PORT", "18123")),
            clickhouse_username=os.getenv("CLICKHOUSE_USERNAME", "default"),
            clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", "ledger_dev"),
            clickhouse_database=os.getenv("CLICKHOUSE_DATABASE", "billing"),
            clickhouse_secure=_as_bool(os.getenv("CLICKHOUSE_SECURE"), default=False),
            clickhouse_recreate_ledger_table=_as_bool(os.getenv("CLICKHOUSE_RECREATE_LEDGER_TABLE"), default=False),
            debezium_connector_name=os.getenv("DEBEZIUM_CONNECTOR_NAME", "telephony_dev"),
            source_database_name=os.getenv("SOURCE_DATABASE_NAME", "telephony_dev"),
            subscription_table_name=os.getenv("SUBSCRIPTION_TABLE_NAME", "subscription"),
            monthly_billing_topic=os.getenv("MONTHLY_BILLING_TOPIC", "monthly_billing"),
            mariadb_host=os.getenv("MARIADB_HOST", "localhost"),
            mariadb_port=int(os.getenv("MARIADB_PORT", "3306")),
            mariadb_database=os.getenv("MARIADB_DATABASE", "telephony_dev"),
            mariadb_user=os.getenv("MARIADB_USER", "ledger"),
            # No safe default for production credentials.
            mariadb_password=os.getenv("MARIADB_PASSWORD", ""),
            mariadb_connect_timeout_seconds=int(os.getenv("MARIADB_CONNECT_TIMEOUT_SECONDS", "5")),
            mariadb_charset=os.getenv("MARIADB_CHARSET", "utf8mb4"),
            mariadb_collation=os.getenv("MARIADB_COLLATION", "utf8mb4_general_ci"),
            default_currency=os.getenv("DEFAULT_CURRENCY", "CHF"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )
