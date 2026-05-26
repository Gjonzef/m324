import logging
import threading

from ledger_listener.clickhouse.client import ClickHouseLedgerClient
from ledger_listener.configuration import Settings
from ledger_listener.kafka.consumer import CDCConsumer
from ledger_listener.mariadb.client import MariaDbReadClient
from ledger_listener.processor.worker import EventWorker

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
logger = logging.getLogger(__name__)


def _run_topic_consumer(settings: Settings, topic: str) -> None:
    clickhouse = ClickHouseLedgerClient(settings)
    mariadb = MariaDbReadClient(settings=settings)
    worker = EventWorker(settings=settings, clickhouse=clickhouse, mariadb=mariadb)
    consumer = CDCConsumer(settings=settings, worker=worker, topics=(topic,))
    logger.info("Starting consumer thread for topic=%s", topic)
    consumer.run_forever()


def run() -> None:
    settings = Settings.from_env()
    logging.basicConfig(level=settings.log_level, format=LOG_FORMAT)

    clickhouse = ClickHouseLedgerClient(settings)
    clickhouse.ensure_schema()

    topics = tuple(dict.fromkeys(settings.kafka_cdc_topics))
    if not topics:
        raise ValueError("KAFKA_CDC_TOPICS is empty")

    if len(topics) == 1:
        logger.info("Single topic configured (%s), starting one consumer.", topics[0])
        _run_topic_consumer(settings, topics[0])
        return

    threads: list[threading.Thread] = []
    for topic in topics:
        thread = threading.Thread(
            target=_run_topic_consumer,
            args=(settings, topic),
            name=f"consumer-{topic}",
            daemon=False,
        )
        thread.start()
        threads.append(thread)

    logger.info("Started %s consumer threads.", len(threads))
    for thread in threads:
        thread.join()
