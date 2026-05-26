import logging

from confluent_kafka import Consumer, KafkaError

from ledger_listener.configuration import Settings
from ledger_listener.events.models import KafkaMetadata
from ledger_listener.processor.worker import EventWorker

logger = logging.getLogger(__name__)


class CDCConsumer:
    def __init__(
        self,
        settings: Settings,
        worker: EventWorker,
        topics: tuple[str, ...] | None = None,
    ):
        self._settings = settings
        self._worker = worker
        self._topics = topics or settings.kafka_cdc_topics
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.kafka_group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": settings.kafka_auto_offset_reset,
            }
        )

    def run_forever(self) -> None:
        if not self._settings.kafka_allow_wildcard_topics:
            invalid = [t for t in self._topics if "*" in t]
            if invalid:
                raise ValueError(f"Wildcard topics are disabled but configured: {invalid}")

        self._consumer.subscribe(list(self._topics))
        logger.info("Subscribed to CDC topics: %s", ", ".join(self._topics))

        try:
            while True:
                message = self._consumer.poll(self._settings.listener_poll_timeout_seconds)
                if message is None:
                    continue
                if message.error():
                    # Partition EOF is informational for low-volume topics.
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Kafka consumer error: %s", message.error())
                    continue

                metadata = KafkaMetadata(
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                )

                try:
                    result = self._worker.process_cdc_payload(message.value(), metadata)
                    if result.should_commit:
                        # Explicit checkpoint after successfully handling this message.
                        self._consumer.commit(message=message, asynchronous=False)
                    if result.duplicate:
                        logger.info(
                            "Duplicate message observed and committed. "
                            "topic=%s partition=%s offset=%s",
                            metadata.topic,
                            metadata.partition,
                            metadata.offset,
                        )
                except Exception:
                    logger.exception(
                        "Failed to process CDC message (left uncommitted). "
                        "topic=%s partition=%s offset=%s",
                        metadata.topic,
                        metadata.partition,
                        metadata.offset,
                    )
        finally:
            self._consumer.close()
