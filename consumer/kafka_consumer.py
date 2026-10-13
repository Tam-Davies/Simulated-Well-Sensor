import json
import logging

from kafka import KafkaConsumer
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "well-telemetry"
CONSUMER_GROUP = "well-sensor-consumer"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def create_consumer():
    """Create and return a Kafka consumer."""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            key_deserializer=lambda key: key.decode("utf-8") if key else None,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )

        logger.info(
            "Kafka consumer connected | broker=%s | topic=%s | group=%s",
            KAFKA_BOOTSTRAP_SERVERS,
            KAFKA_TOPIC,
            CONSUMER_GROUP,
        )

        return consumer

    except KafkaError:
        logger.exception("Failed to create Kafka consumer.")
        raise


def main():
    consumer = None

    try:
        consumer = create_consumer()

        logger.info("Starting Kafka consumer...")

        for message in consumer:
            try:
                telemetry = message.value

                logger.info(
                    "Received telemetry | "
                    "partition=%s | offset=%s | well_id=%s | "
                    "pressure=%.2f psi | temperature=%.2f °C | "
                    "flow=%.2f bpd | vibration=%.3f mm/s",
                    message.partition,
                    message.offset,
                    telemetry["well_id"],
                    telemetry["pressure_psi"],
                    telemetry["temperature_c"],
                    telemetry["flow_rate_bpd"],
                    telemetry["vibration_mms"],
                )

            except (KeyError, TypeError, ValueError):
                logger.exception(
                    "Invalid telemetry message | partition=%s | offset=%s",
                    message.partition,
                    message.offset,
                )

    except KeyboardInterrupt:
        logger.info("Stopping Kafka consumer...")

    except KafkaError:
        logger.exception("Kafka consumer error.")

    finally:
        if consumer is not None:
            consumer.close()
            logger.info("Kafka consumer closed.")


if __name__ == "__main__":
    main()