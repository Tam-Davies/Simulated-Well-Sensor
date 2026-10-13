import json
import logging

from kafka import KafkaConsumer

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
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def main():
    consumer = create_consumer()

    logger.info("Starting Kafka consumer...")
    logger.info("Kafka broker: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Topic: %s", KAFKA_TOPIC)
    logger.info("Consumer group: %s", CONSUMER_GROUP)

    try:
        for message in consumer:
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

    except KeyboardInterrupt:
        logger.info("Stopping Kafka consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()