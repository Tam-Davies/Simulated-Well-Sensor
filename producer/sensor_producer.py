import json
import logging
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "well-telemetry"
WELL_ID = "WELL-001"
SEND_INTERVAL_SECONDS = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def generate_sensor_reading():
    """Generate one simulated well sensor reading."""
    return {
        "well_id": WELL_ID,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pressure_psi": round(random.uniform(2800, 2900), 2),
        "temperature_c": round(random.uniform(75, 82), 2),
        "flow_rate_bpd": round(random.uniform(1150, 1350), 2),
        "vibration_mms": round(random.uniform(0.2, 0.8), 3),
    }


def create_producer():
    """Create and return a Kafka producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            key_serializer=lambda key: key.encode("utf-8"),
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )

        logger.info(
            "Kafka producer connected | broker=%s",
            KAFKA_BOOTSTRAP_SERVERS,
        )

        return producer

    except KafkaError:
        logger.exception("Failed to create Kafka producer.")
        raise


def main():
    try:
        producer = create_producer()

    except KafkaError:
        logger.error("Unable to start sensor producer.")
        return

    logger.info("Starting well sensor producer...")
    logger.info("Topic: %s", KAFKA_TOPIC)
    logger.info("Well ID: %s", WELL_ID)

    try:
        while True:
            reading = generate_sensor_reading()

            future = producer.send(
                KAFKA_TOPIC,
                key=WELL_ID,
                value=reading,
            )

            try:
                metadata = future.get(timeout=10)

                logger.info(
                    "Sent telemetry | partition=%s | offset=%s | "
                    "pressure=%.2f psi | temperature=%.2f °C | "
                    "flow=%.2f bpd | vibration=%.3f mm/s",
                    metadata.partition,
                    metadata.offset,
                    reading["pressure_psi"],
                    reading["temperature_c"],
                    reading["flow_rate_bpd"],
                    reading["vibration_mms"],
                )

            except KafkaError:
                logger.exception("Failed to send telemetry to Kafka.")

            time.sleep(SEND_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Stopping sensor producer...")

    finally:
        producer.close()
        logger.info("Kafka producer closed.")


if __name__ == "__main__":
    main()