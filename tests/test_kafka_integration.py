import json
import time

from kafka import KafkaConsumer, KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "well-telemetry"
TEST_GROUP = "well-sensor-integration-test"


def test_kafka_producer_consumer_integration():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=TEST_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    test_reading = {
        "well_id": "TEST-WELL",
        "timestamp": "2026-10-13T06:00:00+00:00",
        "pressure_psi": 2850.0,
        "temperature_c": 78.0,
        "flow_rate_bpd": 1250.0,
        "vibration_mms": 0.5,
    }

    try:
        producer.send(
            KAFKA_TOPIC,
            key="TEST-WELL",
            value=test_reading,
        )
        producer.flush()

        deadline = time.time() + 10

        while time.time() < deadline:
            records = consumer.poll(timeout_ms=1000)

            for messages in records.values():
                for message in messages:
                    if message.value == test_reading:
                        assert message.key == "TEST-WELL"
                        return

        raise AssertionError("Test message was not received from Kafka.")

    finally:
        producer.close()
        consumer.close()