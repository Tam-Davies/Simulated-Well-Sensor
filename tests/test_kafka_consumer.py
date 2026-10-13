from consumer.kafka_consumer import create_consumer


def test_create_consumer_configuration():
    consumer = create_consumer()

    try:
        assert consumer.config["bootstrap_servers"] == "localhost:9092"
        assert consumer.config["group_id"] == "well-sensor-consumer"
        assert consumer.config["auto_offset_reset"] == "earliest"
        assert consumer.config["enable_auto_commit"] is True
    finally:
        consumer.close()