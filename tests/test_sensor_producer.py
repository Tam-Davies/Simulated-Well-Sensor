from datetime import datetime

from producer.sensor_producer import generate_sensor_reading


def test_generate_sensor_reading_returns_expected_fields():
    reading = generate_sensor_reading()

    expected_fields = {
        "well_id",
        "timestamp",
        "pressure_psi",
        "temperature_c",
        "flow_rate_bpd",
        "vibration_mms",
    }

    assert set(reading.keys()) == expected_fields


def test_generate_sensor_reading_has_correct_well_id():
    reading = generate_sensor_reading()

    assert reading["well_id"] == "WELL-001"


def test_generate_sensor_reading_values_are_numeric():
    reading = generate_sensor_reading()

    assert isinstance(reading["pressure_psi"], float)
    assert isinstance(reading["temperature_c"], float)
    assert isinstance(reading["flow_rate_bpd"], float)
    assert isinstance(reading["vibration_mms"], float)


def test_generate_sensor_reading_values_are_within_expected_ranges():
    reading = generate_sensor_reading()

    assert 2800 <= reading["pressure_psi"] <= 2900
    assert 75 <= reading["temperature_c"] <= 82
    assert 1150 <= reading["flow_rate_bpd"] <= 1350
    assert 0.2 <= reading["vibration_mms"] <= 0.8


def test_generate_sensor_reading_timestamp_is_valid():
    reading = generate_sensor_reading()

    timestamp = datetime.fromisoformat(reading["timestamp"])

    assert timestamp.tzinfo is not None