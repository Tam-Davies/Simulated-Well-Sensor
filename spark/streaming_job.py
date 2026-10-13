from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "well-telemetry"

CHECKPOINT_LOCATION = "./checkpoints/windowed_analytics"


def create_spark_session():
    """Create and configure the Spark session."""
    return (
        SparkSession.builder
        .appName("SimulatedWellSensor")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    telemetry_schema = StructType(
        [
            StructField("well_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("pressure_psi", DoubleType(), True),
            StructField("temperature_c", DoubleType(), True),
            StructField("flow_rate_bpd", DoubleType(), True),
            StructField("vibration_mms", DoubleType(), True),
        ]
    )

    # Read telemetry continuously from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Convert Kafka value from binary to string
    json_stream = raw_stream.select(
        col("value").cast("string").alias("json_value")
    )

    # Parse JSON telemetry
    telemetry_stream = (
        json_stream
        .select(
            from_json(
                col("json_value"),
                telemetry_schema,
            ).alias("telemetry")
        )
        .select("telemetry.*")
    )

    # Aggregate telemetry in 10-second windows for each well
    windowed_stream = (
        telemetry_stream
        .withWatermark("timestamp", "10 seconds")
        .groupBy(
            window(
                col("timestamp"),
                "10 seconds",
            ),
            col("well_id"),
        )
        .agg(
            avg("pressure_psi").alias("avg_pressure_psi"),
            avg("temperature_c").alias("avg_temperature_c"),
            avg("flow_rate_bpd").alias("avg_flow_rate_bpd"),
            avg("vibration_mms").alias("avg_vibration_mms"),
            count("*").alias("reading_count"),
        )
        .select(
            col("well_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg_pressure_psi"),
            col("avg_temperature_c"),
            col("avg_flow_rate_bpd"),
            col("avg_vibration_mms"),
            col("reading_count"),
        )
    )

    # Output aggregated windows to the console
    query = (
        windowed_stream.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 20)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()