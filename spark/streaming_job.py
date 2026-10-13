import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json, window
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from postgres_loader import write_to_postgres


# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "well-telemetry"


# Spark checkpoint locations
POSTGRES_CHECKPOINT = "./checkpoints/postgres_telemetry"
WINDOW_CHECKPOINT = "./checkpoints/windowed_analytics_v2"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def create_spark_session():
    """Create and return the Spark session."""

    return (
        SparkSession.builder
        .appName("SimulatedWellSensor")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    spark = None
    postgres_query = None
    window_query = None

    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        logger.info("Starting Spark streaming job...")
        logger.info(
            "Kafka broker: %s",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        logger.info(
            "Kafka topic: %s",
            KAFKA_TOPIC,
        )

        # ---------------------------------------------------------
        # Telemetry schema
        # ---------------------------------------------------------

        telemetry_schema = StructType(
            [
                StructField(
                    "well_id",
                    StringType(),
                    True,
                ),
                StructField(
                    "timestamp",
                    TimestampType(),
                    True,
                ),
                StructField(
                    "pressure_psi",
                    DoubleType(),
                    True,
                ),
                StructField(
                    "temperature_c",
                    DoubleType(),
                    True,
                ),
                StructField(
                    "flow_rate_bpd",
                    DoubleType(),
                    True,
                ),
                StructField(
                    "vibration_mms",
                    DoubleType(),
                    True,
                ),
            ]
        )

        # ---------------------------------------------------------
        # Kafka → Spark
        # ---------------------------------------------------------

        raw_stream = (
            spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                KAFKA_BOOTSTRAP_SERVERS,
            )
            .option(
                "subscribe",
                KAFKA_TOPIC,
            )
            .option(
                "startingOffsets",
                "latest",
            )
            .load()
        )

        # ---------------------------------------------------------
        # Kafka JSON → Spark columns
        # ---------------------------------------------------------

        json_stream = raw_stream.select(
            col("value")
            .cast("string")
            .alias("json_value")
        )

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

        # ---------------------------------------------------------
        # Raw telemetry → PostgreSQL
        # ---------------------------------------------------------

        postgres_query = (
            telemetry_stream.writeStream
            .foreachBatch(
                write_to_postgres
            )
            .option(
                "checkpointLocation",
                POSTGRES_CHECKPOINT,
            )
            .start()
        )

        logger.info(
            "PostgreSQL streaming query started."
        )

        # ---------------------------------------------------------
        # Windowed analytics
        # ---------------------------------------------------------

        windowed_stream = (
            telemetry_stream
            .withWatermark(
                "timestamp",
                "10 seconds",
            )
            .groupBy(
                window(
                    col("timestamp"),
                    "10 seconds",
                ),
                col("well_id"),
            )
            .agg(
                avg(
                    "pressure_psi"
                ).alias(
                    "avg_pressure_psi"
                ),
                avg(
                    "temperature_c"
                ).alias(
                    "avg_temperature_c"
                ),
                avg(
                    "flow_rate_bpd"
                ).alias(
                    "avg_flow_rate_bpd"
                ),
                avg(
                    "vibration_mms"
                ).alias(
                    "avg_vibration_mms"
                ),
                count("*").alias(
                    "reading_count"
                ),
            )
            .select(
                col("well_id"),
                col("window.start").alias(
                    "window_start"
                ),
                col("window.end").alias(
                    "window_end"
                ),
                col("avg_pressure_psi"),
                col("avg_temperature_c"),
                col("avg_flow_rate_bpd"),
                col("avg_vibration_mms"),
                col("reading_count"),
            )
        )

        # ---------------------------------------------------------
        # Windowed analytics → Console
        # ---------------------------------------------------------

        window_query = (
            windowed_stream.writeStream
            .format("console")
            .outputMode("append")
            .option(
                "truncate",
                "false",
            )
            .option(
                "numRows",
                10,
            )
            .option(
                "checkpointLocation",
                WINDOW_CHECKPOINT,
            )
            .start()
        )

        logger.info(
            "Windowed analytics query started."
        )

        # ---------------------------------------------------------
        # Keep Spark running
        # ---------------------------------------------------------

        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        logger.info(
            "Stopping Spark streaming job..."
        )

    except Exception:
        logger.exception(
            "Spark streaming job failed."
        )
        raise

    finally:

        if postgres_query is not None:
            if postgres_query.isActive:
                postgres_query.stop()

                logger.info(
                    "PostgreSQL streaming query stopped."
                )

        if window_query is not None:
            if window_query.isActive:
                window_query.stop()

                logger.info(
                    "Windowed analytics query stopped."
                )

        if spark is not None:
            spark.stop()

            logger.info(
                "Spark session stopped."
            )


if __name__ == "__main__":
    main()