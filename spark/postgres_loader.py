import logging


# PostgreSQL configuration
POSTGRES_URL = "jdbc:postgresql://localhost:5432/well_sensor"
POSTGRES_TABLE = "well_telemetry"

POSTGRES_PROPERTIES = {
    "user": "well_sensor",
    "password": "well_sensor",
    "driver": "org.postgresql.Driver",
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def write_to_postgres(batch_df, batch_id):
    """Write one Spark micro-batch to PostgreSQL."""

    if batch_df.isEmpty():
        logger.info(
            "PostgreSQL batch %s contains no records.",
            batch_id,
        )
        return

    row_count = batch_df.count()

    logger.info(
        "Writing PostgreSQL batch %s | rows=%s",
        batch_id,
        row_count,
    )

    (
        batch_df.write
        .jdbc(
            url=POSTGRES_URL,
            table=POSTGRES_TABLE,
            mode="append",
            properties=POSTGRES_PROPERTIES,
        )
    )

    logger.info(
        "PostgreSQL batch %s written successfully.",
        batch_id,
    )