import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)
from kafka import KafkaProducer


KAFKA_BOOTSTRAP = "kafka:9092"
STATUS_TOPIC = "pipeline-status"
ICEBERG_TABLE = "local.usage_db.usage_events"


schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("username", StringType()),
    StructField("service", StringType()),
    StructField("usage_type", StringType()),
    StructField("quantity", IntegerType()),
    StructField("timestamp", StringType()),
])


spark = (
    SparkSession.builder
    .appName("Phase3-11-Kafka-Iceberg")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(
        "spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config(
        "spark.sql.catalog.local.type",
        "hadoop",
    )
    .config(
        "spark.sql.catalog.local.warehouse",
        "s3a://warehouse/",
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )
    .getOrCreate()
)

spark.sql("""
CREATE NAMESPACE IF NOT EXISTS local.usage_db
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS local.usage_db.usage_events (
    event_id STRING,
    event_type STRING,
    user_id STRING,
    username STRING,
    service STRING,
    usage_type STRING,
    quantity INT,
    timestamp STRING
)
USING iceberg
""")


def send_status(producer, event_id, stage, status, **kwargs):
    message = {
        "event_id": event_id,
        "stage": stage,
        "status": status,
        **kwargs,
    }

    producer.send(
        STATUS_TOPIC,
        key=event_id,
        value=message,
    )

    producer.flush()


status_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value:
        json.dumps(value).encode("utf-8"),
)


def process_batch(batch_df, batch_id):

    if batch_df.rdd.isEmpty():
        return

    event_ids = [
        row.event_id
        for row in batch_df.select("event_id").collect()
        if row.event_id
    ]

    # Spark processing completed.
    for event_id in event_ids:
        send_status(
            status_producer,
            event_id,
            "spark",
            "completed",
            batch_id=batch_id,
        )

    (
        batch_df
        .select(
            "event_id",
            "event_type",
            "user_id",
            "username",
            "service",
            "usage_type",
            "quantity",
            "timestamp",
        )
        .writeTo(ICEBERG_TABLE)
        .append()
    )

    # Iceberg write completed.
    for event_id in event_ids:

        send_status(
            status_producer,
            event_id,
            "iceberg",
            "completed",
            table=ICEBERG_TABLE,
        )

        # The Iceberg table is physically stored below
        # this MinIO warehouse location.
        send_status(
            status_producer,
            event_id,
            "minio",
            "completed",
            path=(
                "s3a://warehouse/"
                "usage_db/usage_events/"
                "data/"
            ),
        )


stream_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP,
    )
    .option(
        "subscribe",
        "usage-events",
    )
    .option(
        "startingOffsets",
        "earliest",
    )
    .load()
)

parsed_df = (
    stream_df
    .selectExpr(
        "CAST(value AS STRING) AS json"
    )
    .select(
        from_json(
            col("json"),
            schema,
        ).alias("data")
    )
    .select("data.*")
)


query = (
    parsed_df
    .writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "s3a://warehouse/checkpoints/usage_events",
    )
    .start()
)

query.awaitTermination()
