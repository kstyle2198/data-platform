import json

from kafka import KafkaProducer

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)


KAFKA_BOOTSTRAP = "kafka:9092"
STATUS_TOPIC = "pipeline-status"
USAGE_TOPIC = "usage-events"
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
    .appName("Phase3-12-Full-Pipeline")
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
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
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


status_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value:
        json.dumps(value).encode("utf-8"),
)


def status(event_id, stage, state, **extra):

    message = {
        "event_id": event_id,
        "stage": stage,
        "status": state,
        **extra,
    }

    print(
        "[Pipeline Status]",
        message,
    )

    status_producer.send(
        STATUS_TOPIC,
        key=event_id,
        value=message,
    )

    status_producer.flush()


def process_batch(batch_df, batch_id):

    if batch_df.rdd.isEmpty():
        return

    rows = (
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
        .collect()
    )

    # Spark stage
    for row in rows:
        status(
            row.event_id,
            "spark",
            "completed",
            batch_id=batch_id,
        )

    # Iceberg stage
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

    for row in rows:

        status(
            row.event_id,
            "iceberg",
            "completed",
            table=ICEBERG_TABLE,
        )

        status(
            row.event_id,
            "minio",
            "completed",
            path=(
                "s3a://warehouse/"
                "usage_db/usage_events/data/"
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
        USAGE_TOPIC,
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
        "s3a://warehouse/checkpoints/full_usage_pipeline",
    )
    .start()
)


print("=" * 70)
print("FULL PIPELINE STARTED")
print("Kafka -> Spark -> Iceberg -> MinIO")
print("=" * 70)

query.awaitTermination()
