"""Kafka -> Spark Structured Streaming -> Iceberg -> MinIO status pipeline."""
import json
from minio import Minio
from pyspark.sql.functions import col, from_json

from common.config import settings
from common.schema import EVENT_SCHEMA, EVENT_COLUMNS
from common.spark_factory import create_spark



def initialize_minio_bucket() -> None:
    """Ensure the configured MinIO bucket exists."""
    endpoint = settings.minio_endpoint.strip()

    if endpoint.startswith("http://"):
        endpoint = endpoint[len("http://"):]
    elif endpoint.startswith("https://"):
        endpoint = endpoint[len("https://"):]

    client = Minio(
        endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )

    if not client.bucket_exists(settings.minio_bucket):
        client.make_bucket(settings.minio_bucket)
        print(
            f"[MinIO] Created bucket: {settings.minio_bucket}",
            flush=True,
        )
    else:
        print(
            f"[MinIO] Bucket exists: {settings.minio_bucket}",
            flush=True,
        )



def ensure_iceberg_table(spark) -> None:
    """Ensure the Iceberg table exists, creating it if necessary. This is idempotent."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.usage_db")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {settings.iceberg_table} (
            event_id STRING,
            event_type STRING,
            user_id STRING,
            username STRING,
            service STRING,
            usage_type STRING,
            quantity INT,
            timestamp STRING
        ) USING iceberg
    """)
    print(f"[Iceberg] Table ready: {settings.iceberg_table}", flush=True)


def latest_snapshot_id(spark):
    """Return the latest snapshot ID for the Iceberg table, or None if no snapshots exist."""
    try:
        row = spark.sql(f"""
            SELECT snapshot_id
            FROM {settings.iceberg_table}.snapshots
            ORDER BY committed_at DESC
            LIMIT 1
        """).first()
        return str(row["snapshot_id"]) if row else None
    except Exception as exc:
        # Snapshot lookup is diagnostic metadata; don't hide a successful table write.
        print(f"[Iceberg] Snapshot lookup failed: {exc}", flush=True)
        return None


def list_iceberg_parquet_files(spark):
    """List Parquet files for the Iceberg table in MinIO. Returns (path, list of files)."""
    path_text = (
        f"s3a://{settings.minio_bucket}/"
        f"{settings.minio_table_prefix.strip('/')}/data/"
    )
    try:
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        path = spark._jvm.org.apache.hadoop.fs.Path(path_text)
        fs = path.getFileSystem(conf)
        if not fs.exists(path):
            return path_text, []
        files = []
        for status in fs.listStatus(path):
            if status.isFile() and str(status.getPath()).endswith(".parquet"):
                files.append(str(status.getPath()))
        return path_text, files
    except Exception as exc:
        print(f"[MinIO] Could not list {path_text}: {exc}", flush=True)
        return path_text, []


def publish_status(spark, event_ids, stage, status, **extra):
    """Publish one status record per event. Caller decides whether failure is fatal."""
    if not event_ids:
        return
    records = []
    for event_id in event_ids:
        payload = {
            "event_id": str(event_id),
            "stage": stage,
            "status": status,
            **extra,
        }
        records.append((str(event_id), json.dumps(payload, ensure_ascii=False)))
    status_df = spark.createDataFrame(records, ["key", "value"])
    (
        status_df.selectExpr(
            "CAST(key AS STRING) AS key",
            "CAST(value AS STRING) AS value",
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("topic", settings.kafka_status_topic)
        .save()
    )
    print(
        f"[Pipeline Status] stage={stage} status={status} events={len(event_ids)}",
        flush=True,
    )


def publish_status_best_effort(spark, event_ids, stage, status, **extra):
    """Publish status, but log and continue if it fails."""
    try:
        publish_status(spark, event_ids, stage, status, **extra)
    except Exception as exc:
        # A status-topic outage should not overwrite the original processing exception.
        print(f"[Pipeline Status] publish failed ({stage}/{status}): {exc}", flush=True)


def process_batch(spark, batch_df, batch_id):
    """Process a single micro-batch of events."""
    print(f"\n[Batch] batch_id={batch_id}", flush=True)
    if batch_df.isEmpty():
        print("[Batch] Empty batch", flush=True)
        return

    event_ids = [
        row["event_id"]
        for row in (
            batch_df.select("event_id")
            .where(col("event_id").isNotNull())
            .distinct()
            .collect()
        )
        if row["event_id"] is not None
    ]
    if not event_ids:
        print("[Batch] No valid event_id", flush=True)
        return

    publish_status_best_effort(
        spark, event_ids, "spark", "completed", batch_id=int(batch_id)
    )
    publish_status_best_effort(
        spark, event_ids, "iceberg", "processing", batch_id=int(batch_id)
    )

    try:
        (
            batch_df.select(*EVENT_COLUMNS)
            .writeTo(settings.iceberg_table)
            .append()
        )
    except Exception as exc:
        publish_status_best_effort(
            spark, event_ids, "iceberg", "failed",
            batch_id=int(batch_id), error=str(exc),
        )
        publish_status_best_effort(
            spark, event_ids, "spark", "failed",
            batch_id=int(batch_id), error=str(exc),
        )
        raise

    snapshot_id = latest_snapshot_id(spark)
    publish_status_best_effort(
        spark, event_ids, "iceberg", "completed",
        batch_id=int(batch_id),
        table=settings.iceberg_table,
        snapshot_id=snapshot_id,
    )

    publish_status_best_effort(
        spark, event_ids, "minio", "processing", batch_id=int(batch_id)
    )
    path, parquet_files = list_iceberg_parquet_files(spark)
    if not parquet_files:
        message = f"No Parquet files found at {path}"
        publish_status_best_effort(
            spark, event_ids, "minio", "failed",
            batch_id=int(batch_id), error=message, path=path,
        )
        # The Iceberg write has already committed. Raising here retries the batch;
        # foreachBatch therefore requires idempotency planning for production workloads.
        raise RuntimeError(message)

    publish_status_best_effort(
        spark, event_ids, "minio", "completed",
        batch_id=int(batch_id),
        bucket=settings.minio_bucket,
        path=path,
        parquet_files=len(parquet_files),
    )
    print(f"[Batch] Pipeline completed: batch_id={batch_id}", flush=True)


def main():
    """Run the Spark Structured Streaming pipeline."""
    initialize_minio_bucket()
    spark = create_spark("Phase3-12-Full-Pipeline")
    spark.sparkContext.setLogLevel("WARN")
    query = None
    try:
        # Ensure the Iceberg table exists
        ensure_iceberg_table(spark)

        # Read from Kafka
        kafka_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
            .option("subscribe", settings.kafka_source_topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
        # Parse the JSON payload and filter out invalid events
        parsed_df = (
            kafka_df.selectExpr("CAST(value AS STRING) AS json")
            .select(from_json(col("json"), EVENT_SCHEMA).alias("data"))
            .select("data.*")
            .filter(col("event_id").isNotNull())
        )

        print("[Streaming] Starting query", flush=True)
        # Start the streaming query with foreachBatch to process each micro-batch
        query = (
            parsed_df.writeStream
            .foreachBatch(lambda df, batch_id: process_batch(spark, df, batch_id))
            .option("checkpointLocation", settings.checkpoint_location)
            .trigger(processingTime=settings.processing_interval)
            .start()
        )
        # Wait for the streaming query to finish (or be interrupted)
        query.awaitTermination()
    except KeyboardInterrupt:
        print("[Streaming] Stop requested", flush=True)
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()
        print("[Spark] Stopped", flush=True)


if __name__ == "__main__":
    main()
