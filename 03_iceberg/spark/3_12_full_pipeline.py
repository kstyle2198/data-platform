import json
from minio import Minio

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lit,
    struct,
    to_json,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)
# ============================================================
# MinIO Bucket Initialization
# ============================================================

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET_NAME = "warehouse"


client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)


if client.bucket_exists(BUCKET_NAME):
    print(f"[MinIO] Bucket already exists: {BUCKET_NAME}")
else:
    client.make_bucket(BUCKET_NAME)
    print(f"[MinIO] Bucket created: {BUCKET_NAME}")


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_SOURCE_TOPIC = "usage-events"
KAFKA_STATUS_TOPIC = "pipeline-status"

ICEBERG_TABLE = ("local.usage_db.usage_events")
ICEBERG_WAREHOUSE = ("s3a://warehouse/")

CHECKPOINT_LOCATION = (
    "s3a://warehouse/"
    "checkpoints/"
    "full_usage_pipeline"
)

MINIO_ENDPOINT = ("http://minio:9000")
MINIO_ACCESS_KEY = ("minioadmin")
MINIO_SECRET_KEY = ("minioadmin123")
MINIO_BUCKET = ("warehouse")
MINIO_TABLE_PREFIX = ("usage_db/usage_events")

# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder

    .appName("Phase3-12-Full-Pipeline")

    # --------------------------------------------------------
    # Iceberg
    # --------------------------------------------------------
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",)
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog",) #local이라는 Catalog를 Iceberg Catalog로 사용
    .config("spark.sql.catalog.local.type","hadoop",)
    .config("spark.sql.catalog.local.warehouse", ICEBERG_WAREHOUSE,) # Iceberg의 Warehouse를 MinIO의 warehouse Bucket으로 지정

    # --------------------------------------------------------
    # S3A / MinIO
    # --------------------------------------------------------
    .config("spark.hadoop.fs.s3a.endpoint",MINIO_ENDPOINT,)
    .config("spark.hadoop.fs.s3a.access.key",MINIO_ACCESS_KEY,)
    .config("spark.hadoop.fs.s3a.secret.key",MINIO_SECRET_KEY,)
    .config("spark.hadoop.fs.s3a.path.style.access","true",)
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false",)
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem",)

    # --------------------------------------------------------
    # S3A connection
    # --------------------------------------------------------
    .config("spark.hadoop.fs.s3a.connection.maximum","50",)
    .config("spark.hadoop.fs.s3a.attempts.maximum","10",)
    .config("spark.hadoop.fs.s3a.connection.timeout","10000",)
    .config("spark.hadoop.fs.s3a.connection.establish.timeout","10000",)

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("==================================================")
print("Phase 3-12 Full Pipeline")
print("Kafka -> Spark -> Iceberg -> MinIO -> UI")
print("==================================================")


# ============================================================
# 1. Create Iceberg Namespace
# ============================================================

print("[1] Creating Iceberg namespace...")

spark.sql(
    """
    CREATE NAMESPACE IF NOT EXISTS
    local.usage_db
    """
)

# ============================================================
# 2. Create Iceberg Table
# ============================================================

print("[2] Creating Iceberg table...")

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS
    local.usage_db.usage_events
    (
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
    """
)

print("[Iceberg] Table ready:")
print(f"  {ICEBERG_TABLE}")

# ============================================================
# 3. Kafka Event Schema
# ============================================================

event_schema = StructType(
    [
        StructField("event_id",StringType(),True,),
        StructField("event_type",StringType(),True,),
        StructField("user_id",StringType(),True,),
        StructField("username",StringType(),True,),
        StructField("service",StringType(),True,),
        StructField("usage_type",StringType(),True,),
        StructField("quantity",IntegerType(),True,),
        StructField("timestamp",StringType(),True,),
    ]
)

# ============================================================
# 4. Read Kafka
# ============================================================

print("[3] Starting Kafka Structured Streaming...")

kafka_df = (

    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP_SERVERS,)
    .option("subscribe",KAFKA_SOURCE_TOPIC,)

    # --------------------------------------------------------
    # 최초 실행에서는 기존 이벤트도 처리
    # 이후에는 checkpoint 기준으로 이어서 처리
    # --------------------------------------------------------
    .option("startingOffsets","earliest",)
    .option("failOnDataLoss","false",)
    .load()
)


# ============================================================
# 5. Kafka value -> JSON
# ============================================================

json_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json")
)

# ============================================================
# 6. JSON -> Structured DataFrame
# ============================================================

parsed_df = (
    json_df
    .select(from_json(col("json"),event_schema,).alias("data"))
    .select("data.*")
)


# ============================================================
# 7. Validate event_id
# ============================================================

parsed_df = (
    parsed_df
    .filter(col("event_id").isNotNull())
)


# ============================================================
# 8. Get latest Iceberg snapshot
# ============================================================

def get_latest_snapshot_id():

    try:

        snapshot_df = spark.sql(
            f"""
            SELECT
                snapshot_id,
                committed_at
            FROM
                {ICEBERG_TABLE}.snapshots
            ORDER BY
                committed_at DESC
            LIMIT 1
            """
        )

        rows = snapshot_df.collect()

        if not rows:
            return None

        return str(
            rows[0]["snapshot_id"]
        )

    except Exception as exc:

        print(
            "[Iceberg] "
            "Unable to read snapshot: "
            f"{exc}"
        )

        return None


# ============================================================
# 9. Check MinIO Iceberg data files
# ============================================================

def get_minio_data_files():

    data_path = (
        "s3a://"
        f"{MINIO_BUCKET}/"
        f"{MINIO_TABLE_PREFIX}/data/"
    )

    print("[MinIO] Checking data path:")
    print(f"  {data_path}")

    try:

        hadoop_conf = (
            spark.sparkContext
            ._jsc
            .hadoopConfiguration()
        )

        path = (
            spark._jvm
            .org.apache.hadoop.fs.Path(
                data_path
            )
        )

        filesystem = (
            path.getFileSystem(
                hadoop_conf
            )
        )

        status_list = (
            filesystem.listStatus(
                path
            )
        )

        parquet_files = []

        for status in status_list:

            if status.isFile():

                file_path = str(
                    status.getPath()
                )

                if file_path.endswith(
                    ".parquet"
                ):

                    parquet_files.append(
                        file_path
                    )

        return parquet_files

    except Exception as exc:

        print(
            "[MinIO] Failed to list "
            f"data files: {exc}"
        )

        return []


# ============================================================
# 10. Publish pipeline-status to Kafka
# ============================================================

def publish_pipeline_status(
    event_ids,
    stage,
    status,
    **extra,
):

    if not event_ids:

        return

    print(
        "[Pipeline Status]"
        f" stage={stage}"
        f" status={status}"
        f" events={len(event_ids)}"
    )


    records = []

    for event_id in event_ids:

        record = {"event_id":str(event_id), "stage":stage, "status":status,}
        record.update(extra)
        records.append((str(event_id),json.dumps(record),))

    status_df = spark.createDataFrame(
        records,
        [
            "key",
            "value",
        ],
    )


    status_df = (

        status_df
        .selectExpr(
            "CAST(key AS STRING) AS key",
            "CAST(value AS STRING) AS value",
        )
    )


    (

        status_df
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP_SERVERS,)
        .option("topic",KAFKA_STATUS_TOPIC,)
        .save()
    )


# ============================================================
# 11. Process each micro-batch
# ============================================================

def process_batch(
    batch_df,
    batch_id,
):

    print("")
    print("==================================================")
    print(f"[Batch] batch_id={batch_id}")
    print("==================================================")


    # --------------------------------------------------------
    # Empty batch
    # --------------------------------------------------------

    if batch_df.isEmpty():
        print("[Batch] Empty batch")
        return


    # --------------------------------------------------------
    # Get event IDs
    # --------------------------------------------------------

    event_rows = (

        batch_df
        .select("event_id")
        .where(col("event_id").isNotNull())
        .distinct()
        .collect()
    )

    event_ids = [
        row["event_id"]
        for row in event_rows
        if row["event_id"] is not None
    ]


    if not event_ids:
        print("[Batch] No valid event_id")
        return

    print("[Batch] Event IDs:")

    for event_id in event_ids:
        print(f"  {event_id}")


    # --------------------------------------------------------
    # Spark stage
    # --------------------------------------------------------

    publish_pipeline_status(
        event_ids,
        stage="spark",
        status="completed",
        batch_id=int(batch_id),
    )

    # --------------------------------------------------------
    # Iceberg stage - processing
    # --------------------------------------------------------

    publish_pipeline_status(
        event_ids,
        stage="iceberg",
        status="processing",
        batch_id=int(batch_id),
    )


    # --------------------------------------------------------
    # Write to Iceberg
    # --------------------------------------------------------

    try:

        print("[Iceberg] Appending batch...")
        (
            batch_df
            .select("event_id","event_type","user_id","username","service","usage_type","quantity","timestamp",)
            .writeTo(ICEBERG_TABLE)
            .append()
        )
        print("[Iceberg] Append completed")


    except Exception as exc:

        print("[Iceberg] Append failed:")
        print(exc)

        publish_pipeline_status(
            event_ids,
            stage="iceberg",
            status="failed",
            batch_id=int(batch_id),
            error=str(exc),
        )


        # ----------------------------------------------------
        # Spark stage also marked failed
        # ----------------------------------------------------

        publish_pipeline_status(
            event_ids,
            stage="spark",
            status="failed",
            batch_id=int(batch_id),
            error=str(exc),
        )

        raise


    # --------------------------------------------------------
    # Get Iceberg snapshot
    # --------------------------------------------------------

    snapshot_id = (
        get_latest_snapshot_id()
    )

    print(
        "[Iceberg] snapshot_id="
        f"{snapshot_id}"
    )

    # --------------------------------------------------------
    # Iceberg completed
    # --------------------------------------------------------

    publish_pipeline_status(

        event_ids,
        stage="iceberg",
        status="completed",
        batch_id=int(batch_id),
        table=ICEBERG_TABLE,
        snapshot_id=snapshot_id,
    )


    # --------------------------------------------------------
    # MinIO stage - processing
    # --------------------------------------------------------

    publish_pipeline_status(

        event_ids,
        stage="minio",
        status="processing",
        batch_id=int(batch_id),
    )


    # --------------------------------------------------------
    # Check physical Parquet files
    # --------------------------------------------------------

    parquet_files = (
        get_minio_data_files()
    )


    if not parquet_files:

        error_message = (
            "No Parquet files found "
            "in MinIO Iceberg data path"
        )


        print(
            "[MinIO] "
            f"{error_message}"
        )


        publish_pipeline_status(

            event_ids,
            stage="minio",
            status="failed",
            batch_id=int(batch_id),
            error=error_message,
            path=(
                "s3a://warehouse/"
                "usage_db/usage_events/"
                "data/"
            ),
        )

        raise RuntimeError(
            error_message
        )


    # --------------------------------------------------------
    # MinIO completed
    # --------------------------------------------------------

    minio_data_path = (
        "s3a://warehouse/"
        "usage_db/usage_events/"
        "data/"
    )


    print("[MinIO] Parquet files:")

    for file_path in parquet_files:

        print(
            f"  {file_path}"
        )


    publish_pipeline_status(

        event_ids,
        stage="minio",
        status="completed",
        batch_id=int(batch_id),
        bucket=MINIO_BUCKET,
        path=minio_data_path,
        parquet_files=len(parquet_files),
    )


    print("[Batch] Pipeline completed")
    print("==================================================")

# ============================================================
# 12. Start Streaming Query
# ============================================================

print("[4] Starting Structured Streaming...")

query = (

    parsed_df
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation",CHECKPOINT_LOCATION,)
    .trigger(processingTime="5 seconds")
    .start()
)


print("==================================================")
print("Streaming pipeline started")
print(
    f"Source topic      : "
    f"{KAFKA_SOURCE_TOPIC}"
)
print(
    f"Status topic      : "
    f"{KAFKA_STATUS_TOPIC}"
)
print(
    f"Iceberg table     : "
    f"{ICEBERG_TABLE}"
)
print(
    f"Checkpoint        : "
    f"{CHECKPOINT_LOCATION}"
)
print("==================================================")


# ============================================================
# 13. Wait
# ============================================================

try:

    query.awaitTermination()

except KeyboardInterrupt:

    print(
        "[Streaming] "
        "Stopping..."
    )

    query.stop()

finally:

    spark.stop()

    print(
        "[Spark] "
        "Stopped"
    )