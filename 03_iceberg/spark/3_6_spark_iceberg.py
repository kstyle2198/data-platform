from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Phase3-6-Spark-Iceberg")
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

spark.sql("""
SELECT *
FROM local.usage_db.usage_events
""").show(truncate=False)

spark.stop()
