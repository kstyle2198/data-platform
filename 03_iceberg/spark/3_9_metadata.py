from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Phase3-9-Iceberg-Metadata")
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

table = "local.usage_db.usage_events"

print("=== DESCRIBE TABLE ===")
spark.sql(f"DESCRIBE TABLE {table}").show(truncate=False)

print("=== HISTORY ===")
spark.sql(f"SELECT * FROM {table}.history").show(
    truncate=False
)

print("=== SNAPSHOTS ===")
spark.sql(f"SELECT * FROM {table}.snapshots").show(
    truncate=False
)

spark.stop()
