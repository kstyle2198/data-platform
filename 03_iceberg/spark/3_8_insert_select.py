from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Phase3-8-Insert-Select")
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
INSERT INTO local.usage_db.usage_events
VALUES
(
    'demo-001',
    'usage',
    'kim',
    'kim',
    'api',
    'request',
    10,
    '2026-09-27T00:00:00Z'
),
(
    'demo-002',
    'usage',
    'lee',
    'lee',
    'api',
    'request',
    20,
    '2026-09-27T00:00:01Z'
),
(
    'demo-003',
    'usage',
    'kim',
    'kim',
    'llm',
    'token',
    500,
    '2026-09-27T00:00:02Z'
),
(
    'demo-004',
    'usage',
    'park',
    'park',
    'storage',
    'gb',
    50,
    '2026-09-27T00:00:03Z'
)
""")

print("=== ALL EVENTS ===")

spark.sql("""
SELECT *
FROM local.usage_db.usage_events
ORDER BY timestamp
""").show(truncate=False)

print("=== BY SERVICE ===")

spark.sql("""
SELECT
    service,
    SUM(quantity) AS total_quantity
FROM local.usage_db.usage_events
GROUP BY service
ORDER BY service
""").show()

print("=== BY USER ===")

spark.sql("""
SELECT
    username,
    SUM(quantity) AS total_quantity
FROM local.usage_db.usage_events
GROUP BY username
ORDER BY username
""").show()

spark.stop()
