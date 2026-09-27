from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Phase3-4-Spark-MinIO")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

data = [
    ("kim", "api", 10),
    ("lee", "api", 20),
    ("park", "llm", 30),
]

df = spark.createDataFrame(
    data,
    ["user_id", "service", "quantity"],
)

df.show()

output_path = "s3a://warehouse/test_data"

df.write.mode("overwrite").parquet(output_path)

spark.read.parquet(output_path).show()

spark.stop()
