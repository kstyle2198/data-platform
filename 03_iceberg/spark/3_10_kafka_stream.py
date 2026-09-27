from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

spark = (
    SparkSession.builder
    .appName("Phase3-10-Kafka-Stream")
    .getOrCreate()
)

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

stream_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "kafka:9092",
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
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation",
            "s3a://warehouse/checkpoints/phase3_10")
    .start()
)

query.awaitTermination()
