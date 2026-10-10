"""Canonical Kafka usage-event schema."""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("username", StringType(), True),
    StructField("service", StringType(), True),
    StructField("usage_type", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("timestamp", StringType(), True),
])

EVENT_COLUMNS = [
    "event_id", "event_type", "user_id", "username",
    "service", "usage_type", "quantity", "timestamp",
]
