"""Shared configuration for the usage-event data platform."""
import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    return value


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    kafka_source_topic: str = _env("KAFKA_SOURCE_TOPIC", "usage-events")
    kafka_status_topic: str = _env("KAFKA_STATUS_TOPIC", "pipeline-status")

    iceberg_table: str = _env("ICEBERG_TABLE", "local.usage_db.usage_events")
    iceberg_warehouse: str = _env("ICEBERG_WAREHOUSE", "s3a://warehouse/")

    minio_endpoint: str = _env("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key: str = _env("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = _env("MINIO_SECRET_KEY", "minioadmin123")
    minio_bucket: str = _env("MINIO_BUCKET", "warehouse")
    minio_table_prefix: str = _env("MINIO_TABLE_PREFIX", "usage_db/usage_events")
    minio_secure: bool = _env("MINIO_SECURE", "false").lower() == "true"

    checkpoint_location: str = _env(
        "CHECKPOINT_LOCATION",
        "s3a://warehouse/checkpoints/full_usage_pipeline",
    )
    processing_interval: str = _env("PROCESSING_INTERVAL", "5 seconds")

    query_host: str = _env("ICEBERG_QUERY_HOST", "0.0.0.0")
    query_port: int = int(_env("ICEBERG_QUERY_PORT", "8010"))
    query_service_version: str = "3.0.0"


settings = Settings()
