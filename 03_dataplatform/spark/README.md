# Integrated Iceberg Pipeline Refactor

## Modules

- `common/config.py`: shared environment-driven configuration.
- `common/spark_factory.py`: common Spark, Iceberg Hadoop catalog, and MinIO/S3A settings.
- `common/schema.py`: canonical usage-event schema and column list.
- `common/serialization.py`: JSON-safe normalization for Spark/Python values.
- `pipeline.py`: Kafka Structured Streaming, Iceberg writes, MinIO Parquet inspection, and `pipeline-status` events.
- `query_api.py`: HTTP endpoints for health, snapshots, time travel, and guarded row deletion.

The streaming pipeline and query API intentionally remain separate processes/containers. They share code and configuration, not a SparkSession.

## Run

Run commands from the directory containing these files, in the same Python environment/container that has PySpark, Iceberg runtime jars, S3A dependencies, Kafka connector jars, and `minio` installed.

```bash
python pipeline.py
python query_api.py
```

Environment variables (defaults shown):

```dotenv
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_SOURCE_TOPIC=usage-events
KAFKA_STATUS_TOPIC=pipeline-status
ICEBERG_TABLE=local.usage_db.usage_events
ICEBERG_WAREHOUSE=s3a://warehouse/
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
MINIO_BUCKET=warehouse
MINIO_TABLE_PREFIX=usage_db/usage_events
CHECKPOINT_LOCATION=s3a://warehouse/checkpoints/full_usage_pipeline
PROCESSING_INTERVAL=5 seconds
ICEBERG_QUERY_HOST=0.0.0.0
ICEBERG_QUERY_PORT=8010
```

## API smoke tests

```bash
curl http://localhost:8010/health
curl http://localhost:8010/snapshots
curl 'http://localhost:8010/time-travel?snapshot_id=<SNAPSHOT_ID>&limit=100'
curl -X POST http://localhost:8010/clear \
  -H 'Content-Type: application/json' \
  -d '{"confirmation":"DELETE ALL ROWS"}'
```

## Important operational notes

1. The code keeps the original table schema, topic names, checkpoint default, status fields, and API routes.
2. `POST /clear` is destructive. The confirmation string is not authentication; do not expose this API publicly without authentication/authorization and network restrictions.
3. `foreachBatch` and Iceberg append are not automatically an end-to-end exactly-once transaction. A process failure after Iceberg commit but before checkpoint progress may append the same batch again. Consider a durable batch ledger or a deduplication strategy before production use.
4. MinIO Parquet listing is a physical-file diagnostic, not the authoritative definition of Iceberg table contents. Iceberg metadata determines which files belong to the current table state.
5. This refactor has not been run against the user's Docker Compose environment. Validate the Python package paths and Spark/Iceberg/Kafka/S3A connector versions in the existing image before replacing the current scripts.
