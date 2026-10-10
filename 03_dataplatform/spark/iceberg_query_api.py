import json
import math
import os
from datetime import date, datetime
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from pyspark.sql import SparkSession


HOST = "0.0.0.0"
PORT = int(os.getenv("ICEBERG_QUERY_PORT", "8010"))
TABLE = "local.usage_db.usage_events"
SERVICE_VERSION = "2.0.0"

WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://warehouse/")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")


def create_spark():
    """Create a Spark session configured for the Iceberg Hadoop catalog and MinIO."""
    return (
        SparkSession.builder
        .appName("IcebergTimeTravelAPI")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def normalize(value):
    """Convert Spark/Python values to JSON-serializable values."""
    if hasattr(value, "asDict"):
        return normalize(value.asDict(recursive=True))
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {str(key): normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value
    return str(value)


def get_snapshots(spark):
    rows = spark.sql(
        f"""
        SELECT committed_at, snapshot_id, parent_id, operation, summary
        FROM {TABLE}.snapshots
        ORDER BY committed_at DESC
        LIMIT 100
        """
    ).collect()

    snapshots = []
    for row in rows:
        item = normalize(row)
        # Keep Iceberg's 64-bit snapshot IDs as strings for JavaScript clients.
        item["snapshot_id"] = str(item["snapshot_id"])
        if item.get("parent_id") is not None:
            item["parent_id"] = str(item["parent_id"])
        snapshots.append(item)

    current_snapshot_id = None
    try:
        ref = spark.sql(
            f"""
            SELECT snapshot_id
            FROM {TABLE}.refs
            WHERE name = 'main'
            LIMIT 1
            """
        ).first()
        if ref:
            current_snapshot_id = str(ref["snapshot_id"])
    except Exception as exc:
        print(f"[TimeTravel] refs lookup unavailable: {exc}", flush=True)

    # Fallback for Iceberg versions/catalogs where refs metadata is unavailable.
    if current_snapshot_id is None and snapshots:
        current_snapshot_id = snapshots[0]["snapshot_id"]

    for snapshot in snapshots:
        snapshot["is_current"] = snapshot["snapshot_id"] == current_snapshot_id

    return {
        "table": TABLE,
        "current_snapshot_id": current_snapshot_id,
        "count": len(snapshots),
        "snapshots": snapshots,
    }


def get_time_travel(spark, snapshot_id, limit):
    try:
        selected_id = int(snapshot_id)
    except (TypeError, ValueError):
        raise ValueError("snapshot_id must be an integer")

    if limit < 1 or limit > 200:
        raise ValueError("limit must be between 1 and 200")

    found = spark.sql(
        f"""
        SELECT snapshot_id
        FROM {TABLE}.snapshots
        WHERE snapshot_id = {selected_id}
        LIMIT 1
        """
    ).first()
    if not found:
        raise LookupError(f"Snapshot not found: {selected_id}")

    historical_count = spark.sql(
        f"SELECT COUNT(*) AS total FROM {TABLE} VERSION AS OF {selected_id}"
    ).first()["total"]
    current_count = spark.sql(
        f"SELECT COUNT(*) AS total FROM {TABLE}"
    ).first()["total"]
    rows = spark.sql(
        f"SELECT * FROM {TABLE} VERSION AS OF {selected_id} LIMIT {limit}"
    ).collect()

    return {
        "table": TABLE,
        "snapshot_id": str(selected_id),
        "historical_count": historical_count,
        "current_count": current_count,
        "returned_rows": len(rows),
        "limit": limit,
        "columns": list(rows[0].__fields__) if rows else [],
        "rows": [normalize(row) for row in rows],
    }


def clear_table_rows(spark):
    """Delete all rows while retaining the Iceberg table and snapshot history."""
    before_count = spark.sql(
        f"SELECT COUNT(*) AS total FROM {TABLE}"
    ).first()["total"]

    # This creates a new Iceberg snapshot; it does not erase previous snapshots.
    spark.sql(f"DELETE FROM {TABLE}")

    after_count = spark.sql(
        f"SELECT COUNT(*) AS total FROM {TABLE}"
    ).first()["total"]

    latest = spark.sql(
        f"""
        SELECT snapshot_id, committed_at, operation
        FROM {TABLE}.snapshots
        ORDER BY committed_at DESC
        LIMIT 1
        """
    ).first()

    return {
        "table": TABLE,
        "before_count": before_count,
        "deleted_count": before_count - after_count,
        "after_count": after_count,
        "snapshot_id": str(latest["snapshot_id"]) if latest else None,
        "committed_at": normalize(latest["committed_at"]) if latest else None,
        "operation": latest["operation"] if latest else None,
        "history_preserved": True,
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "IcebergQueryAPI/" + SERVICE_VERSION

    def send_json(self, status_code, payload):
        body = json.dumps(
            normalize(payload), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        spark = self.server.spark

        if parsed.path == "/health":
            return self.send_json(200, {
                "status": "ok",
                "service": "iceberg-query",
                "version": SERVICE_VERSION,
                "table": TABLE,
            })

        try:
            if parsed.path == "/snapshots":
                return self.send_json(200, get_snapshots(spark))

            if parsed.path == "/time-travel":
                snapshot_id = params.get("snapshot_id", [""])[0]
                try:
                    limit = int(params.get("limit", ["100"])[0])
                except ValueError:
                    raise ValueError("limit must be an integer")
                return self.send_json(
                    200, get_time_travel(spark, snapshot_id, limit)
                )

            return self.send_json(404, {"detail": "Endpoint not found"})

        except ValueError as exc:
            return self.send_json(400, {"detail": str(exc)})
        except LookupError as exc:
            return self.send_json(404, {"detail": str(exc)})
        except Exception as exc:
            print(f"[TimeTravel] GET {parsed.path} failed: {exc}", flush=True)
            return self.send_json(500, {"detail": "Iceberg query failed"})

    def do_POST(self):
        """Handle destructive table clear requests; this method prevents HTTP 501."""
        parsed = urlparse(self.path)
        if parsed.path != "/clear":
            return self.send_json(404, {"detail": "Endpoint not found"})

        try:
            raw_length = self.headers.get("Content-Length", "0")
            try:
                content_length = int(raw_length)
            except (TypeError, ValueError):
                return self.send_json(400, {"detail": "Invalid Content-Length"})

            if content_length < 0:
                return self.send_json(400, {"detail": "Invalid Content-Length"})
            if content_length > 4096:
                return self.send_json(413, {"detail": "Request body too large"})

            raw_body = self.rfile.read(content_length) if content_length else b"{}"
            try:
                payload = json.loads(raw_body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                return self.send_json(400, {"detail": "Request body must be valid JSON"})

            if not isinstance(payload, dict):
                return self.send_json(400, {"detail": "JSON body must be an object"})

            if payload.get("confirmation") != "DELETE ALL ROWS":
                return self.send_json(
                    400,
                    {"detail": 'confirmation must be exactly "DELETE ALL ROWS"'},
                )

            print("[TimeTravel] POST /clear accepted; clearing table rows", flush=True)
            result = clear_table_rows(self.server.spark)
            print(
                "[TimeTravel] POST /clear completed: "
                f"before={result['before_count']}, after={result['after_count']}",
                flush=True,
            )
            return self.send_json(200, {"success": True, **result})

        except Exception as exc:
            print(f"[TimeTravel] POST /clear failed: {exc}", flush=True)
            return self.send_json(500, {"detail": "Iceberg data deletion failed"})

    def log_message(self, fmt, *args):
        print("[TimeTravel HTTP] " + fmt % args, flush=True)


if __name__ == "__main__":
    spark = create_spark()

    # Fail at startup if the configured Iceberg table/catalog cannot be queried.
    spark.sql(f"SELECT * FROM {TABLE}.snapshots LIMIT 1").collect()

    server = ThreadingHTTPServer((HOST, PORT), Handler)
    server.spark = spark

    print(
        f"[TimeTravel] Listening on {HOST}:{PORT}; "
        f"table={TABLE}; version={SERVICE_VERSION}",
        flush=True,
    )

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        spark.stop()
