import json
import os
from datetime import date, datetime
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from pyspark.sql import SparkSession


HOST = "0.0.0.0"
PORT = int(os.getenv("ICEBERG_QUERY_PORT", "8010"))
TABLE = "local.usage_db.usage_events"

WAREHOUSE = os.getenv(
    "ICEBERG_WAREHOUSE",
    "s3a://warehouse/",
)
MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://minio:9000",
)
MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY",
    "minioadmin",
)
MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    "minioadmin123",
)


def create_spark():
    return (
        SparkSession.builder
        .appName("IcebergTimeTravelAPI")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions."
            "IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.local",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.local.type", "hadoop")
        .config(
            "spark.sql.catalog.local.warehouse",
            WAREHOUSE,
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            MINIO_ENDPOINT,
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            MINIO_ACCESS_KEY,
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            MINIO_SECRET_KEY,
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def normalize(value):
    if hasattr(value, "asDict"):
        return normalize(value.asDict(recursive=True))

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, dict):
        return {
            str(key): normalize(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [normalize(item) for item in value]

    if value is None or isinstance(
        value, (str, int, float, bool)
    ):
        return value

    return str(value)


def get_snapshots(spark):
    rows = spark.sql(
        f"""
        SELECT
            committed_at,
            snapshot_id,
            parent_id,
            operation,
            summary
        FROM {TABLE}.snapshots
        ORDER BY committed_at DESC
        LIMIT 100
        """
    ).collect()

    snapshots = []

    for row in rows:
        item = normalize(row)
        # JavaScript Number는 큰 정수를 정확히 표현하지 못할 수
        # 있으므로 Snapshot ID를 문자열로 전달합니다.
        item["snapshot_id"] = str(item["snapshot_id"])

        if item.get("parent_id") is not None:
            item["parent_id"] = str(item["parent_id"])

        snapshots.append(item)

    current_snapshot_id = None

    # Iceberg refs 메타데이터에서 main 브랜치의
    # 현재 Snapshot ID를 확인합니다.
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
        print(f"[TimeTravel] refs lookup unavailable: {exc}")

    # refs 메타데이터를 조회할 수 없는 경우 목록의 최신
    # 스냅샷을 대체 표시합니다.
    if current_snapshot_id is None and snapshots:
        current_snapshot_id = snapshots[0]["snapshot_id"]

    for snapshot in snapshots:
        snapshot["is_current"] = (
            snapshot["snapshot_id"] == current_snapshot_id
        )

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

    # 목록에 실제 존재하는 스냅샷인지 먼저 확인합니다.
    found = spark.sql(
        f"""
        SELECT snapshot_id
        FROM {TABLE}.snapshots
        WHERE snapshot_id = {selected_id}
        LIMIT 1
        """
    ).first()

    if not found:
        raise LookupError(
            f"Snapshot not found: {selected_id}"
        )

    historical_count = spark.sql(
        f"""
        SELECT COUNT(*) AS total
        FROM {TABLE} VERSION AS OF {selected_id}
        """
    ).first()["total"]

    current_count = spark.sql(
        f"SELECT COUNT(*) AS total FROM {TABLE}"
    ).first()["total"]

    rows = spark.sql(
        f"""
        SELECT *
        FROM {TABLE} VERSION AS OF {selected_id}
        LIMIT {limit}
        """
    ).collect()

    data = [normalize(row) for row in rows]

    return {
        "table": TABLE,
        "snapshot_id": str(selected_id),
        "historical_count": historical_count,
        "current_count": current_count,
        "returned_rows": len(data),
        "limit": limit,
        "columns": rows[0].__fields__ if rows else [],
        "rows": data,
    }


class Handler(BaseHTTPRequestHandler):
    def send_json(self, status_code, payload):
        body = json.dumps(
            payload,
            ensure_ascii=False,
        ).encode("utf-8")

        self.send_response(status_code)
        self.send_header(
            "Content-Type",
            "application/json; charset=utf-8",
        )
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        spark = self.server.spark

        if parsed.path == "/health":
            return self.send_json(200, {"status": "ok"})

        try:
            if parsed.path == "/snapshots":
                return self.send_json(
                    200,
                    get_snapshots(spark),
                )

            if parsed.path == "/time-travel":
                snapshot_id = params.get(
                    "snapshot_id", [""]
                )[0]

                limit = int(
                    params.get("limit", ["100"])[0]
                )

                return self.send_json(
                    200,
                    get_time_travel(
                        spark,
                        snapshot_id,
                        limit,
                    ),
                )

            return self.send_json(
                404,
                {"detail": "Endpoint not found"},
            )

        except ValueError as exc:
            return self.send_json(
                400,
                {"detail": str(exc)},
            )

        except LookupError as exc:
            return self.send_json(
                404,
                {"detail": str(exc)},
            )

        except Exception as exc:
            print(f"[TimeTravel] Query failed: {exc}")
            return self.send_json(
                500,
                {"detail": "Iceberg query failed"},
            )

    def log_message(self, fmt, *args):
        print("[TimeTravel HTTP] " + fmt % args)


if __name__ == "__main__":
    spark = create_spark()

    # 테이블 접근을 시작할 때 확인해 초기 설정 오류를
    # 서비스 시작 로그에서 확인할 수 있도록 합니다.
    spark.sql(f"SELECT * FROM {TABLE}.snapshots LIMIT 1").collect()

    server = ThreadingHTTPServer(
        (HOST, PORT),
        Handler,
    )
    server.spark = spark

    print(
        f"[TimeTravel] Listening on {HOST}:{PORT}; "
        f"table={TABLE}"
    )

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        spark.stop()