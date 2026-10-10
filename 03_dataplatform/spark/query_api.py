"""HTTP API for Iceberg snapshots, time travel, and guarded table clearing."""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from common.config import settings
from common.serialization import normalize
from common.spark_factory import create_spark

SERVICE_VERSION = settings.query_service_version
TABLE = settings.iceberg_table


def get_snapshots(spark):
    rows = spark.sql(f"""
        SELECT committed_at, snapshot_id, parent_id, operation, summary
        FROM {TABLE}.snapshots
        ORDER BY committed_at DESC
        LIMIT 100
    """).collect()
    snapshots = []
    for row in rows:
        item = normalize(row)
        item["snapshot_id"] = str(item["snapshot_id"])
        if item.get("parent_id") is not None:
            item["parent_id"] = str(item["parent_id"])
        snapshots.append(item)

    current_id = None
    try:
        ref = spark.sql(f"""
            SELECT snapshot_id FROM {TABLE}.refs
            WHERE name = 'main' LIMIT 1
        """).first()
        if ref:
            current_id = str(ref["snapshot_id"])
    except Exception as exc:
        print(f"[TimeTravel] refs lookup unavailable: {exc}", flush=True)

    if current_id is None and snapshots:
        current_id = snapshots[0]["snapshot_id"]
    for snapshot in snapshots:
        snapshot["is_current"] = snapshot["snapshot_id"] == current_id
    return {
        "table": TABLE,
        "current_snapshot_id": current_id,
        "count": len(snapshots),
        "snapshots": snapshots,
    }


def get_time_travel(spark, snapshot_id, limit):
    try:
        selected_id = int(snapshot_id)
    except (TypeError, ValueError):
        raise ValueError("snapshot_id must be an integer")
    if not 1 <= limit <= 200:
        raise ValueError("limit must be between 1 and 200")

    found = spark.sql(f"""
        SELECT snapshot_id FROM {TABLE}.snapshots
        WHERE snapshot_id = {selected_id} LIMIT 1
    """).first()
    if not found:
        raise LookupError(f"Snapshot not found: {selected_id}")

    historical_count = spark.sql(
        f"SELECT COUNT(*) AS total FROM {TABLE} VERSION AS OF {selected_id}"
    ).first()["total"]
    current_count = spark.sql(f"SELECT COUNT(*) AS total FROM {TABLE}").first()["total"]
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
    before = spark.sql(f"SELECT COUNT(*) AS total FROM {TABLE}").first()["total"]
    spark.sql(f"DELETE FROM {TABLE}")
    after = spark.sql(f"SELECT COUNT(*) AS total FROM {TABLE}").first()["total"]
    latest = spark.sql(f"""
        SELECT snapshot_id, committed_at, operation
        FROM {TABLE}.snapshots
        ORDER BY committed_at DESC LIMIT 1
    """).first()
    return {
        "table": TABLE,
        "before_count": before,
        "deleted_count": before - after,
        "after_count": after,
        "snapshot_id": str(latest["snapshot_id"]) if latest else None,
        "committed_at": normalize(latest["committed_at"]) if latest else None,
        "operation": latest["operation"] if latest else None,
        "history_preserved": True,
    }


class Handler(BaseHTTPRequestHandler):
    server_version = f"IcebergQueryAPI/{SERVICE_VERSION}"

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
                "status": "ok", "service": "iceberg-query",
                "version": SERVICE_VERSION, "table": TABLE,
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
                return self.send_json(200, get_time_travel(spark, snapshot_id, limit))
            return self.send_json(404, {"detail": "Endpoint not found"})
        except ValueError as exc:
            return self.send_json(400, {"detail": str(exc)})
        except LookupError as exc:
            return self.send_json(404, {"detail": str(exc)})
        except Exception as exc:
            print(f"[TimeTravel] GET {parsed.path} failed: {exc}", flush=True)
            return self.send_json(500, {"detail": "Iceberg query failed"})

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/clear":
            return self.send_json(404, {"detail": "Endpoint not found"})

        try:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except (TypeError, ValueError):
                return self.send_json(400, {"detail": "Invalid Content-Length"})
            if length < 0:
                return self.send_json(400, {"detail": "Invalid Content-Length"})
            if length > 4096:
                return self.send_json(413, {"detail": "Request body too large"})

            raw_body = self.rfile.read(length) if length else b"{}"
            try:
                payload = json.loads(raw_body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                return self.send_json(400, {"detail": "Request body must be valid JSON"})
            if not isinstance(payload, dict):
                return self.send_json(400, {"detail": "JSON body must be an object"})
            if payload.get("confirmation") != "DELETE ALL ROWS":
                return self.send_json(
                    400, {"detail": 'confirmation must be exactly "DELETE ALL ROWS"'}
                )

            result = clear_table_rows(self.server.spark)
            return self.send_json(200, {"success": True, **result})
        except Exception as exc:
            print(f"[TimeTravel] POST /clear failed: {exc}", flush=True)
            return self.send_json(500, {"detail": "Iceberg data deletion failed"})

    def log_message(self, fmt, *args):
        print("[TimeTravel HTTP] " + fmt % args, flush=True)


def main():
    spark = create_spark("IcebergTimeTravelAPI")
    server = None
    try:
        # Validate table/catalog configuration before opening the listening port.
        spark.sql(f"SELECT * FROM {TABLE}.snapshots LIMIT 1").collect()
        server = ThreadingHTTPServer(
            (settings.query_host, settings.query_port), Handler
        )
        server.spark = spark
        print(
            f"[TimeTravel] Listening on {settings.query_host}:{settings.query_port}; "
            f"table={TABLE}; version={SERVICE_VERSION}",
            flush=True,
        )
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        if server is not None:
            server.server_close()
        spark.stop()


if __name__ == "__main__":
    main()
