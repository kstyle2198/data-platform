import os
import uuid
import json
import asyncio
import threading

from io import BytesIO
from collections import OrderedDict
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    status,
)

from fastapi.responses import FileResponse

from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBearer,
)

from jose import jwt
from jose.exceptions import JWTError

from kafka import KafkaConsumer, KafkaProducer

from pydantic import BaseModel, Field

from minio import Minio
from minio.error import S3Error

import pyarrow.parquet as pq


# ============================================================
# Configuration
# ============================================================

KEYCLOAK_INTERNAL_URL = os.getenv(
    "KEYCLOAK_INTERNAL_URL",
    "http://keycloak:8080",
)

KEYCLOAK_ISSUER = os.getenv(
    "KEYCLOAK_ISSUER",
    "http://localhost:8080/realms/hdaic",
)

KEYCLOAK_REALM = os.getenv(
    "KEYCLOAK_REALM",
    "hdaic",
)

KEYCLOAK_CLIENT_ID = os.getenv(
    "KEYCLOAK_CLIENT_ID",
    "usage-api",
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "usage-events",
)

PIPELINE_STATUS_TOPIC = os.getenv(
    "PIPELINE_STATUS_TOPIC",
    "pipeline-status",
)

MAX_PIPELINE_EVENTS = int(
    os.getenv(
        "MAX_PIPELINE_EVENTS",
        "100",
    )
)


# ============================================================
# MinIO / Iceberg Configuration
# ============================================================

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "minio:9000",
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY",
    "minioadmin",
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    "minioadmin123",
)

MINIO_BUCKET = os.getenv(
    "MINIO_BUCKET",
    "warehouse",
)

ICEBERG_TABLE_PREFIX = os.getenv(
    "ICEBERG_TABLE_PREFIX",
    "usage_db/usage_events",
)


# ============================================================
# Keycloak Internal URLs
# ============================================================

REALM_INTERNAL_URL = (
    f"{KEYCLOAK_INTERNAL_URL}"
    f"/realms/{KEYCLOAK_REALM}"
)

JWKS_URL = (
    f"{REALM_INTERNAL_URL}"
    f"/protocol/openid-connect/certs"
)

KEYCLOAK_TOKEN_URL = (
    f"{REALM_INTERNAL_URL}"
    f"/protocol/openid-connect/token"
)


# ============================================================
# Runtime state
# ============================================================

pipeline_status = OrderedDict()

pipeline_status_lock = threading.Lock()

pipeline_consumer_stop_event = threading.Event()

pipeline_consumer_thread = None


# ============================================================
# FastAPI Models
# ============================================================

class UsageRequest(BaseModel):

    service: str = Field(
        min_length=1
    )

    usage_type: str = Field(
        min_length=1
    )

    quantity: int = Field(
        gt=0
    )


class LoginRequest(BaseModel):

    username: str = Field(
        min_length=1
    )

    password: str = Field(
        min_length=1
    )


# ============================================================
# MinIO Client
# ============================================================

def get_minio_client():

    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


# ============================================================
# Pipeline Status Helpers
# ============================================================

def create_pipeline_status(
    event_id: str,
    event: dict,
):

    now = datetime.now(
        timezone.utc
    ).isoformat()

    data = {

        "event_id": event_id,

        "created_at": now,

        "event": event,

        "fastapi": {
            "status": "completed",
            "completed_at": now,
        },

        "kafka": {
            "status": "processing",
        },

        "spark": {
            "status": "waiting",
        },

        "iceberg": {
            "status": "waiting",
        },

        "minio": {
            "status": "waiting",
        },
    }

    with pipeline_status_lock:

        pipeline_status[event_id] = data

        pipeline_status.move_to_end(
            event_id
        )

        while (
            len(pipeline_status)
            > MAX_PIPELINE_EVENTS
        ):

            pipeline_status.popitem(
                last=False
            )


def update_pipeline_stage(
    event_id: str,
    stage: str,
    status_value: str,
    **extra,
):

    with pipeline_status_lock:

        item = pipeline_status.get(
            event_id
        )

        if item is None:

            print(
                "[Pipeline] Unknown "
                f"event_id: {event_id}"
            )

            return

        if stage not in item:

            item[stage] = {}

        item[stage]["status"] = (
            status_value
        )

        item[stage].update(extra)

        item[stage]["updated_at"] = (
            datetime.now(
                timezone.utc
            ).isoformat()
        )

        pipeline_status.move_to_end(
            event_id
        )


def get_pipeline_status(
    event_id: str,
):

    with pipeline_status_lock:

        item = pipeline_status.get(
            event_id
        )

        if item is None:
            return None

        return json.loads(
            json.dumps(item)
        )


def get_recent_pipeline_status(
    limit: int = 20,
):

    limit = max(
        1,
        min(
            limit,
            MAX_PIPELINE_EVENTS,
        ),
    )

    with pipeline_status_lock:

        items = list(
            pipeline_status.values()
        )[-limit:]

        return json.loads(
            json.dumps(items)
        )


# ============================================================
# Kafka pipeline-status Consumer
# ============================================================

def pipeline_status_consumer_loop():

    print(
        "[Pipeline Consumer] Starting..."
    )

    consumer = None

    try:

        consumer = KafkaConsumer(

            PIPELINE_STATUS_TOPIC,

            bootstrap_servers=(
                KAFKA_BOOTSTRAP_SERVERS
            ),

            group_id=(
                "usage-api-pipeline-monitor"
            ),

            auto_offset_reset="latest",

            enable_auto_commit=True,

            value_deserializer=(
                lambda value:
                    json.loads(
                        value.decode(
                            "utf-8"
                        )
                    )
            ),

            consumer_timeout_ms=1000,
        )

        print(
            "[Pipeline Consumer] "
            "Connected to "
            f"{PIPELINE_STATUS_TOPIC}"
        )

        while not pipeline_consumer_stop_event.is_set():

            records = consumer.poll(
                timeout_ms=1000
            )

            for _, messages in records.items():

                for message in messages:

                    try:

                        data = message.value

                        event_id = data.get(
                            "event_id"
                        )

                        stage = data.get(
                            "stage"
                        )

                        status_value = data.get(
                            "status"
                        )

                        if (
                            not event_id
                            or not stage
                            or not status_value
                        ):

                            print(
                                "[Pipeline Consumer] "
                                "Invalid status event: "
                                f"{data}"
                            )

                            continue

                        extra = {
                            key: value
                            for key, value
                            in data.items()
                            if key
                            not in {
                                "event_id",
                                "stage",
                                "status",
                            }
                        }

                        update_pipeline_stage(
                            event_id=event_id,
                            stage=stage,
                            status_value=status_value,
                            **extra,
                        )

                    except Exception as exc:

                        print(
                            "[Pipeline Consumer] "
                            "Message processing error: "
                            f"{exc}"
                        )

    except Exception as exc:

        print(
            "[Pipeline Consumer] "
            f"Consumer error: {exc}"
        )

    finally:

        if consumer is not None:

            try:
                consumer.close()
            except Exception:
                pass

        print(
            "[Pipeline Consumer] Stopped"
        )


# ============================================================
# FastAPI Lifespan
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    global pipeline_consumer_thread

    producer = None

    # --------------------------------------------------------
    # Kafka Producer
    # --------------------------------------------------------

    for attempt in range(1, 11):

        try:

            print(
                "[Kafka] Connecting to "
                f"{KAFKA_BOOTSTRAP_SERVERS} "
                f"(attempt {attempt}/10)"
            )

            producer = KafkaProducer(

                bootstrap_servers=(
                    KAFKA_BOOTSTRAP_SERVERS
                ),

                key_serializer=(
                    lambda key:
                        key.encode("utf-8")
                ),

                value_serializer=(
                    lambda value:
                        json.dumps(
                            value
                        ).encode("utf-8")
                ),

                request_timeout_ms=10000,
            )

            print(
                "[Kafka] Connected successfully"
            )

            break

        except Exception as exc:

            print(
                "[Kafka] Connection failed: "
                f"{exc}"
            )

            if attempt == 10:

                print(
                    "[Kafka] Failed to connect "
                    "after 10 attempts"
                )

                raise

            await asyncio.sleep(5)

    app.state.kafka_producer = producer

    # --------------------------------------------------------
    # Pipeline Status Consumer
    # --------------------------------------------------------

    pipeline_consumer_stop_event.clear()

    pipeline_consumer_thread = (
        threading.Thread(
            target=pipeline_status_consumer_loop,
            daemon=True,
        )
    )

    pipeline_consumer_thread.start()

    # --------------------------------------------------------
    # Application running
    # --------------------------------------------------------

    yield

    # --------------------------------------------------------
    # Shutdown
    # --------------------------------------------------------

    print(
        "[Pipeline Consumer] "
        "Stopping..."
    )

    pipeline_consumer_stop_event.set()

    if (
        pipeline_consumer_thread
        and pipeline_consumer_thread.is_alive()
    ):

        pipeline_consumer_thread.join(
            timeout=5
        )

    if producer is not None:

        print(
            "[Kafka] Closing producer"
        )

        producer.flush()

        producer.close()


# ============================================================
# FastAPI Application
# ============================================================

app = FastAPI(

    title=(
        "Keycloak + Kafka + "
        "Iceberg Usage API"
    ),

    description=(
        "Usage data pipeline demo"
    ),

    version="1.0.0",

    lifespan=lifespan,
)


# ============================================================
# HTTP Bearer
# ============================================================

security = HTTPBearer()


# ============================================================
# Login
# ============================================================

@app.post("/login")
async def login(
    login_request: LoginRequest,
):

    data = {

        "client_id":
            KEYCLOAK_CLIENT_ID,

        "username":
            login_request.username,

        "password":
            login_request.password,

        "grant_type":
            "password",
    }

    print(
        "========================================"
    )

    print(
        "Login request"
    )

    print(
        "========================================"
    )

    print(
        f"username = "
        f"{login_request.username}"
    )

    print(
        f"client_id = "
        f"{KEYCLOAK_CLIENT_ID}"
    )

    print(
        f"token_url = "
        f"{KEYCLOAK_TOKEN_URL}"
    )

    try:

        async with httpx.AsyncClient() as client:

            response = await client.post(

                KEYCLOAK_TOKEN_URL,

                data=data,

                timeout=5.0,
            )

        print(
            "Keycloak status = "
            f"{response.status_code}"
        )

        if response.status_code != 200:

            raise HTTPException(

                status_code=401,

                detail=(
                    "Keycloak login failed: "
                    f"{response.text}"
                ),
            )

        token_data = response.json()

        return {

            "access_token":
                token_data[
                    "access_token"
                ],

            "token_type":
                token_data.get(
                    "token_type",
                    "Bearer",
                ),

            "expires_in":
                token_data.get(
                    "expires_in"
                ),
        }

    except HTTPException:

        raise

    except Exception as exc:

        raise HTTPException(

            status_code=503,

            detail=(
                "Unable to connect to "
                f"Keycloak: {exc}"
            ),
        )


# ============================================================
# Get Keycloak JWKS
# ============================================================

async def get_jwks():

    try:

        async with httpx.AsyncClient() as client:

            response = await client.get(

                JWKS_URL,

                timeout=5.0,
            )

            response.raise_for_status()

            return response.json()

    except httpx.HTTPError as exc:

        raise HTTPException(

            status_code=(
                status.HTTP_503_SERVICE_UNAVAILABLE
            ),

            detail=(
                "Unable to connect to "
                "Keycloak JWKS endpoint: "
                f"{exc}"
            ),
        )


# ============================================================
# Verify JWT
# ============================================================

async def verify_token(
    token: str,
):

    try:

        header = jwt.get_unverified_header(
            token
        )

        kid = header.get(
            "kid"
        )

        if not kid:

            raise HTTPException(

                status_code=(
                    status.HTTP_401_UNAUTHORIZED
                ),

                detail="JWT kid is missing",
            )

        jwks = await get_jwks()

        key = None

        for jwk in jwks.get(
            "keys",
            [],
        ):

            if jwk.get(
                "kid"
            ) == kid:

                key = jwk

                break

        if key is None:

            raise HTTPException(

                status_code=(
                    status.HTTP_401_UNAUTHORIZED
                ),

                detail=(
                    "Unable to find matching "
                    "Keycloak public key"
                ),
            )

        payload = jwt.decode(

            token,

            key,

            algorithms=[
                "RS256"
            ],

            issuer=KEYCLOAK_ISSUER,

            audience=KEYCLOAK_CLIENT_ID,
        )

        return payload

    except HTTPException:

        raise

    except JWTError as exc:

        raise HTTPException(

            status_code=(
                status.HTTP_401_UNAUTHORIZED
            ),

            detail=(
                f"Invalid JWT: {exc}"
            ),
        )

    except Exception as exc:

        raise HTTPException(

            status_code=(
                status.HTTP_500_INTERNAL_SERVER_ERROR
            ),

            detail=(
                "JWT verification error: "
                f"{exc}"
            ),
        )


# ============================================================
# Current User
# ============================================================

async def get_current_user(
    credentials:
        HTTPAuthorizationCredentials =
        Depends(security),
):

    token = credentials.credentials

    payload = await verify_token(
        token
    )

    return payload


# ============================================================
# Health
# ============================================================

@app.get("/health")
async def health():

    return {
        "status": "ok"
    }


# ============================================================
# Public API
# ============================================================

@app.get("/public")
async def public():

    return {
        "message":
            "This endpoint is public"
    }


# ============================================================
# Current User API
# ============================================================

@app.get("/me")
async def me(
    user=Depends(get_current_user),
):

    return {

        "user_id":
            user.get("sub"),

        "username":
            user.get(
                "preferred_username"
            ),

        "email":
            user.get("email"),

        "name":
            user.get("name"),

        "roles":
            user.get(
                "realm_access",
                {},
            ).get(
                "roles",
                [],
            ),
    }


# ============================================================
# Usage API
# ============================================================

@app.post("/usage")
async def create_usage(

    usage: UsageRequest,

    user=Depends(get_current_user),
):

    event_id = str(
        uuid.uuid4()
    )

    user_id = user.get(
        "sub"
    )

    username = user.get(
        "preferred_username"
    )

    event = {

        "event_id":
            event_id,

        "event_type":
            "usage",

        "user_id":
            user_id,

        "username":
            username,

        "service":
            usage.service,

        "usage_type":
            usage.usage_type,

        "quantity":
            usage.quantity,

        "timestamp":
            datetime.now(
                timezone.utc
            ).isoformat(),
    }

    create_pipeline_status(
        event_id=event_id,
        event=event,
    )

    producer = app.state.kafka_producer

    try:

        future = producer.send(

            KAFKA_TOPIC,

            key=user_id,

            value=event,
        )

        metadata = future.get(
            timeout=10
        )

        update_pipeline_stage(

            event_id,

            "kafka",

            "completed",

            topic=metadata.topic,

            partition=metadata.partition,

            offset=metadata.offset,
        )

        update_pipeline_stage(

            event_id,

            "spark",

            "processing",
        )

    except Exception as exc:

        update_pipeline_stage(

            event_id,

            "kafka",

            "failed",

            error=str(exc),
        )

        raise HTTPException(

            status_code=503,

            detail=(
                "Failed to send usage "
                "event to Kafka: "
                f"{exc}"
            ),
        )

    return {

        "message":
            "Usage event sent successfully",

        "event_id":
            event_id,

        "event":
            event,

        "kafka": {

            "topic":
                metadata.topic,

            "partition":
                metadata.partition,

            "offset":
                metadata.offset,
        },
    }


# ============================================================
# Pipeline Monitoring APIs
# ============================================================

@app.get("/pipeline/{event_id}")
async def get_pipeline(

    event_id: str,

    user=Depends(get_current_user),
):

    data = get_pipeline_status(
        event_id
    )

    if data is None:

        raise HTTPException(

            status_code=404,

            detail=(
                f"Event not found: "
                f"{event_id}"
            ),
        )

    return data


@app.get("/pipeline")
async def get_recent_pipeline(

    limit: int = 20,

    user=Depends(get_current_user),
):

    events = get_recent_pipeline_status(
        limit
    )

    return {

        "count":
            len(events),

        "events":
            events,
    }


@app.get("/pipeline/{event_id}/summary")
async def get_pipeline_summary(

    event_id: str,

    user=Depends(get_current_user),
):

    data = get_pipeline_status(
        event_id
    )

    if data is None:

        raise HTTPException(

            status_code=404,

            detail="Event not found",
        )

    stages = [

        "fastapi",

        "kafka",

        "spark",

        "iceberg",

        "minio",
    ]

    completed = sum(

        data.get(
            stage,
            {},
        ).get(
            "status"
        ) == "completed"

        for stage in stages
    )

    failed = sum(

        data.get(
            stage,
            {},
        ).get(
            "status"
        ) == "failed"

        for stage in stages
    )

    if failed:

        overall_status = "failed"

    elif completed == len(stages):

        overall_status = "completed"

    else:

        overall_status = "processing"

    return {

        "event_id":
            event_id,

        "overall_status":
            overall_status,

        "completed_stages":
            completed,

        "total_stages":
            len(stages),

        "failed_stages":
            failed,

        "stages": {

            stage:
                data.get(
                    stage,
                    {},
                ).get(
                    "status"
                )

            for stage in stages
        },
    }


# ============================================================
# Parquet Helpers
# ============================================================

def get_parquet_prefix():

    return (
        f"{ICEBERG_TABLE_PREFIX}"
        f"/data/"
    )


def normalize_parquet_value(
    value
):

    if value is None:

        return None

    if hasattr(
        value,
        "isoformat",
    ):

        try:
            return value.isoformat()
        except Exception:
            pass

    if isinstance(
        value,
        bytes,
    ):

        return value.decode(
            "utf-8",
            errors="replace",
        )

    if isinstance(
        value,
        (
            str,
            int,
            float,
            bool,
        ),
    ):

        return value

    return str(value)


def read_parquet_rows_for_event(

    event_id: str,

    limit: int = 100,
):

    client = get_minio_client()

    prefix = get_parquet_prefix()

    parquet_files = []

    matched_rows = []

    objects = client.list_objects(

        MINIO_BUCKET,

        prefix=prefix,

        recursive=True,
    )

    for obj in objects:

        object_name = obj.object_name

        if not object_name.endswith(
            ".parquet"
        ):

            continue

        parquet_files.append(
            object_name
        )

        if len(
            matched_rows
        ) >= limit:

            continue

        try:

            response = client.get_object(

                MINIO_BUCKET,

                object_name,
            )

            try:

                payload = response.read()

            finally:

                response.close()

                response.release_conn()

            table = pq.read_table(
                BytesIO(payload)
            )

            rows = table.to_pylist()

            for row in rows:

                if str(
                    row.get(
                        "event_id"
                    )
                ) != str(
                    event_id
                ):

                    continue

                normalized = {

                    key:
                        normalize_parquet_value(
                            value
                        )

                    for key, value
                    in row.items()
                }

                normalized[
                    "_parquet_file"
                ] = object_name

                matched_rows.append(
                    normalized
                )

                if len(
                    matched_rows
                ) >= limit:

                    break

        except Exception as exc:

            print(
                "[Parquet] Failed to read "
                f"{object_name}: {exc}"
            )

    return {

        "event_id":
            event_id,

        "bucket":
            MINIO_BUCKET,

        "prefix":
            prefix,

        "files_scanned":
            len(parquet_files),

        "parquet_files":
            parquet_files,

        "row_count":
            len(matched_rows),

        "rows":
            matched_rows[:limit],
    }


def read_recent_parquet_rows(

    limit: int = 50,
):

    client = get_minio_client()

    prefix = get_parquet_prefix()

    parquet_files = []

    rows = []

    objects = client.list_objects(

        MINIO_BUCKET,

        prefix=prefix,

        recursive=True,
    )

    for obj in objects:

        object_name = obj.object_name

        if not object_name.endswith(
            ".parquet"
        ):

            continue

        parquet_files.append(
            object_name
        )

        if len(rows) >= limit:
            break

        try:

            response = client.get_object(

                MINIO_BUCKET,

                object_name,
            )

            try:

                payload = response.read()

            finally:

                response.close()

                response.release_conn()

            table = pq.read_table(
                BytesIO(payload)
            )

            file_rows = table.to_pylist()

            for row in file_rows:

                normalized = {

                    key:
                        normalize_parquet_value(
                            value
                        )

                    for key, value
                    in row.items()
                }

                normalized[
                    "_parquet_file"
                ] = object_name

                rows.append(
                    normalized
                )

                if len(rows) >= limit:
                    break

        except Exception as exc:

            print(
                "[Parquet] Failed to read "
                f"{object_name}: {exc}"
            )

    return {

        "bucket":
            MINIO_BUCKET,

        "prefix":
            prefix,

        "files_scanned":
            len(parquet_files),

        "parquet_files":
            parquet_files,

        "row_count":
            len(rows),

        "rows":
            rows[:limit],
    }


# ============================================================
# Iceberg / Parquet APIs
# ============================================================

@app.get(
    "/iceberg/parquet/{event_id}"
)
async def get_event_parquet(

    event_id: str,

    limit: int = 100,

    user=Depends(get_current_user),
):

    limit = max(
        1,
        min(
            limit,
            1000,
        ),
    )

    try:

        return read_parquet_rows_for_event(

            event_id=event_id,

            limit=limit,
        )

    except S3Error as exc:

        raise HTTPException(

            status_code=503,

            detail=(
                "MinIO error: "
                f"{exc}"
            ),
        )

    except Exception as exc:

        print(
            "[Parquet API] "
            f"{exc}"
        )

        raise HTTPException(

            status_code=500,

            detail=(
                "Failed to read "
                f"Parquet data: {exc}"
            ),
        )


@app.get(
    "/iceberg/parquet"
)
async def get_recent_parquet(

    limit: int = 50,

    user=Depends(get_current_user),
):

    limit = max(
        1,
        min(
            limit,
            1000,
        ),
    )

    try:

        return read_recent_parquet_rows(
            limit=limit
        )

    except S3Error as exc:

        raise HTTPException(

            status_code=503,

            detail=(
                "MinIO error: "
                f"{exc}"
            ),
        )

    except Exception as exc:

        print(
            "[Parquet API] "
            f"{exc}"
        )

        raise HTTPException(

            status_code=500,

            detail=(
                "Failed to read "
                f"Parquet data: {exc}"
            ),
        )


# ============================================================
# Web UI
# ============================================================

@app.get("/")
async def index():

    return FileResponse(
        "static/index.html"
    )