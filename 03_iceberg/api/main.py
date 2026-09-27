import os
import uuid
import json
import asyncio
import threading

from collections import OrderedDict
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import FileResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import jwt
from jose.exceptions import JWTError
from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel, Field


# ============================================================
# Configuration
# ============================================================

KEYCLOAK_INTERNAL_URL = os.getenv(
    "KEYCLOAK_INTERNAL_URL", "http://keycloak:8080"
)
KEYCLOAK_ISSUER = os.getenv(
    "KEYCLOAK_ISSUER", "http://localhost:8080/realms/hdaic"
)
KEYCLOAK_REALM = os.getenv("KEYCLOAK_REALM", "hdaic")
KEYCLOAK_CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID", "usage-api")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "usage-events")
PIPELINE_STATUS_TOPIC = os.getenv(
    "PIPELINE_STATUS_TOPIC", "pipeline-status"
)
MAX_PIPELINE_EVENTS = int(
    os.getenv("MAX_PIPELINE_EVENTS", "100")
)

REALM_INTERNAL_URL = (
    f"{KEYCLOAK_INTERNAL_URL}/realms/{KEYCLOAK_REALM}"
)
JWKS_URL = (
    f"{REALM_INTERNAL_URL}/protocol/openid-connect/certs"
)
KEYCLOAK_TOKEN_URL = (
    f"{REALM_INTERNAL_URL}/protocol/openid-connect/token"
)


# ============================================================
# Runtime state
# ============================================================

pipeline_status = OrderedDict()
pipeline_status_lock = threading.Lock()

pipeline_consumer_stop_event = threading.Event()
pipeline_consumer_thread = None


# ============================================================
# Models
# ============================================================

class UsageRequest(BaseModel):
    service: str = Field(min_length=1)
    usage_type: str = Field(min_length=1)
    quantity: int = Field(gt=0)


class LoginRequest(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


# ============================================================
# Pipeline state helpers
# ============================================================

def create_pipeline_status(event_id: str, event: dict):
    now = datetime.now(timezone.utc).isoformat()

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
        pipeline_status.move_to_end(event_id)

        while len(pipeline_status) > MAX_PIPELINE_EVENTS:
            pipeline_status.popitem(last=False)


def update_pipeline_stage(
    event_id: str,
    stage: str,
    status_value: str,
    **extra,
):
    with pipeline_status_lock:
        item = pipeline_status.get(event_id)

        if item is None:
            print(
                f"[Pipeline] Unknown event_id: {event_id}"
            )
            return

        if stage not in item:
            item[stage] = {}

        item[stage]["status"] = status_value
        item[stage].update(extra)
        item[stage]["updated_at"] = (
            datetime.now(timezone.utc).isoformat()
        )

        pipeline_status.move_to_end(event_id)


def get_pipeline_status(event_id: str):
    with pipeline_status_lock:
        item = pipeline_status.get(event_id)

        if item is None:
            return None

        return json.loads(json.dumps(item))


def get_recent_pipeline_status(limit: int = 20):
    limit = max(1, min(limit, MAX_PIPELINE_EVENTS))

    with pipeline_status_lock:
        items = list(pipeline_status.values())[-limit:]
        return json.loads(json.dumps(items))


# ============================================================
# Kafka pipeline-status consumer
# ============================================================

def pipeline_status_consumer_loop():
    print("[Pipeline Consumer] Starting...")

    consumer = None

    try:
        consumer = KafkaConsumer(
            PIPELINE_STATUS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="usage-api-pipeline-monitor",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda value: json.loads(
                value.decode("utf-8")
            ),
            consumer_timeout_ms=1000,
        )

        print(
            "[Pipeline Consumer] Connected to "
            f"{PIPELINE_STATUS_TOPIC}"
        )

        while not pipeline_consumer_stop_event.is_set():
            records = consumer.poll(timeout_ms=1000)

            for _, messages in records.items():
                for message in messages:
                    try:
                        data = message.value

                        event_id = data.get("event_id")
                        stage = data.get("stage")
                        status_value = data.get("status")

                        if not event_id or not stage or not status_value:
                            print(
                                "[Pipeline Consumer] "
                                "Invalid status event: "
                                f"{data}"
                            )
                            continue

                        extra = {
                            key: value
                            for key, value in data.items()
                            if key not in {
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
                            f"Message error: {exc}"
                        )

    except Exception as exc:
        print(
            "[Pipeline Consumer] Startup error: "
            f"{exc}"
        )

    finally:
        if consumer is not None:
            try:
                consumer.close()
            except Exception:
                pass

        print("[Pipeline Consumer] Stopped")


def start_pipeline_status_consumer():
    global pipeline_consumer_thread

    pipeline_consumer_stop_event.clear()

    pipeline_consumer_thread = threading.Thread(
        target=pipeline_status_consumer_loop,
        daemon=True,
    )
    pipeline_consumer_thread.start()


def stop_pipeline_status_consumer():
    pipeline_consumer_stop_event.set()

    if (
        pipeline_consumer_thread
        and pipeline_consumer_thread.is_alive()
    ):
        pipeline_consumer_thread.join(timeout=5)


# ============================================================
# FastAPI lifespan
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    producer = None

    for attempt in range(1, 11):
        try:
            print(
                "[Kafka] Connecting to "
                f"{KAFKA_BOOTSTRAP_SERVERS} "
                f"(attempt {attempt}/10)"
            )

            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                key_serializer=lambda key: key.encode("utf-8"),
                value_serializer=lambda value: json.dumps(
                    value
                ).encode("utf-8"),
                request_timeout_ms=10000,
            )

            print("[Kafka] Connected successfully")
            break

        except Exception as exc:
            print(f"[Kafka] Connection failed: {exc}")

            if attempt == 10:
                raise

            await asyncio.sleep(5)

    app.state.kafka_producer = producer

    start_pipeline_status_consumer()

    yield

    stop_pipeline_status_consumer()

    if producer is not None:
        try:
            producer.flush()
        except Exception:
            pass

        try:
            producer.close()
        except Exception:
            pass


# ============================================================
# Application
# ============================================================

app = FastAPI(
    title="Usage Pipeline Monitor",
    description=(
        "Keycloak + FastAPI + Kafka + Spark + "
        "Iceberg + MinIO pipeline demo"
    ),
    version="2.0.0",
    lifespan=lifespan,
)

security = HTTPBearer()


# ============================================================
# Authentication
# ============================================================

@app.post("/login")
async def login(login_request: LoginRequest):
    data = {
        "client_id": KEYCLOAK_CLIENT_ID,
        "username": login_request.username,
        "password": login_request.password,
        "grant_type": "password",
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                KEYCLOAK_TOKEN_URL,
                data=data,
                timeout=5.0,
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
            "access_token": token_data["access_token"],
            "token_type": token_data.get(
                "token_type", "Bearer"
            ),
        }

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=(
                "Unable to connect to Keycloak: "
                f"{exc}"
            ),
        )


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
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Unable to connect to Keycloak JWKS endpoint: "
                f"{exc}"
            ),
        )


async def verify_token(token: str):
    try:
        header = jwt.get_unverified_header(token)
        kid = header.get("kid")

        if not kid:
            raise HTTPException(
                status_code=401,
                detail="JWT kid is missing",
            )

        jwks = await get_jwks()

        key = None

        for jwk in jwks.get("keys", []):
            if jwk.get("kid") == kid:
                key = jwk
                break

        if key is None:
            raise HTTPException(
                status_code=401,
                detail=(
                    "Unable to find matching "
                    "Keycloak public key"
                ),
            )

        return jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            issuer=KEYCLOAK_ISSUER,
            audience=KEYCLOAK_CLIENT_ID,
        )

    except HTTPException:
        raise

    except JWTError as exc:
        raise HTTPException(
            status_code=401,
            detail=f"Invalid JWT: {exc}",
        )

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"JWT verification error: {exc}",
        )


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(
        security
    ),
):
    return await verify_token(
        credentials.credentials
    )


# ============================================================
# Health
# ============================================================

@app.get("/health")
async def health():
    producer = getattr(
        app.state,
        "kafka_producer",
        None,
    )

    return {
        "status": "ok",
        "kafka": (
            "connected"
            if producer is not None
            else "disconnected"
        ),
        "kafka_topic": KAFKA_TOPIC,
        "pipeline_status_topic": PIPELINE_STATUS_TOPIC,
        "tracked_events": len(pipeline_status),
    }


# ============================================================
# User
# ============================================================

@app.get("/me")
async def me(user=Depends(get_current_user)):
    return {
        "user_id": user.get("sub"),
        "username": user.get("preferred_username"),
        "email": user.get("email"),
        "name": user.get("name"),
        "roles": user.get(
            "realm_access", {}
        ).get("roles", []),
    }


# ============================================================
# Usage event
# ============================================================

@app.post("/usage")
async def create_usage(
    usage: UsageRequest,
    user=Depends(get_current_user),
):
    event_id = str(uuid.uuid4())

    event = {
        "event_id": event_id,
        "event_type": "usage",
        "user_id": user.get("sub"),
        "username": user.get("preferred_username"),
        "service": usage.service,
        "usage_type": usage.usage_type,
        "quantity": usage.quantity,
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    create_pipeline_status(
        event_id,
        event,
    )

    producer = app.state.kafka_producer

    try:
        future = producer.send(
            KAFKA_TOPIC,
            key=event["user_id"],
            value=event,
        )

        metadata = future.get(timeout=10)

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
                "Failed to send usage event to Kafka: "
                f"{exc}"
            ),
        )

    return {
        "message": "Usage event sent successfully",
        "event_id": event_id,
        "event": event,
        "kafka": {
            "topic": metadata.topic,
            "partition": metadata.partition,
            "offset": metadata.offset,
        },
    }


# ============================================================
# Pipeline monitoring APIs
# ============================================================

@app.get("/pipeline/{event_id}")
async def get_pipeline(
    event_id: str,
    user=Depends(get_current_user),
):
    data = get_pipeline_status(event_id)

    if data is None:
        raise HTTPException(
            status_code=404,
            detail=f"Event not found: {event_id}",
        )

    return data


@app.get("/pipeline")
async def get_recent_pipeline(
    limit: int = 20,
    user=Depends(get_current_user),
):
    events = get_recent_pipeline_status(limit)

    return {
        "count": len(events),
        "events": events,
    }


@app.get("/pipeline/{event_id}/summary")
async def get_pipeline_summary(
    event_id: str,
    user=Depends(get_current_user),
):
    data = get_pipeline_status(event_id)

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
        data[stage].get("status") == "completed"
        for stage in stages
    )

    failed = sum(
        data[stage].get("status") == "failed"
        for stage in stages
    )

    if failed:
        overall_status = "failed"
    elif completed == len(stages):
        overall_status = "completed"
    else:
        overall_status = "processing"

    return {
        "event_id": event_id,
        "overall_status": overall_status,
        "completed_stages": completed,
        "total_stages": len(stages),
        "failed_stages": failed,
        "stages": {
            stage: data[stage].get("status")
            for stage in stages
        },
    }


# ============================================================
# Web UI
# ============================================================

@app.get("/")
async def index():
    return FileResponse(
        "static/index.html"
    )
