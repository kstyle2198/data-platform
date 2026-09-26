import os
import json

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

import httpx

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    status,
)

from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBearer,
)

from jose import jwt
from jose.exceptions import JWTError

from kafka import KafkaProducer

from pydantic import BaseModel, Field


# ============================================================
# 1. Configuration
# ============================================================

KEYCLOAK_INTERNAL_URL = os.getenv(
    "KEYCLOAK_INTERNAL_URL",
    "http://keycloak:8080"
)

KEYCLOAK_ISSUER = os.getenv(
    "KEYCLOAK_ISSUER",
    "http://localhost:8080/realms/hdaic"
)

KEYCLOAK_REALM = os.getenv(
    "KEYCLOAK_REALM",
    "hdaic"
)

KEYCLOAK_CLIENT_ID = os.getenv(
    "KEYCLOAK_CLIENT_ID",
    "usage-api"
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "usage-events"
)



# ============================================================
# 2. Keycloak Internal URLs
# ============================================================

REALM_INTERNAL_URL = (
    f"{KEYCLOAK_INTERNAL_URL}"
    f"/realms/{KEYCLOAK_REALM}"
)

OIDC_CONFIG_URL = (
    f"{REALM_INTERNAL_URL}"
    f"/.well-known/openid-configuration"
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
# 3. Pydantic Request Model
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


# ============================================================
# 4. FastAPI Lifespan
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    print("========================================")
    print("FastAPI startup")
    print("========================================")

    print(
        f"Kafka server: "
        f"{KAFKA_BOOTSTRAP_SERVERS}"
    )

    print(
        f"Kafka topic: "
        f"{KAFKA_TOPIC}"
    )

    # --------------------------------------------------------
    # Startup
    # --------------------------------------------------------

    producer = KafkaProducer(

        bootstrap_servers=
            KAFKA_BOOTSTRAP_SERVERS,

        key_serializer=lambda key:
            key.encode("utf-8"),

        value_serializer=lambda value:
            json.dumps(value).encode("utf-8")
    )

    app.state.kafka_producer = producer

    print(
        "KafkaProducer initialized"
    )

    # --------------------------------------------------------
    # Application Running
    # --------------------------------------------------------

    yield

    # --------------------------------------------------------
    # Shutdown
    # --------------------------------------------------------

    print("========================================")
    print("FastAPI shutdown")
    print("========================================")

    producer.flush()

    producer.close()

    print(
        "KafkaProducer closed"
    )


# ============================================================
# 5. FastAPI Application
# ============================================================

app = FastAPI(
    title="Keycloak + Kafka Usage API",

    description=(
        "Keycloak JWT authentication "
        "and Kafka usage event demo"
    ),

    version="1.0.0",

    lifespan=lifespan
)


# ============================================================
# 6. HTTP Bearer
# ============================================================

security = HTTPBearer()

# ============================================================
# 7. Login# ============================================================

class LoginRequest(BaseModel):

    username: str = Field(
        min_length=1
    )

    password: str = Field(
        min_length=1
    )

@app.post("/login")
async def login(
    login_request: LoginRequest
):

    data = {
        "client_id": KEYCLOAK_CLIENT_ID,
        "username": login_request.username,
        "password": login_request.password,
        "grant_type": "password"
    }

    print("========================================")
    print("Login request")
    print("========================================")
    print(
        f"username = {login_request.username}"
    )
    print(
        f"client_id = {KEYCLOAK_CLIENT_ID}"
    )
    print(
        f"token_url = {KEYCLOAK_TOKEN_URL}"
    )

    try:

        async with httpx.AsyncClient() as client:

            response = await client.post(
                KEYCLOAK_TOKEN_URL,
                data=data,
                timeout=5.0
            )

        print(
            f"Keycloak status = "
            f"{response.status_code}"
        )

        print(
            f"Keycloak response = "
            f"{response.text}"
        )

        if response.status_code != 200:

            raise HTTPException(
                status_code=401,
                detail=(
                    "Keycloak login failed: "
                    f"{response.text}"
                )
            )

        token_data = response.json()

        return {
            "access_token":
                token_data["access_token"],

            "token_type":
                token_data.get(
                    "token_type",
                    "Bearer"
                )
        }

    except HTTPException:
        raise

    except Exception as e:

        print(
            f"Keycloak connection error: {e}"
        )

        raise HTTPException(
            status_code=503,
            detail=(
                "Unable to connect to "
                f"Keycloak: {str(e)}"
            )
        )


# ============================================================
# 7. Get Keycloak Public Keys
# ============================================================

async def get_jwks():

    try:

        async with httpx.AsyncClient() as client:

            response = await client.get(
                JWKS_URL,
                timeout=5.0
            )

            response.raise_for_status()

            return response.json()

    except httpx.HTTPError as e:

        raise HTTPException(
            status_code=
                status.HTTP_503_SERVICE_UNAVAILABLE,

            detail=(
                "Unable to connect to "
                "Keycloak JWKS endpoint: "
                f"{str(e)}"
            )
        )


# ============================================================
# 8. Verify JWT
# ============================================================

async def verify_token(token: str):

    try:

        # ----------------------------------------------------
        # JWT Header
        # ----------------------------------------------------

        header = jwt.get_unverified_header(
            token
        )

        kid = header.get("kid")

        if not kid:

            raise HTTPException(
                status_code=
                    status.HTTP_401_UNAUTHORIZED,

                detail="JWT kid is missing"
            )

        # ----------------------------------------------------
        # Keycloak JWKS
        # ----------------------------------------------------

        jwks = await get_jwks()

        key = None

        for jwk in jwks.get("keys", []):

            if jwk.get("kid") == kid:

                key = jwk

                break

        if key is None:

            raise HTTPException(
                status_code=
                    status.HTTP_401_UNAUTHORIZED,

                detail=(
                    "Unable to find matching "
                    "Keycloak public key"
                )
            )

        # ----------------------------------------------------
        # JWT Decode
        # ----------------------------------------------------

        payload = jwt.decode(

            token,

            key,

            algorithms=["RS256"],

            issuer=KEYCLOAK_ISSUER,

            audience=KEYCLOAK_CLIENT_ID
        )

        return payload

    except HTTPException:

        raise

    except JWTError as e:

        raise HTTPException(

            status_code=
                status.HTTP_401_UNAUTHORIZED,

            detail=(
                f"Invalid JWT: {str(e)}"
            )
        )

    except Exception as e:

        raise HTTPException(

            status_code=
                status.HTTP_500_INTERNAL_SERVER_ERROR,

            detail=(
                "JWT verification error: "
                f"{str(e)}"
            )
        )


# ============================================================
# 9. Current User
# ============================================================

async def get_current_user(

    credentials:
        HTTPAuthorizationCredentials =
        Depends(security)
):

    token = credentials.credentials

    payload = await verify_token(
        token
    )

    return payload


# ============================================================
# 10. Health
# ============================================================

@app.get("/health")
async def health():

    return {
        "status": "ok"
    }


# ============================================================
# 11. Public API
# ============================================================

@app.get("/public")
async def public():

    return {
        "message":
            "This endpoint is public"
    }


# ============================================================
# 12. Current User
# ============================================================

@app.get("/me")
async def me(
    user=Depends(get_current_user)
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
                {}
            ).get(
                "roles",
                []
            )
    }


# ============================================================
# 13. Usage API
# ============================================================

@app.post("/usage")
async def create_usage(

    usage: UsageRequest,

    user=Depends(
        get_current_user
    )
):

    # --------------------------------------------------------
    # Keycloak JWT에서 사용자 정보 추출
    # --------------------------------------------------------

    user_id = user.get(
        "sub"
    )

    username = user.get(
        "preferred_username"
    )

    # --------------------------------------------------------
    # 표준 Usage Event 생성
    # --------------------------------------------------------

    event = {

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
            ).isoformat()
    }

    # --------------------------------------------------------
    # Kafka Producer
    # --------------------------------------------------------

    producer = (
        app.state.kafka_producer
    )

    # --------------------------------------------------------
    # Kafka 전송
    # --------------------------------------------------------

    try:

        future = producer.send(

            KAFKA_TOPIC,

            key=user_id,

            value=event
        )

        metadata = future.get(
            timeout=10
        )

    except Exception as e:

        raise HTTPException(

            status_code=503,

            detail=(
                "Failed to send usage "
                "event to Kafka: "
                f"{str(e)}"
            )
        )

    # --------------------------------------------------------
    # Response
    # --------------------------------------------------------

    return {

        "message":
            "Usage event sent successfully",

        "topic":
            metadata.topic,

        "partition":
            metadata.partition,

        "offset":
            metadata.offset,

        "event":
            event
    }

# ============================================================
# Web UI
# ============================================================

@app.get("/")
async def index():

    return FileResponse(
        "static/index.html"
    )