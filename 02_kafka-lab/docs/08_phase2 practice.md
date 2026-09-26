# Phase 2-10 — JWT 사용자 정보 → Kafka 이벤트 표준화

Phase 2-9까지는 `/usage` 요청을 **Pydantic으로 검증해서 Kafka에 전달**하는 것까지 했습니다.

Phase 2-10에서는 한 단계 더 나아가서:

> **Keycloak JWT의 사용자 정보를 신뢰하고 → 표준화된 Usage Event를 만들어 → Kafka에 저장**

하는 구조를 완성합니다.

---

## 1. 현재 구조

지금까지는 다음과 같습니다.

```text
사용자
 │
 │ username / password
 ▼
Keycloak
 │
 │ Access Token (JWT)
 ▼
FastAPI
 │
 │ JWT 검증
 │
 │ 사용자 정보 추출
 ▼
Usage Event 생성
 │
 ▼
Kafka
 │
 ▼
usage-events
```

여기서 중요한 점은 **사용자가 `user_id`를 직접 보내지 않는 것**입니다.

예를 들어 사용자가 이런 요청을 보내면:

```json
{
  "service": "api",
  "usage_type": "request",
  "quantity": 10
}
```

FastAPI가 JWT에서:

```text
sub                  → user_id
preferred_username   → username
```

을 가져옵니다.

따라서 최종 Kafka 이벤트는:

```json
{
  "user_id": "Keycloak에서 가져온 ID",
  "username": "kim",
  "service": "api",
  "usage_type": "request",
  "quantity": 10,
  "timestamp": "2026-09-26T..."
}
```

가 됩니다.

---

# 2. 왜 JWT에서 사용자 정보를 가져와야 할까요?

잘못된 구조는 이것입니다.

```json
POST /usage

{
  "user_id": "kim",
  "service": "api",
  "quantity": 10
}
```

사용자가 마음대로:

```json
{
  "user_id": "admin"
}
```

이라고 보낼 수도 있습니다.

즉,

```text
사용자가 주장하는 사용자
        ↓
       user_id
```

를 믿게 됩니다.

---

반면 현재 구조는:

```text
사용자
  │
  │ JWT
  ▼
FastAPI
  │
  ├── JWT 검증
  │
  └── JWT에서 user_id 추출
          │
          ▼
       Kafka Event
```

입니다.

즉 **사용자 식별 정보는 Keycloak JWT를 기준으로 합니다.**

---

# 3. Phase 2-10에서 사용할 Event 구조

앞으로 Kafka에 저장할 이벤트를 일정한 형식으로 통일합니다.

```json
{
  "event_type": "usage",
  "user_id": "xxxx",
  "username": "kim",
  "service": "api",
  "usage_type": "request",
  "quantity": 10,
  "timestamp": "2026-09-26T07:00:00+00:00"
}
```

여기서:

| 필드           | 의미              |
| ------------ | --------------- |
| `event_type` | 이벤트 종류          |
| `user_id`    | Keycloak 사용자 ID |
| `username`   | Keycloak 사용자명   |
| `service`    | 사용한 서비스         |
| `usage_type` | 사용 유형           |
| `quantity`   | 사용량             |
| `timestamp`  | 이벤트 발생 시간       |

---

# 4. `event_type`을 추가하는 이유

나중에는 Kafka에 usage 이벤트만 들어가지 않을 수 있습니다.

예를 들어:

```text
usage-events
    │
    ├── usage
    ├── login
    ├── logout
    ├── resource_created
    └── resource_deleted
```

따라서 이벤트 자체에:

```json
"event_type": "usage"
```

를 넣어두면 Consumer가 이벤트 종류를 쉽게 구분할 수 있습니다.

---

# 5. `/usage` 요청에는 user_id를 넣지 않습니다

사용자가 보내는 데이터는 딱 이것입니다.

```json
{
  "service": "api",
  "usage_type": "request",
  "quantity": 10
}
```

Pydantic 모델:

```python
class UsageRequest(BaseModel):
    service: str = Field(min_length=1)
    usage_type: str = Field(min_length=1)
    quantity: int = Field(gt=0)
```

여기에는:

```text
user_id
username
timestamp
event_type
```

가 없습니다.

이 값들은 **서버가 생성합니다.**

---

# 6. 서버에서 JWT 정보 추출

JWT 검증이 끝나면:

```python
user_id = user.get("sub")
username = user.get("preferred_username")
```

을 사용합니다.

예를 들어 Keycloak JWT가:

```json
{
  "sub": "12345678-abcd",
  "preferred_username": "kim",
  "email": "kim@example.com"
}
```

이라면:

```python
user_id
```

는:

```text
12345678-abcd
```

가 되고,

```python
username
```

은:

```text
kim
```

이 됩니다.

---

# 7. 최종 Kafka Event 생성

서버에서:

```python
event = {
    "event_type": "usage",
    "user_id": user_id,
    "username": username,
    "service": usage.service,
    "usage_type": usage.usage_type,
    "quantity": usage.quantity,
    "timestamp": datetime.now(
        timezone.utc
    ).isoformat()
}
```

를 만듭니다.

즉,

```text
Client Request
       │
       ▼
┌──────────────────────┐
│ service              │
│ usage_type           │
│ quantity             │
└──────────────────────┘
       │
       │ + JWT
       ▼
┌──────────────────────┐
│ FastAPI              │
│                      │
│ JWT → user_id        │
│ JWT → username       │
│ server → timestamp   │
│ server → event_type  │
└──────────────────────┘
       │
       ▼
   Kafka Event
```

---

# 8. Kafka Key는 `user_id`

이 부분도 중요합니다.

```python
producer.send(
    KAFKA_TOPIC,
    key=user_id,
    value=event
)
```

즉:

```text
Kafka Key
   │
   └── user_id
```

입니다.

예를 들어:

```text
kim → partition 1
kim → partition 1
kim → partition 1

lee → partition 2
lee → partition 2
```

처럼 같은 사용자의 이벤트가 동일 partition으로 갈 가능성이 높습니다.

따라서 사용자별 이벤트 순서를 유지하기 좋습니다.

---

# 9. 전체 `main.py`

현재 Phase 2-10 기준으로는 다음 형태가 가장 깔끔합니다.

```python
import os
import json

from contextlib import asynccontextmanager
from datetime import datetime, timezone

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
```

---

# 10. 실습 ① 컨테이너 재빌드

`keycloak-lab` 디렉터리에서 실행합니다.

```powershell
docker compose build api
```

그 다음:

```powershell
docker compose up -d
```

확인:

```powershell
docker compose ps
```

대략:

```text
NAME
keycloak
kafka
usage-api
```

가 실행되어야 합니다.

---

# 11. 실습 ② Consumer 실행

Consumer를 먼저 실행합니다.

```powershell
docker compose run --rm consumer
```

그러면:

```text
Kafka Consumer started...
Waiting for messages...
```

상태가 됩니다.

---

# 12. 실습 ③ Access Token 생성

PowerShell:

```powershell
$body = @{
    client_id  = "usage-api"
    username   = "kim"
    password   = "test1234"
    grant_type = "password"
}

$response = Invoke-RestMethod `
    -Method Post `
    -Uri "http://localhost:8080/realms/hdaic/protocol/openid-connect/token" `
    -ContentType "application/x-www-form-urlencoded" `
    -Body $body
```

Token:

```powershell
$token = $response.access_token
```

---

# 13. 실습 ④ `/usage` 호출

```powershell
$body = @{
    service    = "api"
    usage_type = "request"
    quantity   = 10
} | ConvertTo-Json
```

호출:

```powershell
Invoke-RestMethod `
    -Method Post `
    -Uri "http://localhost:8000/usage" `
    -Headers @{
        Authorization = "Bearer $token"
    } `
    -ContentType "application/json" `
    -Body $body
```

예상 결과:

```text
message   : Usage event sent successfully
topic     : usage-events
partition : 1
offset    : 0
event     : ...
```

---

# 14. Consumer 화면 확인

Consumer에서는:

```text
================================
Received event
================================

topic     : usage-events
partition : 1
offset    : 0

user_id   : 1234....
service   : api
usage_type: request
quantity  : 10
```

을 볼 수 있습니다.

다만 현재 Consumer가 출력하는 코드에는 `event_type`과 `username` 출력이 없으므로 다음처럼 바꾸면 더 좋습니다.

```python
print(f"user_id   : {event.get('user_id')}")
print(f"username  : {event.get('username')}")
print(f"event_type: {event.get('event_type')}")
print(f"service   : {event.get('service')}")
print(f"usage_type: {event.get('usage_type')}")
print(f"quantity  : {event.get('quantity')}")
```

---

# 15. Phase 2-10에서 가장 중요한 변화

이전:

```text
HTTP Request
     │
     ▼
{
  user_id,
  service,
  quantity
}
     │
     ▼
Kafka
```

현재:

```text
                 Keycloak
                    │
                    │ JWT
                    ▼
HTTP Request ──→ FastAPI
                  │
                  ├── JWT 검증
                  │
                  ├── user_id 추출
                  ├── username 추출
                  │
                  ├── service
                  ├── usage_type
                  ├── quantity
                  │
                  └── timestamp 생성
                         │
                         ▼
                    Usage Event
                         │
                         ▼
                       Kafka
```

이것이 이번 단계의 핵심입니다.

---

# 16. 최종적으로 Kafka에는 이런 데이터가 쌓입니다

사용자가 여러 번 API를 호출하면:

```text
usage-events
│
├── Event 1
│   ├── user_id = kim
│   ├── service = api
│   └── quantity = 10
│
├── Event 2
│   ├── user_id = kim
│   ├── service = api
│   └── quantity = 20
│
├── Event 3
│   ├── user_id = kim
│   ├── service = search
│   └── quantity = 5
│
└── Event 4
    ├── user_id = lee
    ├── service = api
    └── quantity = 30
```

이제 이 데이터가 **사용량 원천 데이터(raw usage event)**가 됩니다.

---

# 17. 전체 프로젝트에서 Phase 2가 의미하는 것

지금까지 만든 구조를 합치면:

```text
                ┌─────────────┐
                │  Keycloak   │
                │             │
                │ kim / lee   │
                └──────┬──────┘
                       │
                       │ JWT
                       ▼
                ┌─────────────┐
                │   FastAPI   │
                │             │
                │ JWT 검증    │
                │ Usage API   │
                └──────┬──────┘
                       │
                       │ Usage Event
                       ▼
                ┌─────────────┐
                │    Kafka    │
                │             │
                │usage-events │
                │             │
                │ P0 P1 P2    │
                └──────┬──────┘
                       │
                       ▼
                ┌─────────────┐
                │   Consumer  │
                └─────────────┘
```

**Phase 2-10의 목적은 여기까지입니다.**

다음 Phase 2의 마지막 정리에서는 **Keycloak → FastAPI → Kafka → Consumer 전체 흐름을 하나의 실습으로 재현**하고, 특히 `user_id → Kafka key → partition → offset → consumer group`이 어떻게 연결되는지 확인하면 좋습니다.
