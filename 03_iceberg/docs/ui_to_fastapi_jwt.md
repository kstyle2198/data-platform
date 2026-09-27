네. 여기서 중요한 점은 **FastAPI가 UI의 JWT 값을 “가져오는” 것이 아니라, UI가 FastAPI 요청의 HTTP `Authorization` 헤더에 JWT를 전달하고 FastAPI가 그 값을 읽는 것**입니다.

```text
UI
 │
 │ Authorization: Bearer <JWT>
 ▼
FastAPI
 │
 │ JWT 검증
 ▼
Keycloak JWKS
```

### 1. UI가 JWT를 가지고 있음

Keycloak 로그인 성공 후 UI가 Access Token을 받습니다.

```javascript
const token = "...Keycloak Access Token...";
```

그리고 FastAPI 호출 시:

```javascript
fetch("/usage", {
    method: "POST",
    headers: {
        "Authorization": `Bearer ${token}`,
        "Content-Type": "application/json"
    },
    body: JSON.stringify({
        service: "api",
        usage_type: "request",
        quantity: 10
    })
});
```

즉 JWT가 HTTP Header에 들어갑니다.

```http
POST /usage

Authorization: Bearer eyJhbGciOi...
Content-Type: application/json
```

---

### 2. FastAPI가 JWT를 읽음

FastAPI에서는 `Authorization` Header를 Dependency로 받을 수 있습니다.

```python
from fastapi import Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

security = HTTPBearer()

@app.post("/usage")
async def create_usage(
    usage: UsageRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    token = credentials.credentials

    print("JWT:", token)

    # JWT 검증
    user = verify_token(token)

    ...
```

여기서:

```python
credentials.credentials
```

가 실제 JWT 문자열입니다.

즉,

```text
Authorization: Bearer eyJ...
                         ↑
                         credentials.credentials
```

---

### 3. FastAPI가 Keycloak에 JWT를 검증

FastAPI가 단순히 JWT 문자열을 읽는 것만으로 끝나는 것은 아닙니다.

```text
UI
 │
 │ Bearer JWT
 ▼
FastAPI
 │
 │ JWT 검증
 │
 ▼
Keycloak
 │
 │ JWKS Public Key
 ▼
FastAPI
```

검증이 성공하면 JWT 안의 정보를 사용할 수 있습니다.

예:

```json
{
  "sub": "12345678",
  "preferred_username": "kim",
  "aud": "usage-api",
  "iss": "http://localhost:8080/realms/hdaic"
}
```

FastAPI에서는:

```python
user_id = user.get("sub")
username = user.get("preferred_username")
```

처럼 사용할 수 있습니다.

---

### 4. 현재 Phase 3.12에서는 이렇게 연결됨

따라서 현재 구조를 정확하게 표현하면:

```text
                   ┌────────────┐
                   │  Keycloak  │
                   └─────┬──────┘
                         │
                  Access Token
                         │
                         ▼
                   ┌────────────┐
                   │    UI      │
                   └─────┬──────┘
                         │
               Authorization: Bearer JWT
                         │
                         ▼
                   ┌────────────┐
                   │  FastAPI   │
                   └─────┬──────┘
                         │
                  Kafka Producer
                         │
                         ▼
                   ┌────────────┐
                   │   Kafka    │
                   └─────┬──────┘
                         │
                  Structured Streaming
                         │
                         ▼
                   ┌────────────┐
                   │   Spark    │
                   └─────┬──────┘
                         │
                    Iceberg API
                         │
                         ▼
                   ┌────────────┐
                   │  Iceberg   │
                   └─────┬──────┘
                         │
                        S3A
                         │
                         ▼
                   ┌────────────┐
                   │   MinIO    │
                   └────────────┘
```

따라서 README에서는 **`Keycloak → FastAPI → Kafka`라고 단순하게 그리는 것보다**, 인증과 데이터 흐름을 분리해서 다음처럼 표현하는 것이 정확합니다.

```text
인증:
Keycloak ──JWT──> UI ──Bearer JWT──> FastAPI

데이터:
UI ──HTTP──> FastAPI ──Kafka──> Spark ──Iceberg──> MinIO
```

그리고 **FastAPI가 JWT에서 `user_id`, `username`을 추출하여 Kafka Event에 넣기 때문에**, 이후 Kafka → Spark → Iceberg → MinIO 데이터에 사용자 정보가 함께 전달됩니다.
