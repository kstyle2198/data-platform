좋습니다. 이제 **실제로 성공한 구성**을 기준으로 Phase 1을 하나의 실습 내용으로 정리하면 다음과 같습니다.

# Keycloak + FastAPI JWT 인증 실습 최종 정리

## 1. 전체 구조

이번 실습의 목표는 **Keycloak에서 로그인한 사용자의 JWT를 FastAPI가 검증하고 사용자 정보를 가져오는 것**입니다.

```text
                         Docker Compose
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│   ┌──────────────┐                  ┌──────────────────┐    │
│   │   Keycloak   │                  │     FastAPI      │    │
│   │              │                  │                  │    │
│   │ Realm: hdaic │                  │ usage-api        │    │
│   │              │                  │ :8000            │    │
│   │ :8080        │                  │                  │    │
│   └──────┬───────┘                  └────────┬─────────┘    │
│          │                                   │              │
│          │ JWT                               │              │
│          └───────────────┐                   │              │
│                          ▼                   │              │
│                    JWT 검증 ◄────────────────┘              │
│                                                             │
└─────────────────────────────────────────────────────────────┘

Windows
   │
   ├── http://localhost:8080  → Keycloak
   │
   └── http://localhost:8000  → FastAPI
```

---

# 2. 프로젝트 구조

```text
keycloak-lab/
│
├── docker-compose.yml
│
├── keycloak/
│   └── hdaic-realm.json
│
└── api/
    ├── Dockerfile
    ├── requirements.txt
    └── main.py
```

---

# 3. Docker Compose

핵심은 Keycloak의 **데이터 영속성**과 **Realm 자동 import**입니다.

```yaml
services:

  keycloak:
    image: quay.io/keycloak/keycloak:26.7.4
    container_name: keycloak

    command:
      - start-dev
      - --import-realm
      - --http-port=8080
      - --hostname=http://localhost:8080

    environment:
      KC_BOOTSTRAP_ADMIN_USERNAME: admin
      KC_BOOTSTRAP_ADMIN_PASSWORD: admin123

    ports:
      - "8080:8080"

    volumes:
      - keycloak_data:/opt/keycloak/data
      - ./keycloak/hdaic-realm.json:/opt/keycloak/data/import/hdaic-realm.json:ro

  api:
    build: ./api
    container_name: usage-api

    environment:
      KEYCLOAK_INTERNAL_URL: http://keycloak:8080
      KEYCLOAK_ISSUER: http://localhost:8080/realms/hdaic
      KEYCLOAK_REALM: hdaic
      KEYCLOAK_CLIENT_ID: usage-api

    ports:
      - "8000:8000"

    depends_on:
      - keycloak

volumes:
  keycloak_data:
```

### 중요한 부분

```yaml
KEYCLOAK_INTERNAL_URL: http://keycloak:8080
```

이것은 **Docker 내부 통신용**입니다.

```text
FastAPI container
      │
      │ http://keycloak:8080
      ▼
Keycloak container
```

반면:

```yaml
KEYCLOAK_ISSUER: http://localhost:8080/realms/hdaic
```

이것은 **JWT의 `iss` 값을 검증하기 위한 외부 주소**입니다.

---

# 4. Realm 자동 생성

`hdaic-realm.json`에 다음 설정이 들어 있습니다.

```text
Realm
└── hdaic

Client
└── usage-api

User
└── kim
    ├── password: test1234
    └── role: user

Audience Mapper
└── usage-api
```

따라서 처음 실행하면:

```bash
docker compose up -d
```

Keycloak이 자동으로:

```text
hdaic Realm
usage-api Client
kim User
user Role
Audience Mapper
```

를 생성합니다.

로그에서도:

```text
Realm 'hdaic' imported
Import finished successfully
Keycloak 26.7.4 ... started
Listening on: http://0.0.0.0:8080
```

를 확인할 수 있습니다.

---

# 5. Keycloak의 역할

Keycloak은 여기서 **인증 서버**입니다.

사용자가:

```text
kim
test1234
```

로 로그인하면 Keycloak이 JWT Access Token을 발급합니다.

개념적으로:

```text
kim
 │
 │ username/password
 ▼
Keycloak
 │
 │ Access Token
 ▼
JWT
```

JWT 안에는 다음과 같은 정보가 들어갑니다.

```json
{
  "sub": "사용자 ID",
  "preferred_username": "kim",
  "email": "kim@example.com",
  "name": "Kim Style",
  "aud": "usage-api",
  "iss": "http://localhost:8080/realms/hdaic"
}
```

---

# 6. FastAPI의 역할

FastAPI는 사용자가 보내온 JWT를 검증합니다.

```text
Client
  │
  │ Authorization: Bearer JWT
  ▼
FastAPI
  │
  ├── JWT Header 확인
  │
  ├── kid 확인
  │
  ├── Keycloak Public Key 조회
  │
  ├── Signature 검증
  │
  ├── Issuer 검증
  │
  └── Audience 검증
  │
  ▼
사용자 정보
```

---

# 7. 가장 중요한 문제: Internal URL vs External Issuer

이번 실습에서 가장 중요한 개념입니다.

## 외부 주소

Windows에서 Keycloak에 접근할 때:

```text
http://localhost:8080
```

따라서 JWT의 Issuer는:

```text
http://localhost:8080/realms/hdaic
```

입니다.

---

## Docker 내부 주소

FastAPI 컨테이너에서 Keycloak에 접근할 때는:

```text
http://keycloak:8080
```

입니다.

Docker Compose가:

```text
keycloak
```

이라는 서비스 이름을 DNS처럼 사용할 수 있기 때문입니다.

---

# 8. 왜 `localhost`를 사용하면 안 되는가?

FastAPI 컨테이너 안에서:

```text
localhost
```

는 **Keycloak이 아닙니다.**

```text
┌─────────────────┐
│ FastAPI         │
│                 │
│ localhost       │───> FastAPI 자신
└─────────────────┘
```

Keycloak은 별도의 컨테이너입니다.

```text
┌─────────────────┐       ┌─────────────────┐
│ FastAPI         │       │ Keycloak        │
│ localhost       │       │ localhost       │
│                 │       │                 │
└─────────────────┘       └─────────────────┘
         │                         │
         └──── keycloak:8080 ─────┘
```

따라서 FastAPI → Keycloak 통신에는:

```text
http://keycloak:8080
```

을 사용해야 합니다.

---

# 9. 특히 중요한 JWKS 처리

JWT 검증에는 Keycloak의 **Public Key**가 필요합니다.

Keycloak의 JWKS endpoint:

```text
/realms/hdaic/protocol/openid-connect/certs
```

입니다.

따라서 FastAPI에서는:

```python
JWKS_URL = (
    f"{REALM_INTERNAL_URL}"
    f"/protocol/openid-connect/certs"
)
```

로 직접 구성했습니다.

결과:

```text
http://keycloak:8080/realms/hdaic/protocol/openid-connect/certs
```

입니다.

이 부분이 이번 `All connection attempts failed` 문제를 해결한 핵심입니다.

---

# 10. 최종 `main.py`의 핵심 구조

전체 코드는 다음 순서입니다.

```text
main.py

① 환경변수 읽기
        ↓
② Docker 내부 Keycloak URL 생성
        ↓
③ OIDC URL 생성
        ↓
④ JWKS URL 생성
        ↓
⑤ JWT Header에서 kid 확인
        ↓
⑥ Keycloak JWKS에서 Public Key 찾기
        ↓
⑦ JWT 검증
        ↓
   ├── RS256
   ├── Signature
   ├── Issuer
   └── Audience
        ↓
⑧ 사용자 정보 반환
```

---

# 11. `/public`과 `/me`

API에는 두 가지 endpoint가 있습니다.

### Public

```http
GET /public
```

JWT가 없어도 됩니다.

```json
{
  "message": "This endpoint is public"
}
```

### Protected

```http
GET /me
```

JWT가 반드시 필요합니다.

```http
Authorization: Bearer <ACCESS_TOKEN>
```

성공하면:

```json
{
  "user_id": "...",
  "username": "kim",
  "email": "kim@example.com",
  "name": "Kim Style",
  "roles": [
    "user"
  ]
}
```

---

# 12. 전체 인증 흐름

이제 전체 과정을 하나로 연결하면 다음과 같습니다.

```text
① 사용자
   │
   │ 로그인
   ▼
② Keycloak
   │
   │ Access Token 발급
   ▼
③ Client
   │
   │ Authorization: Bearer JWT
   ▼
④ FastAPI /me
   │
   │ JWT Header
   │ kid 확인
   ▼
⑤ Keycloak JWKS
   │
   │ Public Key
   ▼
⑥ FastAPI
   │
   ├── Signature 검증
   ├── iss 검증
   └── aud 검증
   │
   ▼
⑦ 사용자 정보 추출
   │
   ├── sub
   ├── preferred_username
   ├── email
   ├── name
   └── roles
```

---

# 13. 이번 실습에서 배운 핵심 개념

| 개념           | 의미                         |
| ------------ | -------------------------- |
| Keycloak     | 인증/인가 서버                   |
| Realm        | 사용자/인증 영역                  |
| Client       | Keycloak을 사용하는 애플리케이션      |
| Access Token | 인증 결과로 발급되는 JWT            |
| JWT          | 사용자 인증 정보를 담은 토큰           |
| `iss`        | 토큰을 발급한 서버                 |
| `aud`        | 이 토큰을 사용할 대상               |
| `sub`        | 사용자 식별자                    |
| `kid`        | JWT 서명에 사용된 Key ID         |
| JWKS         | JWT 검증에 사용하는 Public Key 목록 |
| RS256        | RSA 기반 JWT 서명 알고리즘         |
| Bearer Token | HTTP 요청에 전달하는 Access Token |

---

# 14. `docker compose down`과 데이터

이번 구성에서는:

```yaml
volumes:
  - keycloak_data:/opt/keycloak/data
```

가 있기 때문에 일반적인:

```bash
docker compose down
```

후 다시:

```bash
docker compose up -d
```

해도 Keycloak 데이터가 유지됩니다.

반면:

```bash
docker compose down -v
```

를 실행하면:

```text
keycloak_data
```

까지 삭제됩니다.

그러면 다시 Realm import가 실행됩니다.

즉:

```text
docker compose down
        ↓
컨테이너 삭제
        ↓
데이터 유지
        ↓
docker compose up
        ↓
기존 Keycloak 데이터 사용
```

입니다.

---

# 15. 현재 Phase 1의 최종 결과

현재까지 구축한 것은 다음과 같습니다.

```text
┌───────────────┐
│   Keycloak    │
│               │
│ Realm: hdaic  │
│ User: kim     │
│ Client:       │
│ usage-api     │
└───────┬───────┘
        │
        │ JWT
        ▼
┌────────────────────┐
│      FastAPI       │
│                    │
│      GET /me       │
│                    │
│ JWT Verification   │
│        ↓           │
│ User Information   │
└────────────────────┘
```

**즉, "Keycloak에서 인증 → JWT 발급 → FastAPI가 JWT 검증 → 사용자 정보 추출"까지 Docker 환경에서 완성된 상태입니다.**

---

## 다음 단계: Kafka를 연결

이제 이 프로젝트를 처음 목표였던 **Keycloak + Kafka + Iceberg 학습 프로젝트**로 확장하면 자연스럽습니다.

다음 단계에서는 `/usage` API를 하나 추가해서:

```text
사용자 kim
   │
   │ POST /usage
   │
   ▼
FastAPI
   │
   │ JWT 검증
   │
   │ user_id = kim
   │ usage = 100
   ▼
Kafka
   │
   │ topic: usage-events
   ▼
Consumer
```

형태로 만들면 됩니다.

그 다음:

```text
Keycloak
    ↓
FastAPI
    ↓
Kafka
    ↓
Spark Structured Streaming
    ↓
Apache Iceberg
    ↓
MinIO
```

로 확장하면 **인증된 사용자별 사용량을 Kafka로 수집하고 Iceberg에 저장하는 실전형 데이터 파이프라인**이 완성됩니다.
