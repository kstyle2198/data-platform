좋습니다. 지금 Phase 2-10의 API는 기능적으로는 동작하지만 **Swagger에서 직접 호출하는 개발용 API**에 가깝습니다.

이번에는 간단한 웹 UI를 붙여서 다음처럼 만들어보겠습니다.

```text
┌──────────────────────────────────────────────┐
│          Usage Management System             │
├──────────────────────────────────────────────┤
│                                              │
│  Username [ kim       ]                     │
│  Password [ ********  ]      [ Login ]       │
│                                              │
├──────────────────────────────────────────────┤
│  👤 Logged in: kim                           │
│                                              │
│  Service       [ api       ▼ ]              │
│  Usage Type    [ request   ▼ ]              │
│  Quantity      [ 10          ]              │
│                              [ Submit Usage ] │
│                                              │
├──────────────────────────────────────────────┤
│  Last Event                                   │
│  ------------------------------------------  │
│  User      : kim                             │
│  Service   : api                             │
│  Quantity  : 10                              │
│  Partition : 1                               │
│  Offset    : 25                              │
└──────────────────────────────────────────────┘
```

그리고 전체 구조는:

```text
Browser
   │
   │ Login
   ▼
FastAPI
   │
   │ username/password
   ▼
Keycloak
   │
   │ JWT
   ▼
FastAPI
   │
   │ Usage Event
   ▼
Kafka
   │
   ▼
Consumer
```

로 만들어보겠습니다.

---

# 1. 이번 단계에서 추가할 기능

이번에는 크게 5가지를 추가합니다.

### ① 로그인 화면

```text
Username
Password
[Login]
```

### ② Keycloak 인증

FastAPI가 Keycloak에 인증 요청을 합니다.

### ③ 로그인 사용자 표시

```text
Logged in: kim
```

### ④ 사용량 등록

```text
Service
Usage Type
Quantity

[Submit Usage]
```

### ⑤ Kafka 전송 결과 표시

```text
Kafka Topic : usage-events
Partition   : 1
Offset      : 27
```

즉 **실제 사용자가 서비스를 사용하고 사용량을 등록하는 것처럼** 보이게 합니다.

---

# 2. 프로젝트 구조 변경

현재:

```text
keycloak-lab/
│
├── docker-compose.yml
│
├── keycloak/
│   └── hdaic-realm.json
│
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
│
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py
│
└── consumer/
    ├── Dockerfile
    ├── requirements.txt
    └── consumer.py
```

여기에:

```text
api/
├── Dockerfile
├── requirements.txt
├── main.py
└── static/
    └── index.html
```

을 추가합니다.

---

# 3. FastAPI에서 HTML 서비스

FastAPI가:

```text
http://localhost:8000/
```

으로 UI를 제공하게 합니다.

그리고 API는 그대로:

```text
http://localhost:8000/me
http://localhost:8000/usage
```

를 사용합니다.

따라서 별도의 React/Vue 서버를 만들지 않습니다.

**이번 학습에서는 HTML + JavaScript만 사용하겠습니다.**

이렇게 하면 구조를 이해하기 쉽습니다.

---

# 4. `main.py` 수정

먼저 import를 추가합니다.

```python
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
```

그리고 Keycloak Token Endpoint도 추가합니다.

```python
KEYCLOAK_TOKEN_URL = (
    f"{REALM_INTERNAL_URL}"
    f"/protocol/openid-connect/token"
)
```

---

# 5. Login API 추가

FastAPI가 Keycloak에 로그인 요청을 전달합니다.

```python
class LoginRequest(BaseModel):

    username: str = Field(
        min_length=1
    )

    password: str = Field(
        min_length=1
    )
```

그리고:

```python
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

    try:

        async with httpx.AsyncClient() as client:

            response = await client.post(
                KEYCLOAK_TOKEN_URL,
                data=data,
                timeout=5.0
            )

        if response.status_code != 200:

            raise HTTPException(
                status_code=401,
                detail="Invalid username or password"
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

        raise HTTPException(
            status_code=503,
            detail=(
                "Unable to connect to "
                f"Keycloak: {str(e)}"
            )
        )
```

이제 브라우저가:

```text
POST /login
```

을 호출하면 FastAPI가 Keycloak에 인증합니다.

---

# 6. UI 파일 생성

다음 파일을 만드세요.

```text
api/static/index.html
```

전체 코드는 다음과 같습니다.

```html
<!DOCTYPE html>
<html lang="ko">

<head>

    <meta charset="UTF-8">

    <meta
        name="viewport"
        content="width=device-width, initial-scale=1.0"
    >

    <title>Usage Management System</title>

    <style>

        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            font-family:
                Arial,
                sans-serif;

            background: #f4f6f8;
            color: #222;
        }

        .container {
            width: 900px;
            max-width: 95%;

            margin: 40px auto;
        }

        .header {
            background: #1f2937;
            color: white;

            padding: 24px;

            border-radius: 12px 12px 0 0;
        }

        .header h1 {
            margin: 0;
            font-size: 24px;
        }

        .header p {
            margin:
                8px 0 0 0;

            color: #d1d5db;
        }

        .card {
            background: white;

            padding: 24px;

            margin-top: 16px;

            border-radius: 12px;

            box-shadow:
                0 2px 8px
                rgba(0,0,0,0.08);
        }

        .card h2 {
            margin-top: 0;
        }

        .form-group {
            margin-bottom: 16px;
        }

        label {
            display: block;

            margin-bottom: 6px;

            font-weight: bold;
        }

        input,
        select {

            width: 100%;

            padding: 10px;

            border: 1px solid #d1d5db;

            border-radius: 6px;

            font-size: 15px;
        }

        button {

            border: none;

            padding: 11px 18px;

            border-radius: 6px;

            background: #2563eb;

            color: white;

            font-size: 15px;

            cursor: pointer;
        }

        button:hover {
            background: #1d4ed8;
        }

        button.logout {
            background: #6b7280;
        }

        .user-info {

            display: flex;

            justify-content:
                space-between;

            align-items: center;

            background: #eff6ff;

            padding: 14px;

            border-radius: 8px;
        }

        .hidden {
            display: none;
        }

        .status {

            margin-top: 15px;

            padding: 12px;

            border-radius: 6px;

            background: #f3f4f6;
        }

        .success {
            background: #ecfdf5;
            color: #065f46;
        }

        .error {
            background: #fef2f2;
            color: #991b1b;
        }

        .event {

            background: #111827;

            color: #e5e7eb;

            padding: 18px;

            border-radius: 8px;

            font-family:
                Consolas,
                monospace;

            white-space: pre-wrap;
        }

        .login-card {
            max-width: 500px;

            margin: 40px auto;
        }

        .login-card h2 {
            text-align: center;
        }

    </style>

</head>


<body>


<div class="container">


    <!-- ================================================= -->
    <!-- Header -->
    <!-- ================================================= -->

    <div class="header">

        <h1>
            Usage Management System
        </h1>

        <p>
            Keycloak + FastAPI + Kafka
        </p>

    </div>


    <!-- ================================================= -->
    <!-- Login -->
    <!-- ================================================= -->

    <div
        id="loginSection"
        class="card login-card"
    >

        <h2>
            Login
        </h2>


        <div class="form-group">

            <label>
                Username
            </label>

            <input
                id="username"
                type="text"
                value="kim"
                placeholder="Username"
            >

        </div>


        <div class="form-group">

            <label>
                Password
            </label>

            <input
                id="password"
                type="password"
                value="test1234"
                placeholder="Password"
            >

        </div>


        <button
            onclick="login()"
        >
            Login
        </button>


        <div
            id="loginStatus"
            class="status hidden"
        ></div>

    </div>


    <!-- ================================================= -->
    <!-- Main Application -->
    <!-- ================================================= -->

    <div
        id="appSection"
        class="hidden"
    >


        <!-- User -->

        <div class="card">

            <div class="user-info">

                <div>

                    👤 Logged in:

                    <strong
                        id="loggedUser"
                    >
                    </strong>

                </div>


                <button
                    class="logout"
                    onclick="logout()"
                >
                    Logout
                </button>

            </div>

        </div>


        <!-- Usage -->

        <div class="card">

            <h2>
                Register Usage
            </h2>


            <div class="form-group">

                <label>
                    Service
                </label>

                <select id="service">

                    <option value="api">
                        API
                    </option>

                    <option value="search">
                        Search
                    </option>

                    <option value="storage">
                        Storage
                    </option>

                    <option value="llm">
                        LLM
                    </option>

                </select>

            </div>


            <div class="form-group">

                <label>
                    Usage Type
                </label>

                <select id="usageType">

                    <option value="request">
                        Request
                    </option>

                    <option value="token">
                        Token
                    </option>

                    <option value="gb">
                        GB
                    </option>

                    <option value="minute">
                        Minute
                    </option>

                </select>

            </div>


            <div class="form-group">

                <label>
                    Quantity
                </label>

                <input
                    id="quantity"
                    type="number"
                    value="10"
                    min="1"
                >

            </div>


            <button
                onclick="submitUsage()"
            >
                Submit Usage
            </button>


            <div
                id="usageStatus"
                class="status hidden"
            ></div>

        </div>


        <!-- Last Event -->

        <div class="card">

            <h2>
                Last Kafka Event
            </h2>

            <div
                id="eventResult"
                class="event"
            >
No event yet.
            </div>

        </div>


    </div>


</div>


<script>


// ========================================================
// Token
// ========================================================

let accessToken =
    localStorage.getItem(
        "access_token"
    );


// ========================================================
// Login
// ========================================================

async function login() {

    const username =
        document
            .getElementById(
                "username"
            )
            .value;

    const password =
        document
            .getElementById(
                "password"
            )
            .value;


    const status =
        document
            .getElementById(
                "loginStatus"
            );


    status.className =
        "status";

    status.innerText =
        "Logging in...";


    try {

        const response =
            await fetch(
                "/login",
                {

                    method: "POST",

                    headers: {
                        "Content-Type":
                            "application/json"
                    },

                    body: JSON.stringify({
                        username:
                            username,

                        password:
                            password
                    })
                }
            );


        const data =
            await response.json();


        if (!response.ok) {

            throw new Error(
                data.detail ||
                "Login failed"
            );
        }


        accessToken =
            data.access_token;


        localStorage.setItem(
            "access_token",
            accessToken
        );


        await loadUser();


    } catch (error) {

        status.className =
            "status error";

        status.innerText =
            error.message;
    }
}


// ========================================================
// Load User
// ========================================================

async function loadUser() {

    try {

        const response =
            await fetch(
                "/me",
                {

                    headers: {
                        "Authorization":
                            "Bearer " +
                            accessToken
                    }
                }
            );


        if (!response.ok) {

            throw new Error(
                "Token verification failed"
            );
        }


        const user =
            await response.json();


        document
            .getElementById(
                "loggedUser"
            )
            .innerText =
                user.username;


        document
            .getElementById(
                "loginSection"
            )
            .classList
            .add("hidden");


        document
            .getElementById(
                "appSection"
            )
            .classList
            .remove("hidden");


    } catch (error) {

        logout();

    }
}


// ========================================================
// Submit Usage
// ========================================================

async function submitUsage() {

    const service =
        document
            .getElementById(
                "service"
            )
            .value;


    const usageType =
        document
            .getElementById(
                "usageType"
            )
            .value;


    const quantity =
        Number(
            document
                .getElementById(
                    "quantity"
                )
                .value
        );


    const status =
        document
            .getElementById(
                "usageStatus"
            );


    status.className =
        "status";

    status.innerText =
        "Sending usage event...";


    try {

        const response =
            await fetch(
                "/usage",
                {

                    method: "POST",

                    headers: {

                        "Authorization":
                            "Bearer " +
                            accessToken,

                        "Content-Type":
                            "application/json"
                    },

                    body: JSON.stringify({

                        service:
                            service,

                        usage_type:
                            usageType,

                        quantity:
                            quantity
                    })
                }
            );


        const data =
            await response.json();


        if (!response.ok) {

            throw new Error(
                data.detail ||
                "Usage submission failed"
            );
        }


        status.className =
            "status success";

        status.innerText =
            "Usage event sent to Kafka.";


        document
            .getElementById(
                "eventResult"
            )
            .innerText =
                JSON.stringify(
                    data,
                    null,
                    2
                );


    } catch (error) {

        status.className =
            "status error";

        status.innerText =
            error.message;
    }
}


// ========================================================
// Logout
// ========================================================

function logout() {

    accessToken = null;

    localStorage.removeItem(
        "access_token"
    );


    document
        .getElementById(
            "appSection"
        )
        .classList
        .add("hidden");


    document
        .getElementById(
            "loginSection"
        )
        .classList
        .remove("hidden");
}


// ========================================================
// Page Load
// ========================================================

if (accessToken) {

    loadUser();

}


</script>


</body>

</html>
```

---

# 7. `main.py`에 UI 연결

`main.py` 마지막 부분에 다음을 추가합니다.

```python
# ============================================================
# Web UI
# ============================================================

@app.get("/")
async def index():

    return FileResponse(
        "static/index.html"
    )
```

그리고 `api/Dockerfile`에서 `static` 디렉터리를 복사하도록 변경합니다.

기존:

```dockerfile
COPY main.py .
```

변경:

```dockerfile
COPY main.py .
COPY static ./static
```

전체 Dockerfile은:

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY static ./static

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

---

# 8. 최종 `api` 구조

이제:

```text
api/
│
├── Dockerfile
│
├── requirements.txt
│
├── main.py
│
└── static/
    └── index.html
```

입니다.

---

# 9. 컨테이너 재빌드

반드시 `keycloak-lab` 디렉터리에서:

```powershell
docker compose build api
```

그리고:

```powershell
docker compose up -d
```

확인:

```powershell
docker compose ps
```

---

# 10. 브라우저에서 접속

이제 Swagger가 아니라:

```text
http://localhost:8000
```

으로 접속합니다.

로그인:

```text
Username: kim
Password: test1234
```

그리고:

```text
Login
```

을 클릭합니다.

---

# 11. 로그인하면

Keycloak 인증이 성공하면 화면이:

```text
┌──────────────────────────────────────┐
│ Usage Management System              │
│ Keycloak + FastAPI + Kafka           │
├──────────────────────────────────────┤
│ 👤 Logged in: kim             Logout │
├──────────────────────────────────────┤
│ Register Usage                       │
│                                      │
│ Service                              │
│ [ API                         ▼ ]    │
│                                      │
│ Usage Type                           │
│ [ Request                     ▼ ]    │
│                                      │
│ Quantity                             │
│ [ 10                          ]       │
│                                      │
│             [ Submit Usage ]         │
└──────────────────────────────────────┘
```

으로 변경됩니다.

---

# 12. Submit Usage

예를 들어:

```text
Service    : API
Usage Type : Request
Quantity   : 10
```

으로 입력하고:

```text
Submit Usage
```

을 누릅니다.

그러면:

```text
Browser
   │
   │ POST /usage
   │ Authorization: Bearer JWT
   ▼
FastAPI
   │
   ├── JWT 검증
   │
   ├── user_id = JWT.sub
   │
   ├── username = JWT.preferred_username
   │
   └── event 생성
   │
   ▼
Kafka
   │
   └── usage-events
```

가 실행됩니다.

---

# 13. 화면에 Kafka 결과 표시

예를 들어:

```json
{
  "message": "Usage event sent successfully",
  "topic": "usage-events",
  "partition": 1,
  "offset": 15,
  "event": {
    "event_type": "usage",
    "user_id": "xxxx-xxxx",
    "username": "kim",
    "service": "api",
    "usage_type": "request",
    "quantity": 10,
    "timestamp": "2026-09-26T07:20:31+00:00"
  }
}
```

가 UI에 표시됩니다.

즉 단순히:

> "Kafka에 메시지가 들어갔다."

가 아니라 사용자가 **실제로 서비스를 사용하는 것처럼** 보입니다.

---

# 14. Consumer에서도 동시에 확인

Consumer 터미널에서는:

```text
================================
Received event
================================

topic     : usage-events
partition : 1
offset    : 15

user_id   : xxxx-xxxx
username  : kim
event_type: usage
service   : api
usage_type: request
quantity  : 10
```

이렇게 나옵니다.

따라서 **UI와 Kafka Consumer를 동시에 보면서 전체 흐름을 확인**할 수 있습니다.

---

# 15. 현재 시스템의 전체 모습

이제 상당히 재미있는 구조가 됩니다.

```text
                 ┌─────────────────┐
                 │    Browser      │
                 │                 │
                 │ Usage UI        │
                 └────────┬────────┘
                          │
                          │ Login
                          ▼
                 ┌─────────────────┐
                 │    FastAPI      │
                 │                 │
                 │ /login          │
                 │ /me             │
                 │ /usage          │
                 └───────┬─────────┘
                         │
             ┌───────────┴───────────┐
             │                       │
             ▼                       ▼
      ┌─────────────┐          ┌─────────────┐
      │  Keycloak   │          │    Kafka    │
      │             │          │             │
      │ JWT         │          │usage-events │
      └─────────────┘          └──────┬──────┘
                                      │
                                      ▼
                               ┌─────────────┐
                               │  Consumer   │
                               └─────────────┘
```

---

# 16. 그런데 한 가지 중요한 점

현재 로그인 방식은 **학습용으로는 좋지만 실제 운영 서비스에서는 개선해야 합니다.**

현재:

```text
Browser
   │
   │ username/password
   ▼
FastAPI
   │
   │ username/password
   ▼
Keycloak
```

입니다.

실제 서비스에서는 일반적으로:

```text
Browser
   │
   ▼
Keycloak Login
   │
   │ Authorization Code
   ▼
Browser
   │
   │ Access Token
   ▼
FastAPI
```

형태인 **OIDC Authorization Code Flow**를 사용합니다.

즉 이번 구현은:

> **Phase 2 학습용 간단한 UI**

로 생각하면 됩니다.

---

## Phase 2 최종 완성 구조

지금까지 학습한 내용을 합치면:

```text
             ┌──────────────┐
             │   Keycloak   │
             │              │
             │ kim          │
             └──────┬───────┘
                    │
                    │ JWT
                    ▼
┌─────────────────────────────────┐
│             FastAPI             │
│                                 │
│  Web UI                         │
│  /login                         │
│  /me                            │
│  /usage                         │
│                                 │
│  JWT 검증                       │
│  사용자 정보 추출               │
│  Usage Event 생성               │
└───────────────┬─────────────────┘
                │
                │ JSON Event
                │ key = user_id
                ▼
       ┌───────────────────┐
       │       Kafka       │
       │                   │
       │   usage-events    │
       │                   │
       │ P0 │ P1 │ P2      │
       └─────────┬─────────┘
                 │
                 ▼
          ┌─────────────┐
          │  Consumer   │
          └─────────────┘
```

이제 **Phase 2의 실습 목적이 거의 완성**됐습니다.

다음 단계인 **Phase 3에서는 이 Kafka의 `usage-events`를 실제 데이터 저장소로 보내기 위해 `MinIO + Apache Iceberg + Spark`를 붙이는 구조**로 넘어가면 됩니다.
