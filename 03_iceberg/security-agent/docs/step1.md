# Security Agent 실습 현황

## 1. 목표

기존 데이터 플랫폼에 **보안 이벤트 탐지 및 자동 대응 기능**을 추가합니다.

```text
Keycloak / LDAP / FastAPI
          │
          │ Security Event
          ▼
        Kafka
          │
          ▼
   Security Agent
          │
     ┌────┴────┐
     ▼         ▼
  Detector   Risk Engine
                │
                ▼
          Policy Engine
                │
                ▼
         Response Agent
                │
        ┌───────┴───────┐
        ▼               ▼
    Keycloak          Audit
     대응          Iceberg/MinIO
```

핵심 실습 시나리오는:

> **Keycloak 로그인 실패 → Kafka → Brute Force 탐지 → Risk Score → 정책 판단 → 사용자 잠금**

입니다.

---

# 2. Step 1 — Security Event Pipeline

기존 Kafka 데이터 파이프라인과 별도로 보안 이벤트용 토픽을 추가했습니다.

```text
Keycloak
   │
   │ LOGIN_FAILED
   ▼
Kafka
   │
   └── security-events
           │
           ▼
    security-agent
```

Kafka Topic:

```text
security-events
```

Security Agent의 Docker 내부 Kafka 주소:

```text
kafka:9092
```

호스트에서 테스트할 때:

```text
localhost:9092
```

---

# 3. Step 2 — Brute Force Detection

Security Agent 내부에 탐지기를 만들었습니다.

```text
security-events
      │
      ▼
BruteForceDetector
      │
      ▼
LOGIN_FAILED 횟수 계산
```

현재 기본 정책:

```text
5회 이상
60초 이내
같은 username
```

이면 Brute Force 의심 이벤트를 생성합니다.

예:

```text
kim
LOGIN_FAILED
LOGIN_FAILED
LOGIN_FAILED
LOGIN_FAILED
LOGIN_FAILED
        │
        ▼
BRUTE FORCE DETECTED
```

현재 Detector 구조:

```text
security-agent/
├── agent.py
├── models/
│   └── security_event.py
└── detector/
    └── brute_force.py
```

---

# 4. Security Event 구조

현재 이벤트에는 대략 다음 정보가 들어갑니다.

```json
{
  "event_id": "uuid",
  "timestamp": "2026-10-05T...",
  "source": "keycloak",
  "event_type": "LOGIN_FAILED",
  "username": "kim",
  "ip_address": "10.10.10.100",
  "client_id": "usage-api",
  "realm": "hdaic"
}
```

즉 앞으로 Keycloak뿐 아니라

```text
Keycloak
LDAP
FastAPI
Linux
Docker
Kubernetes
```

등에서도 동일한 형태로 Security Event를 Kafka에 전달할 수 있도록 설계했습니다.

---

# 5. Step 3 — Security Agent

Docker Compose에 다음 서비스를 추가했습니다.

```text
security-agent
```

구조:

```text
Kafka
  │
  │ security-events
  ▼
security-agent
  │
  ▼
BruteForceDetector
```

Consumer Group:

```text
security-defense-agent
```

현재 Agent는 이벤트를 받아서 다음과 같이 출력합니다.

```text
[Security Event]
event_id=...
type=LOGIN_FAILED
user=kim
ip=10.10.10.100

[Detector]
user=kim failed_attempts=5

[BRUTE FORCE DETECTED]
user=kim
failed_attempts=5
window=60s
```

---

# 6. Step 4 — Risk Engine

탐지했다고 바로 사용자를 차단하지 않고,

> **얼마나 위험한가?**

를 계산하는 계층을 추가했습니다.

```text
Detector
   │
   ▼
Risk Engine
   │
   ▼
Risk Score
```

현재 예시 정책:

| 조건     |  점수 |
| ------ | --: |
| 5회 실패  | +30 |
| 10회 실패 | +20 |
| 20회 실패 | +20 |
| IP 존재  | +10 |
| 최대     | 100 |

Risk Level:

```text
0 ~ 49   → LOW
50 ~ 79  → MEDIUM
80 ~ 100 → HIGH
```

예:

```text
5회 실패 + IP
     ↓
30 + 10
     ↓
Risk Score = 40
Risk Level = LOW
```

20회 이상이면:

```text
20회 실패 + IP
     ↓
30 + 20 + 20 + 10
     ↓
Risk Score = 80
Risk Level = HIGH
```

---

# 7. Step 5 — Policy Engine

Risk Score를 실제 행동으로 변환합니다.

```text
Risk Engine
     │
     ▼
Policy Engine
     │
     ├── LOG
     ├── ALERT
     └── DISABLE_USER
```

현재 정책:

```text
LOW
 ↓
LOG

MEDIUM
 ↓
ALERT

HIGH
 ↓
DISABLE_USER
```

즉 구조적으로는:

```text
Detection ≠ Response
```

입니다.

이 부분이 Security Agent 설계에서 중요합니다.

---

# 8. 현재는 실제 차단이 아니라 DRY RUN

가장 중요한 현재 상태입니다.

실제 Keycloak 사용자를 바로 disable하지 않습니다.

```text
HIGH
 │
 ▼
DISABLE_USER
 │
 ▼
[DRY RUN]
Would disable user: kim
```

먼저 탐지와 Risk/Policy 판단이 제대로 동작하는지 검증합니다.

그 다음 실제 Keycloak Admin API를 연결합니다.

---

# 9. 현재 Security Agent 전체 구조

현재까지 구현한 구조를 합치면:

```text
                     ┌──────────────┐
                     │   Keycloak   │
                     │ LOGIN_FAILED │
                     └──────┬───────┘
                            │
                            ▼
                     ┌──────────────┐
                     │    Kafka     │
                     │security-events│
                     └──────┬───────┘
                            │
                            ▼
                  ┌───────────────────┐
                  │  Security Agent   │
                  │                   │
                  │  1. Detector      │
                  │        ↓          │
                  │  2. Risk Engine   │
                  │        ↓          │
                  │  3. Policy Engine │
                  └─────────┬─────────┘
                            │
                 ┌──────────┴──────────┐
                 ▼                     ▼
              LOG/ALERT          DISABLE_USER
                                      │
                                  현재 DRY RUN
```

---

# 10. 기존 Data Platform과의 관계

기존 파이프라인은:

```text
Keycloak
   ↓
FastAPI
   ↓
Kafka
   ↓
Spark
   ↓
Iceberg
   ↓
MinIO
```

Security Agent를 추가하면:

```text
                 ┌──────────────┐
                 │   Keycloak   │
                 └──────┬───────┘
                        │
            ┌───────────┴───────────┐
            ▼                       ▼
       Usage Event             Security Event
            │                       │
            ▼                       ▼
          Kafka                   Kafka
            │                       │
            ▼                       ▼
          Spark              Security Agent
            │                       │
            ▼                  Risk / Policy
        Iceberg                     │
            │                       ▼
          MinIO                Keycloak 대응
```

즉 **Data Platform + Security Platform** 형태로 발전시키는 것입니다.

---

# 11. 현재 디렉터리

현재 Security Agent 관련 부분은 대략:

```text
D:\data_platform\03_iceberg
│
├── docker-compose.yml
│
├── api/
├── keycloak/
├── openldap/
├── spark/
├── minio/
│
└── security-agent/
    │
    ├── Dockerfile
    ├── requirements.txt
    ├── agent.py
    │
    ├── models/
    │   ├── __init__.py
    │   └── security_event.py
    │
    └── detector/
        ├── __init__.py
        └── brute_force.py
```

Step 4~5를 실제 파일로 추가하면:

```text
security-agent/
│
├── agent.py
│
├── models/
│   └── security_event.py
│
├── detector/
│   └── brute_force.py
│
└── decision/
    ├── __init__.py
    ├── risk_engine.py
    └── policy_engine.py
```

형태가 됩니다.

---

# 12. 앞으로 진행할 단계

현재 위치는 **Step 1~3 구현 + Step 4~5 설계 단계**입니다.

다음 순서는 다음과 같이 가는 것이 좋습니다.

```text
[완료]
Step 1  Security Event Pipeline
   ↓
[완료]
Step 2  Brute Force Detector
   ↓
[완료]
Step 3  Security Agent
   ↓
[진행]
Step 4  Risk Engine
   ↓
[진행]
Step 5  Policy Engine
   ↓
Step 5.5 Keycloak Admin API
   ↓
Step 6  Keycloak 실제 LOGIN_FAILED 연동
   ↓
Step 7  Security Incident 저장
   ↓
Step 8  Iceberg / MinIO Audit
   ↓
Step 9  Security Dashboard
   ↓
Step 10 AI Security Analyst
```

특히 **Step 5.5 → Step 6**에서 실제 Keycloak과 연결하고, 그 이후에 AI를 붙이는 순서가 안전합니다.

최종적으로는 단순한 `Brute Force Detector`를 넘어,

```text
Detect
  ↓
Correlate
  ↓
Risk
  ↓
Policy
  ↓
Respond
  ↓
Audit
  ↓
Learn
```

하는 **실제 방어형 Security Agent 플랫폼**으로 확장할 수 있습니다.
