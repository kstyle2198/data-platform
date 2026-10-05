# Phase 3. OpenLDAP → Keycloak → FastAPI → Kafka → Spark → Iceberg → MinIO


https://github.com/user-attachments/assets/cd4ae5d6-c878-469c-97ab-01cd920e88f4


Docker Compose 환경에서 **사용자 인증부터 실시간 데이터 저장까지** 하나의 End-to-End Pipeline을 구성한다.

핵심 흐름은 다음과 같다.

```text
OpenLDAP
   ↓ LDAP Federation
Keycloak
   ↓ JWT
FastAPI
   ↓ Usage Event
Kafka
   ↓ Streaming
Spark
   ↓
Apache Iceberg
   ↓
Parquet
   ↓
MinIO
```

---

# 1. Architecture

```text
                         ┌──────────────┐
                         │   OpenLDAP   │
                         │ User         │
                         │ Directory    │
                         └──────┬───────┘
                                │ LDAP
                                ▼
                         ┌──────────────┐
                         │   Keycloak   │
                         │ Authentication│
                         │ Authorization │
                         └──────┬───────┘
                                │ JWT
                                ▼
                         ┌──────────────┐
                         │   FastAPI    │
                         │  Usage API   │
                         │  LDAP Admin  │
                         └──────┬───────┘
                                │
                                │ Usage Event
                                ▼
                    ┌──────────────────────┐
                    │        Kafka         │
                    │    usage-events      │
                    └──────────┬───────────┘
                               │
                               │ Streaming
                               ▼
                    ┌──────────────────────┐
                    │        Spark         │
                    │ Structured Streaming │
                    └──────────┬───────────┘
                               │
                               │ Append
                               ▼
                    ┌──────────────────────┐
                    │   Apache Iceberg     │
                    │ usage_db.usage_events│
                    └──────────┬───────────┘
                               │
                               │ Data Files
                               ▼
                    ┌──────────────────────┐
                    │        MinIO         │
                    │   S3 Object Storage  │
                    └──────────────────────┘
```

Keycloak은 LDAP User Federation을 통해 LDAP 사용자를 인증하고 사용자 정보를 Keycloak 사용자 모델로 연결할 수 있다. LDAP 사용자의 비밀번호 검증은 LDAP 서버에서 수행된다. ([Keycloak][1])

---

# 2. Authentication

OpenLDAP에는 사용자 정보를 저장한다.

```text
OpenLDAP

dc=example,dc=org
└── ou=people
    ├── uid=kim
    └── uid=user01
```

Keycloak은 OpenLDAP와 User Federation으로 연결한다.

```text
Keycloak
└── Realm: hdaic
    └── User Federation
        └── OpenLDAP
```

Docker 내부 연결:

```text
ldap://openldap:389
```

주요 LDAP Attribute:

```text
uid
cn
givenName
sn
mail
departmentNumber
```

예:

```text
uid       → username
cn        → name
givenName → firstName
sn        → lastName
mail      → email
```

LDAP 사용자를 전체 Keycloak 사용자 DB로 동기화할 경우 `Synchronize all users`를 사용할 수 있다. ([Keycloak][1])

---

# 3. Keycloak Role

FastAPI의 LDAP 관리 기능은 `ldap-admin` Role로 보호한다.

```text
kim

Roles
 ├── user
 └── ldap-admin
```

JWT:

```text
realm_access.roles

[
  "user",
  "ldap-admin"
]
```

FastAPI에서는 `ldap-admin`이 없는 사용자의 LDAP 관리 API 접근을 차단한다.

```text
GET  /admin/ldap/users
POST /admin/ldap/users
```

---

# 4. FastAPI

FastAPI의 주요 역할:

```text
1. Keycloak JWT 검증
2. 사용자 정보 조회
3. Usage Event 생성
4. Kafka Event 발행
5. LDAP 사용자 관리
6. Pipeline 상태 조회
7. Iceberg / Parquet 데이터 조회
```

사용자 정보:

```text
GET /me
```

예:

```json
{
  "username": "kim",
  "email": "kim@example.com",
  "name": "Kim User",
  "roles": [
    "user",
    "ldap-admin"
  ]
}
```

---

# 5. Kafka

Usage Event는 Kafka Topic으로 전달한다.

```text
usage-events
```

예:

```json
{
  "event_id": "a123...",
  "event_type": "usage",
  "username": "kim",
  "service": "api",
  "usage_type": "request",
  "quantity": 10,
  "timestamp": "2026-10-05T..."
}
```

Pipeline 상태는 별도의 Topic을 사용한다.

```text
pipeline-status
```

---

# 6. Spark Structured Streaming

Spark는 Kafka의 `usage-events`를 실시간으로 읽는다.

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Iceberg
```

최종 Streaming 프로그램:

```text
spark/3_12_full_pipeline.py
```

실행:

```powershell
docker exec spark `
  /opt/spark/bin/spark-submit `
  /opt/spark/work/3_12_full_pipeline.py
```

---

# 7. Apache Iceberg

Iceberg Table:

```text
local.usage_db.usage_events
```

Iceberg는 단순한 파일 저장소가 아니라 Table의 schema, snapshot, metadata 등을 관리한다.

```text
Iceberg Table

 ├── Schema
 ├── Snapshot
 ├── Metadata
 └── Data Files
        ↓
     Parquet
```

---

# 8. MinIO

MinIO는 S3 호환 Object Storage 역할을 한다.

Bucket:

```text
warehouse
```

구조:

```text
warehouse/
├── usage_db/
│   └── usage_events/
│       ├── metadata/
│       └── data/
│           └── *.parquet
│
└── checkpoints/
    └── full_usage_pipeline/
```

접속:

```text
http://localhost:9002
```

---

# 9. event_id

Phase 3에서는 `event_id`를 이용하여 하나의 이벤트를 전체 Pipeline에서 추적한다.

```text
event_id
   │
   ├── FastAPI
   ├── Kafka
   ├── Spark
   ├── Iceberg
   └── MinIO / Parquet
```

즉:

```text
User
 ↓
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

하나의 이벤트가 전체 구간을 통과하는 것을 확인할 수 있다.

---

# 10. Pipeline Status

FastAPI UI에서는 Pipeline 상태를 확인할 수 있다.

```text
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

예:

```text
FastAPI     COMPLETED
Kafka       COMPLETED
Spark       COMPLETED
Iceberg     COMPLETED
MinIO       COMPLETED
```

---

# 11. Project Structure

```text
03_iceberg/
│
├── docker-compose.yml
├── README.md
│
├── keycloak/
│   └── hdaic-realm.json
│
├── openldap/
│
├── minio/
│   └── Dockerfile
│
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py
│   └── static/
│       └── index.html
│
└── spark/
    ├── Dockerfile
    ├── requirements.txt
    │
    ├── 3_3_minio_init.py
    ├── 3_4_spark_minio.py
    ├── 3_6_spark_iceberg.py
    ├── 3_7_create_table.py
    ├── 3_8_insert_select.py
    ├── 3_9_metadata.py
    ├── 3_10_kafka_stream.py
    ├── 3_11_kafka_to_iceberg.py
    └── 3_12_full_pipeline.py
```

---

# 12. Docker Services

```text
OpenLDAP   :389
Keycloak   :8080
FastAPI    :8000
Kafka      :9092
MinIO      :9000 / 9002
Spark      Structured Streaming
```

서비스 확인:

```powershell
docker compose ps
```

---

# 13. 주요 URL

### FastAPI

```text
http://localhost:8000
```

API 문서:

```text
http://localhost:8000/docs
```

### Keycloak

```text
http://localhost:8080
```

Realm:

```text
hdaic
```

### MinIO

```text
http://localhost:9002
```

---

# 14. 전체 실행

프로젝트 이동:

```powershell
cd D:\data_platform\03_iceberg
```

Build:

```powershell
docker compose build
```

실행:

```powershell
docker compose up -d
```

상태 확인:

```powershell
docker compose ps
```

---

# 15. Pipeline 실행

MinIO 초기화:

```powershell
docker exec spark `
  /opt/spark/bin/spark-submit `
  /opt/spark/work/3_3_minio_init.py
```

Iceberg Table 생성:

```powershell
docker exec spark `
  /opt/spark/bin/spark-submit `
  /opt/spark/work/3_7_create_table.py
```

Full Pipeline:

```powershell
docker exec spark `
  /opt/spark/bin/spark-submit `
  /opt/spark/work/3_12_full_pipeline.py
```

Spark 로그:

```powershell
docker logs -f spark
```

---

# 16. Kafka 확인

Topic 목록:

```powershell
docker exec -it kafka `
  /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:9092 `
  --list
```

주요 Topic:

```text
usage-events
pipeline-status
```

Event 확인:

```powershell
docker exec -it kafka `
  /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:9092 `
  --topic usage-events `
  --from-beginning
```

---

# 17. 핵심 개념

```text
OpenLDAP
 = User Directory

Keycloak
 = Authentication / Authorization

JWT
 = 인증 정보 전달

FastAPI
 = Application API

Kafka
 = Event Stream

Spark
 = Stream Processing

Iceberg
 = Data Lake Table

Parquet
 = Physical Data File

MinIO
 = Object Storage

Checkpoint
 = Streaming 처리 상태

event_id
 = End-to-End 추적 ID
```

---

# 18. 최종 구조

```text
                 Authentication
                 
OpenLDAP
    ↓
Keycloak
    ↓ JWT
FastAPI
    │
    │
    ▼
    Data Pipeline

FastAPI
    ↓
Kafka
    ↓
Spark
    ↓
Iceberg
    ↓
Parquet
    ↓
MinIO
```

## Phase 3 최종 목표

**OpenLDAP에서 사용자를 관리하고, Keycloak으로 인증한 후, FastAPI에서 생성한 Usage Event를 Kafka → Spark → Iceberg → MinIO로 처리하는 End-to-End Data Platform을 Docker Compose 환경에서 구축한다.**

특히 `event_id`를 이용하여 하나의 이벤트가 전체 Pipeline을 통과하는 과정을 확인한다.


