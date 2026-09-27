# Phase 3. Kafka → Spark → Iceberg → MinIO Data Pipeline



https://github.com/user-attachments/assets/cd4ae5d6-c878-469c-97ab-01cd920e88f4



Docker Compose 환경에서 **Kafka → Spark Structured Streaming → Apache Iceberg → MinIO**로 이어지는 데이터 처리 파이프라인을 구축하고 실습한다.

Phase 3의 핵심 목표는 Kafka로 들어온 사용량 이벤트가 Spark에서 처리되고, Apache Iceberg 테이블로 저장되며, 실제 데이터 파일인 Parquet가 MinIO Object Storage에 생성되는 전체 과정을 확인하는 것이다.

---

## 1. Architecture

```text
                         ┌──────────────┐
                         │   Keycloak   │
                         │ Authentication
                         └──────┬───────┘
                                │ JWT
                                ▼
                         ┌──────────────┐
                         │   FastAPI    │
                         │ Usage API    │
                         └──────┬───────┘
                                │
                                │ usage event
                                ▼
                    ┌──────────────────────┐
                    │        Kafka         │
                    │    usage-events      │
                    └──────────┬───────────┘
                               │
                               │ Structured Streaming
                               ▼
                    ┌──────────────────────┐
                    │        Spark         │
                    │ Structured Streaming │
                    └──────────┬───────────┘
                               │
                               │ append
                               ▼
                    ┌──────────────────────┐
                    │   Apache Iceberg     │
                    │ usage_db.usage_events│
                    └──────────┬───────────┘
                               │
                               │ Parquet
                               ▼
                    ┌──────────────────────┐
                    │        MinIO         │
                    │   S3 Object Storage  │
                    └──────────────────────┘
```

전체 데이터 흐름:

```text
User
  ↓
Keycloak Login
  ↓
JWT
  ↓
FastAPI
  ↓
Kafka : usage-events
  ↓
Spark Structured Streaming
  ↓
Apache Iceberg Table
  ↓
Parquet Files
  ↓
MinIO warehouse bucket
```

---

# 2. Phase 3 핵심 학습 내용

Phase 3에서는 다음 개념을 연결해서 학습한다.

| 기술                   | 역할                     |
| -------------------- | ---------------------- |
| Kafka                | 실시간 이벤트 스트림            |
| Spark                | 대용량 데이터 처리 / Streaming |
| Structured Streaming | Kafka 실시간 데이터 처리       |
| Apache Iceberg       | 데이터 레이크 테이블 관리         |
| Parquet              | 실제 데이터 저장 파일           |
| MinIO                | S3 호환 Object Storage   |
| Checkpoint           | Streaming 처리 상태 저장     |
| FastAPI              | Pipeline 상태 및 결과 조회    |

핵심 관계는 다음과 같다.

```text
Kafka
  │
  │ Event Stream
  ▼
Spark
  │
  │ Data Processing
  ▼
Iceberg
  │
  │ Table Metadata
  ▼
Parquet
  │
  │ Physical Data
  ▼
MinIO
```

---

# 3. Project Structure

```text
03_iceberg/
│
├── docker-compose.yml
├── README.md
│
├── keycloak/
│   └── hdaic-realm.json
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

# 4. Docker Services

Docker Compose로 다음 서비스를 실행한다.

```text
┌─────────────┐
│  Keycloak   │ :8080
└─────────────┘

┌─────────────┐
│   FastAPI   │ :8000
└─────────────┘

┌─────────────┐
│    Kafka    │ :9092
└─────────────┘

┌─────────────┐
│    MinIO    │
│ API :9000   │
│ Console:9002│
└─────────────┘

┌─────────────┐
│    Spark    │
│ Structured  │
│ Streaming   │
└─────────────┘
```

---

# 5. 실행 환경

Windows + Docker Desktop 기준.

프로젝트 위치:

```powershell
D:\data_platform\03_iceberg
```

프로젝트 디렉터리로 이동한다.

```powershell
cd D:\data_platform\03_iceberg
```

---

# 6. 전체 서비스 실행

처음 실행하거나 이미지가 변경된 경우:

```powershell
docker compose build
```

전체 서비스를 실행한다.

```powershell
docker compose up -d
```

실행 상태 확인:

```powershell
docker compose ps
```

또는:

```powershell
docker ps
```

정상적으로 다음 컨테이너가 실행되어야 한다.

```text
keycloak
kafka
kafka-init
api
minio
spark
```

---

# 7. 주요 접속 URL

## FastAPI

```text
http://localhost:8000
```

API 문서:

```text
http://localhost:8000/docs
```

---

## Keycloak

```text
http://localhost:8080
```

Realm:

```text
hdaic
```

---

## MinIO

MinIO Console:

```text
http://localhost:9002
```

로그인:

```text
Username:
minioadmin

Password:
minioadmin123
```

S3 API:

```text
http://localhost:9000
```

---

# 16. Phase 3-12. Full Pipeline

Phase 3의 최종 실습이다.

목표:

```text
FastAPI
   ↓
Kafka
   ↓
Spark Structured Streaming
   ↓
Iceberg
   ↓
MinIO
   ↓
Parquet
   ↓
FastAPI UI
```

실행:

```powershell
docker exec spark `
  /opt/spark/bin/spark-submit `
  /opt/spark/work/3_12_full_pipeline.py
```

최종적으로 Spark Streaming 프로세스가 계속 실행된다.

확인:

```powershell
docker logs -f spark
```

---

# 17. Full Pipeline의 event_id 추적

전체 Pipeline에서는 하나의 사용량 이벤트를 `event_id`로 추적한다.

예:

```text
event_id
  │
  ├── FastAPI
  │
  ├── Kafka
  │
  ├── Spark
  │
  ├── Iceberg
  │
  └── MinIO / Parquet
```

예시:

```json
{
  "event_id": "a123...",
  "event_type": "usage",
  "user_id": "keycloak-user-id",
  "username": "kim",
  "service": "api",
  "usage_type": "request",
  "quantity": 10,
  "timestamp": "2026-09-27T..."
}
```

이 `event_id`가 전체 파이프라인을 연결하는 핵심 식별자이다.

---

# 18. Pipeline Status

FastAPI는 `pipeline-status` Kafka topic을 이용하여 각 단계의 처리 상태를 확인한다.

```text
pipeline-status
       │
       ▼
    FastAPI
       │
       ▼
      UI
```

UI에서는 다음 단계의 상태를 확인할 수 있다.

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
FastAPI    COMPLETED
Kafka      COMPLETED
Spark      COMPLETED
Iceberg    COMPLETED
MinIO      COMPLETED
```

---

# 19. Parquet 실제 데이터 확인

Iceberg의 실제 데이터는 Parquet 파일로 MinIO에 저장된다.

MinIO 구조 예:

```text
warehouse
└── usage_db
    └── usage_events
        ├── metadata
        └── data
            ├── 00000-....parquet
            ├── 00001-....parquet
            └── ...
```

FastAPI UI의:

```text
4. Iceberg / Parquet Data
```

영역에서 실제 Parquet 데이터를 조회할 수 있다.

예:

```text
event_id     username   service   usage_type   quantity
--------------------------------------------------------
a123...      kim        api       request      10
```

이를 통해:

```text
Kafka Event
     ↓
Iceberg Table
     ↓
Physical Parquet File
```

관계를 직접 확인할 수 있다.

---

# 20. MinIO에서 데이터 확인

브라우저:

```text
http://localhost:9002
```

로그인:

```text
minioadmin
minioadmin123
```

Bucket:

```text
warehouse
```

Iceberg 데이터 경로:

```text
usage_db/
└── usage_events/
    ├── metadata/
    └── data/
```

Streaming checkpoint:

```text
checkpoints/
└── ...
```

---

# 21. Kafka Topic 확인

Kafka 컨테이너 접속:

```powershell
docker exec -it kafka bash
```

Topic 목록:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list
```

예상:

```text
usage-events
pipeline-status
```

Topic 상세:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic usage-events
```

---

# 22. Kafka Event 직접 확인

Consumer를 실행한다.

```powershell
docker exec -it kafka `
  /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:9092 `
  --topic usage-events `
  --from-beginning
```

FastAPI에서 Usage Event를 생성하면 Kafka에 JSON 이벤트가 들어온다.

예:

```json
{
  "event_id": "a123...",
  "event_type": "usage",
  "user_id": "...",
  "username": "kim",
  "service": "api",
  "usage_type": "request",
  "quantity": 10,
  "timestamp": "..."
}
```

---

# 23. Spark 로그 확인

Spark Streaming 상태:

```powershell
docker logs -f spark
```

Spark 컨테이너 상태:

```powershell
docker ps
```

Spark Streaming 프로세스가 정상적으로 실행 중인지 확인한다.

---

# 24. Full Pipeline 실행 순서

처음부터 전체 실습을 실행하는 경우:

### Step 1. 프로젝트 이동

```powershell
cd D:\data_platform\03_iceberg
```

### Step 2. 이미지 Build

```powershell
docker compose build
```

### Step 3. 서비스 실행

```powershell
docker compose up -d
```

### Step 4. 상태 확인

```powershell
docker compose ps
```

### Step 5. MinIO Bucket 생성

```powershell
docker exec spark `
  /opt/spark/bin/spark-submit `
  /opt/spark/work/3_3_minio_init.py
```

### Step 6. Iceberg Table 생성

```powershell
docker exec spark `
  /opt/spark/bin/spark-submit `
  /opt/spark/work/3_7_create_table.py
```

### Step 7. Full Streaming Pipeline 실행

```powershell
docker exec spark `
  /opt/spark/bin/spark-submit `
  /opt/spark/work/3_12_full_pipeline.py
```

### Step 8. Spark 로그 확인

```powershell
docker logs -f spark
```

### Step 9. FastAPI 접속

```text
http://localhost:8000
```

### Step 10. Usage Event 생성

UI에서 로그인 후 Usage Event를 생성한다.

---

# 25. Spark 자동 실행 방식

최종 Docker Compose에서는 Spark 컨테이너가 단순히 실행 상태만 유지하는 것이 아니라 Streaming 애플리케이션을 자동으로 실행하도록 구성할 수 있다.

```yaml
spark:
  build:
    context: ./spark
    dockerfile: Dockerfile

  image: local/spark-iceberg:3.5.7

  container_name: spark

  volumes:
    - ./spark:/opt/spark/work

  command:
    - /opt/spark/bin/spark-submit
    - /opt/spark/work/3_12_full_pipeline.py

  depends_on:
    kafka:
      condition: service_healthy
    minio:
      condition: service_started

  restart: unless-stopped
```

이렇게 구성하면:

```text
docker compose up -d
        ↓
Spark Container Start
        ↓
spark-submit
        ↓
3_12_full_pipeline.py
        ↓
Streaming 계속 실행
```

따라서 별도로 다음 명령을 실행할 필요가 없다.

```powershell
docker exec spark spark-submit ...
```

---

# 26. Checkpoint 주의사항

Structured Streaming은 checkpoint를 사용한다.

예:

```text
s3a://warehouse/checkpoints/full_usage_pipeline
```

코드를 크게 변경한 후 기존 checkpoint와 충돌하는 경우 새로운 경로를 사용한다.

예:

```text
s3a://warehouse/checkpoints/full_usage_pipeline_v2
```

또는 실습 환경에서는 MinIO의 기존 checkpoint를 삭제하고 다시 시작할 수 있다.

Checkpoint는 다음 정보를 기억한다.

```text
Kafka Offset
      ↓
Spark가 어디까지 읽었는가?
      ↓
다음 실행 시 어디부터 이어서 읽을 것인가?
```

---

# 27. Spark Java Version

Apache Iceberg 라이브러리와 Spark 환경의 Java 버전을 맞추는 것이 중요하다.

현재 Spark 컨테이너는 Java 17을 사용한다.

확인:

```powershell
docker exec spark java -version
```

예:

```text
openjdk version "17.0.15"
```

Iceberg에서 다음 오류가 발생할 경우:

```text
UnsupportedClassVersionError

class file version 61.0
this version only recognizes class file versions up to 55.0
```

Java 11 runtime으로 Iceberg Java 17 class를 실행하려는 상황이다.

따라서 Spark Dockerfile에서 Java 17을 사용하도록 구성한다.

---

# 28. 핵심 데이터 저장 구조

Phase 3에서 가장 중요한 개념이다.

## Kafka

Kafka는 이벤트를 저장한다.

```text
usage-events
   │
   ├── event 1
   ├── event 2
   ├── event 3
   └── ...
```

## Spark

Spark는 Kafka 이벤트를 읽고 처리한다.

```text
Kafka
  ↓
Spark DataFrame
```

## Iceberg

Iceberg는 Data Lake Table을 관리한다.

```text
Iceberg Table
 ├── Schema
 ├── Snapshot
 ├── History
 └── Data Files
```

## Parquet

실제 데이터는 Parquet 파일에 저장된다.

```text
Parquet
 ├── columnar data
 ├── event_id
 ├── username
 ├── service
 └── quantity
```

## MinIO

Parquet와 Iceberg metadata/checkpoint가 Object Storage에 저장된다.

```text
MinIO
└── warehouse
    ├── usage_db
    │   └── usage_events
    │       ├── metadata
    │       └── data
    │           └── *.parquet
    │
    └── checkpoints
```

---

# 29. 전체 프로세스 한눈에 보기

```text
                    ┌─────────────┐
                    │  Keycloak   │
                    └──────┬──────┘
                           │ JWT
                           ▼
                    ┌─────────────┐
                    │   FastAPI   │
                    └──────┬──────┘
                           │
                           │ event_id
                           ▼
                 ┌───────────────────┐
                 │      Kafka        │
                 │   usage-events    │
                 └────────┬──────────┘
                          │
                          │ Streaming
                          ▼
                 ┌───────────────────┐
                 │      Spark        │
                 │ Structured        │
                 │ Streaming         │
                 └────────┬──────────┘
                          │
                          │ append
                          ▼
                 ┌───────────────────┐
                 │ Apache Iceberg    │
                 │ usage_events      │
                 └────────┬──────────┘
                          │
                          │ physical files
                          ▼
                 ┌───────────────────┐
                 │      MinIO        │
                 │ S3 Object Storage │
                 └────────┬──────────┘
                          │
                          ▼
                    *.parquet


Pipeline Status

Kafka
  │
  │ pipeline-status
  ▼
FastAPI
  │
  ▼
Web UI

FastAPI → Kafka → Spark → Iceberg → MinIO
   │         │       │        │        │
   └─────────┴───────┴────────┴────────┘
                 event_id
```

---

# 30. Phase 3에서 반드시 이해할 개념

### ① Kafka ≠ Database

Kafka는 주로 이벤트 스트림과 로그를 관리한다.

```text
Producer
   ↓
Kafka Topic
   ↓
Consumer
```

---

### ② Spark ≠ Storage

Spark는 데이터를 처리하는 Engine이다.

```text
Kafka → Spark → Storage
```

---

### ③ Iceberg ≠ Parquet

둘은 역할이 다르다.

```text
Iceberg
  = Table Management

Parquet
  = Physical Data File
```

즉:

```text
Iceberg Table
      │
      ├── Metadata
      ├── Snapshot
      ├── History
      │
      └── Parquet Files
```

---

### ④ MinIO ≠ Iceberg

MinIO는 실제 Object Storage이고 Iceberg는 그 위에서 Table을 관리한다.

```text
             Iceberg
                │
                ▼
             MinIO
                │
        ┌───────┴────────┐
        ▼                ▼
    Metadata          Parquet
```

---

### ⑤ Checkpoint

Streaming에서 처리 위치를 기억한다.

```text
Kafka Offset
     ↓
Checkpoint
     ↓
Spark Restart
     ↓
이어서 처리
```

---

# 31. Phase 3 최종 목표

Phase 3의 최종 목표는 단순히 Docker 컨테이너를 실행하는 것이 아니다.

다음 데이터 파이프라인을 직접 구성하고 이해하는 것이다.

```text
             Real-time Data Pipeline

                 User Request
                       │
                       ▼
                    FastAPI
                       │
                       ▼
                     Kafka
                       │
                       ▼
              Spark Structured
                 Streaming
                       │
                       ▼
                  Iceberg
                       │
                       ▼
                   Parquet
                       │
                       ▼
                    MinIO
                       │
                       ▼
              Data Lake Storage
```

그리고 `event_id`를 이용하여 하나의 이벤트가:

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
   ↓
Parquet
```

전체 구간을 통과하는 과정을 UI에서 확인하는 것이 Phase 3의 핵심 실습이다.

---

# 32. 주요 명령어 요약

### 전체 시작

```powershell
cd D:\data_platform\03_iceberg

docker compose build

docker compose up -d
```

### 상태 확인

```powershell
docker compose ps
```

### 로그

```powershell
docker logs -f spark
```

```powershell
docker logs -f api
```

```powershell
docker logs -f kafka
```

### MinIO Bucket

```powershell
docker exec spark `
  /opt/spark/bin/spark-submit `
  /opt/spark/work/3_3_minio_init.py
```

### Spark → MinIO

```powershell
docker exec spark `
  /opt/spark/bin/spark-submit `
  /opt/spark/work/3_4_spark_minio.py
```

### Iceberg Table 생성

```powershell
docker exec spark `
  /opt/spark/bin/spark-submit `
  /opt/spark/work/3_7_create_table.py
```

### Iceberg INSERT / SELECT

```powershell
docker exec spark `
  /opt/spark/bin/spark-submit `
  /opt/spark/work/3_8_insert_select.py
```

### Iceberg Metadata

```powershell
docker exec spark `
  /opt/spark/bin/spark-submit `
  /opt/spark/work/3_9_metadata.py
```

### Kafka Streaming

```powershell
docker exec spark `
  /opt/spark/bin/spark-submit `
  /opt/spark/work/3_10_kafka_stream.py
```

### Kafka → Iceberg

```powershell
docker exec spark `
  /opt/spark/bin/spark-submit `
  /opt/spark/work/3_11_kafka_to_iceberg.py
```

### Full Pipeline

```powershell
docker exec spark `
  /opt/spark/bin/spark-submit `
  /opt/spark/work/3_12_full_pipeline.py
```

### Spark Java 확인

```powershell
docker exec spark java -version
```

### Kafka Topic 확인

```powershell
docker exec -it kafka `
  /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:9092 `
  --list
```

---

# 33. 서비스 종료

```powershell
docker compose down
```

Volume까지 삭제하려면:

```powershell
docker compose down -v
```

> `docker compose down -v`는 Keycloak, MinIO 등의 persistent volume도 삭제할 수 있으므로 학습 데이터가 필요한 경우 주의한다.

---

# Phase 3 Learning Summary

```text
Phase 3

Kafka
  ↓
Real-time Event

Spark
  ↓
Stream Processing

Iceberg
  ↓
Data Lake Table

Parquet
  ↓
Physical Data

MinIO
  ↓
Object Storage

Checkpoint
  ↓
Streaming State

event_id
  ↓
End-to-End Tracking
```

**핵심 한 문장**

> Kafka에서 발생한 실시간 사용량 이벤트를 Spark Structured Streaming으로 처리하고, Apache Iceberg Table로 관리하면서 실제 Parquet 데이터를 MinIO에 저장하는 Data Lake Pipeline을 Docker Compose 환경에서 구축한다.
