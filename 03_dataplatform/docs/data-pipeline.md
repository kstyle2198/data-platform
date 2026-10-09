## Phase 3. 전체 Data Pipeline 구조

현재 프로젝트는 **사용자 인증 → API 이벤트 생성 → Kafka → Spark → Iceberg → MinIO → Pipeline 상태 → UI** 흐름으로 구성됩니다.

```text
                    ┌──────────────┐
                    │   Keycloak   │
                    │ Authentication│
                    └──────┬───────┘
                           │ JWT
                           ▼
                    ┌──────────────┐
                    │   FastAPI    │
                    │   Usage API  │
                    └──────┬───────┘
                           │
                     Usage Event
                           │
                           ▼
                 ┌──────────────────┐
                 │      Kafka       │
                 │  usage-events    │
                 └────────┬─────────┘
                          │
                    Structured
                    Streaming
                          │
                          ▼
                 ┌──────────────────┐
                 │      Spark       │
                 │ Micro-batch 처리 │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │     Iceberg      │
                 │   usage_events   │
                 └────────┬─────────┘
                          │
                    실제 데이터
                    Parquet 저장
                          │
                          ▼
                 ┌──────────────────┐
                 │      MinIO       │
                 │     warehouse    │
                 └──────────────────┘


     Pipeline 처리상태
            │
            ▼
     ┌──────────────────┐
     │      Kafka       │
     │  pipeline-status │
     └────────┬─────────┘
              │
              ▼
             UI
```

## 단계별 역할

| 단계        | 구성요소                           | 역할                              |
| --------- | ------------------------------ | ------------------------------- |
| ① 인증      | **Keycloak**                   | 사용자 인증 및 JWT 발급                 |
| ② API     | **FastAPI**                    | JWT 검증 및 Usage Event 생성         |
| ③ 메시징     | **Kafka `usage-events`**       | Usage Event 전달                  |
| ④ 처리      | **Spark Structured Streaming** | Kafka 데이터를 Micro-batch로 처리      |
| ⑤ 테이블     | **Apache Iceberg**             | 데이터를 `usage_events` 테이블로 관리     |
| ⑥ Storage | **MinIO**                      | Iceberg의 실제 Parquet/Metadata 저장 |
| ⑦ 상태      | **Kafka `pipeline-status`**    | Spark/Iceberg/MinIO 처리 상태 전달    |
| ⑧ 화면      | **UI**                         | 이벤트와 Pipeline 상태 표시             |

### 데이터 자체의 흐름

```text
User
 ↓
Keycloak
 ↓ JWT
FastAPI
 ↓
Kafka: usage-events
 ↓
Spark
 ↓
Iceberg Table
 ↓
MinIO
   └── warehouse/usage_db/usage_events/
        ├── metadata/
        └── data/*.parquet
```

### 상태 정보의 흐름

```text
Spark
 │
 ├── spark: completed
 │
 ├── iceberg: processing
 │
 ├── iceberg: completed
 │
 ├── minio: processing
 │
 └── minio: completed
          │
          ▼
Kafka: pipeline-status
          │
          ▼
         UI
```

## 핵심 추적 단위: `event_id`

하나의 이벤트에 고유한 `event_id`를 부여하여 전체 Pipeline을 연결합니다.

```text
event_id = EVT-001

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
   ↓
pipeline-status
   ↓
UI
```

따라서 UI에서는 **`EVT-001` 하나를 기준으로 전체 처리 상태를 추적**할 수 있습니다.

### 핵심 정리

> **Keycloak은 인증, FastAPI는 이벤트 생성, Kafka는 전달, Spark는 처리, Iceberg는 테이블 관리, MinIO는 실제 저장, `pipeline-status`는 처리 상태 전달, UI는 전체 결과를 시각화합니다.**

그리고 **Iceberg와 MinIO는 별도의 데이터 처리 단계라기보다**,
**Iceberg가 테이블을 관리하고 MinIO가 실제 데이터를 저장하는 관계**입니다.
