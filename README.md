# 03. Data Platform

Docker Compose 기반으로 인증, 이벤트 스트리밍, 데이터 처리 및 Lakehouse 저장 과정을 학습하는 프로젝트입니다.

- Repository: https://github.com/kstyle2198/data-platform
- Project directory: `03_dataplatform/`
- 기존 디렉터리명: `03_iceberg`

## 프로젝트 개요

이 프로젝트는 OpenLDAP과 Keycloak을 이용한 사용자 인증 환경, FastAPI 기반 API, Kafka 이벤트 메시징, Spark Structured Streaming 처리, Apache Iceberg 테이블 관리, MinIO 객체 저장소를 하나의 로컬 환경에서 연동합니다.

핵심은 애플리케이션에서 발생한 이벤트가 Kafka를 거쳐 Spark에서 처리되고, Iceberg 테이블로 저장된 뒤 MinIO의 데이터 파일과 메타데이터로 관리되는 과정을 직접 확인하는 것입니다.

## 주요 구성 요소

| 구성 요소 | 역할 |
|---|---|
| Docker Compose | 여러 서비스를 하나의 로컬 환경에서 실행하고 연결 |
| OpenLDAP | 사용자 디렉터리 및 LDAP 사용자 정보 관리 |
| Keycloak | 로그인, Realm/Client 설정, 토큰 발급 및 LDAP 연동 |
| FastAPI | API 제공, 인증 토큰 검증, 이벤트 발행 및 파이프라인 연동 |
| Kafka | 애플리케이션 이벤트와 파이프라인 상태 메시지 전달 |
| Spark Structured Streaming | Kafka 이벤트를 읽어 마이크로배치 단위로 처리 |
| Apache Iceberg | 테이블 스키마, 스냅샷 및 메타데이터 관리 |
| MinIO | S3 호환 객체 저장소. Parquet 데이터 파일과 Iceberg 메타데이터 저장 |
| 웹 UI | 이벤트와 파이프라인 상태 등 실습 정보를 확인하는 화면 |

## 데이터 처리 흐름

```text
OpenLDAP
   │ 사용자 정보
   ▼
Keycloak ── 인증 토큰 ──> FastAPI / 웹 UI
                              │
                              │ Usage Event
                              ▼
                         Kafka
                     usage-events
                              │
                              ▼
                 Spark Structured Streaming
                              │
                              ▼
                    Apache Iceberg Table
                    local.usage_db.usage_events
                              │
                              ▼
                            MinIO
                     Parquet + Metadata
```

파이프라인 상태 메시지는 `pipeline-status` 토픽을 통해 전달될 수 있습니다.

### 처리 단계

1. Keycloak은 로그인 및 토큰 발급을 담당하며, LDAP 사용자 연동은 OpenLDAP을 이용합니다.
2. FastAPI는 Usage Event를 Kafka의 `usage-events` 토픽으로 발행합니다.
3. Spark Structured Streaming은 Kafka 토픽을 읽어 이벤트 JSON을 스키마에 맞게 파싱합니다.
4. 유효한 이벤트를 마이크로배치 단위로 처리하고 Iceberg 테이블에 추가합니다.
5. Spark는 파이프라인 처리 상태를 `pipeline-status` 토픽으로 발행합니다.
6. Iceberg는 테이블의 스냅샷과 메타데이터를 관리하며, 데이터 파일은 Parquet 형식으로 MinIO에 저장됩니다.

## Iceberg와 MinIO 저장 구조

웨어하우스는 `s3a://warehouse/` 경로를 사용합니다. Spark 카탈로그는 `local` HadoopCatalog로 구성되어 있으며, 테이블 예시는 `local.usage_db.usage_events`입니다.

개념적인 저장 구조는 다음과 같습니다.

```text
MinIO
└── warehouse/
    └── usage_db/
        └── usage_events/
            ├── data/
            │   └── *.parquet
            └── metadata/
                ├── *.metadata.json
                ├── snap-*.avro
                └── manifest files
```

실제 파일명과 배치는 Iceberg 버전 및 쓰기 방식에 따라 달라질 수 있습니다.

- **Parquet 파일**: 실제 이벤트 행 데이터
- **Metadata JSON**: 테이블 스키마, 현재 메타데이터 및 스냅샷 참조
- **Snapshot**: 특정 시점의 테이블 상태
- **Manifest 파일**: 스냅샷에서 참조하는 데이터 파일 정보
- **Checkpoint**: Spark Structured Streaming 작업의 진행 상태 복구에 사용

## 프로젝트 디렉터리 확인

하위 경로와 파일은 현재 체크아웃된 저장소를 기준으로 확인합니다.

```powershell
cd D:\data_platform\03_dataplatform

Get-ChildItem
Get-ChildItem -Recurse -File |
  Select-Object -ExpandProperty FullName
```

Docker Compose에 등록된 서비스 확인:

```powershell
docker compose config --services
```

주요 설정과 코드 영역은 다음과 같습니다.

- `docker-compose.yml`: 컨테이너, 네트워크, 포트, 환경변수 및 볼륨
- `api/`: FastAPI 애플리케이션 및 API 관련 코드
- `keycloak/`: Keycloak Realm 등 초기 설정
- `openldap/`: LDAP 초기 설정 및 사용자 데이터
- `spark/`: Spark 이미지, 공통 설정 및 스트리밍 처리 코드
- `minio/`: MinIO 관련 설정 또는 이미지 빌드 파일

실제 저장소에 존재하는 경로와 파일명은 위 명령으로 확인합니다.

## 실행 및 상태 확인

Docker Desktop이 실행 중인 상태에서 프로젝트 디렉터리로 이동합니다.

```powershell
docker compose config --services
docker compose build
docker compose up -d
docker compose ps
```

로그 확인:

```powershell
docker compose logs --tail=100
docker compose logs -f spark
docker compose logs -f api
```

`api`, `spark`는 Compose에 등록된 실제 서비스 이름과 일치하는지 확인한 뒤 사용합니다.

서비스 종료:

```powershell
docker compose down
```

일반 종료 시 `-v` 옵션을 사용하지 않으면 볼륨 데이터 보존에 유리합니다. `docker compose down -v`는 연결된 볼륨을 삭제할 수 있으므로 주의합니다.

## Kafka 이벤트 확인

프로젝트에서 사용하는 주요 토픽은 다음과 같습니다.

| 토픽 | 용도 |
|---|---|
| `usage-events` | 애플리케이션 Usage Event |
| `pipeline-status` | 파이프라인 처리 단계 및 상태 |

Kafka CLI 사용 예시입니다. Kafka 컨테이너 이름과 CLI 경로는 현재 이미지 구성에 맞춰 확인합니다.

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:9092 `
  --list
```

이벤트 확인:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:9092 `
  --topic usage-events `
  --from-beginning
```

Docker 내부 서비스끼리는 `kafka:9092`와 같은 Compose 서비스 주소를 사용합니다. Windows 호스트에서 실행하는 클라이언트는 Compose에 공개된 호스트 포트를 사용해야 합니다.

## 현재 프로젝트에서 다루는 핵심 개념

- LDAP의 DN, DC, OU, CN, UID와 사용자 디렉터리
- Keycloak Realm, Client, JWT 및 LDAP 연동
- Kafka Producer/Consumer와 토픽 기반 이벤트 전달
- Spark Structured Streaming, 마이크로배치 및 체크포인트
- Python JSON 파싱과 이벤트 스키마 정규화
- Iceberg 테이블, 스냅샷, 메타데이터 및 데이터 파일
- MinIO의 S3 호환 저장 구조
- 파이프라인 상태 이벤트와 로그를 통한 처리 결과 확인

## 프로젝트 요약

```text
인증
OpenLDAP + Keycloak
        │
애플리케이션
FastAPI / 웹 UI
        │
이벤트 전달
Kafka
        │
스트리밍 처리
Spark Structured Streaming
        │
테이블 관리
Apache Iceberg
        │
객체 저장
MinIO (Parquet + Iceberg metadata)
```

이 프로젝트는 위 데이터 흐름을 직접 실행하고 각 구성 요소의 역할과 데이터 저장 구조를 확인하기 위한 실습 환경입니다.
