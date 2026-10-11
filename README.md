# Mini Data Platform 프로젝트

Kafka, Spark Structured Streaming, Apache Iceberg, MinIO, FastAPI, Next.js, Keycloak, OpenLDAP을 Docker Compose로 연동하는 데이터 플랫폼 실습 프로젝트입니다.

## 1. 프로젝트 구조

```text
03_dataplatform/
├── .env                         # 서비스 계정 및 비밀번호 등 환경 변수
├── docker-compose.yml           # 전체 서비스 정의 및 실행 설정
├── people.ldif                  # OpenLDAP 초기 OU/엔트리 정의
├── keycloak-ldap-init.sh        # Keycloak LDAP User Federation 초기화 스크립트
├── keycloak/
│   └── hdaic-realm.json          # Keycloak realm import 설정
├── minio/
│   └── Dockerfile                # MinIO 이미지 빌드 설정
├── spark/
│   ├── Dockerfile                # Spark + Iceberg 실행 이미지
│   ├── pipeline.py               # Kafka → Spark → Iceberg 데이터 파이프라인
│   ├── query_api.py              # Iceberg 조회 및 스냅샷 비교 API
│   └── common/
│       ├── config.py             # 공통 환경 설정
│       ├── schema.py             # 이벤트 스키마 및 컬럼 정의
│       └── spark_factory.py      # SparkSession 생성 및 Iceberg 설정
├── api/
│   ├── Dockerfile                # FastAPI 이미지 빌드 설정
│   ├── main.py                   # REST API, 인증 및 파이프라인 연동
│   └── requirements.txt          # Python 의존성
├── frontend/
│   ├── Dockerfile                # Next.js 이미지 빌드 설정
│   ├── package.json              # 프런트엔드 의존성 및 스크립트
│   ├── next.config.mjs           # Next.js 설정
│   └── app/
│       ├── layout.tsx            # 공통 레이아웃
│       ├── page.tsx              # 메인 파이프라인 UI
│       ├── globals.css            # 전역 스타일
│       └── time-travel/
│           └── page.tsx           # Iceberg Snapshot/Time Travel UI
├── security-agent/
│   ├── Dockerfile                # 보안 에이전트 이미지 빌드 설정
│   └── ...                       # Kafka 보안 이벤트 소비 및 탐지 로직
└── docs/
    ├── data-pipeline.md          # 데이터 파이프라인 설명
    ├── ldap-keycloak.md          # LDAP/Keycloak 연동 설명
    └── ui_to_fastapi_jwt.md      # UI와 FastAPI JWT 인증 연동 설명
```

> 참고: 디렉터리 및 파일 구성은 프로젝트에서 공유된 구조를 바탕으로 정리했습니다. 실제 체크아웃한 저장소의 파일명이나 추가 파일은 로컬 트리와 대조해 주세요.

## 2. 로컬 서비스 접속 링크

| 서비스 | 접속 링크 | 용도 |
|---|---|---|
| Next.js Frontend | [http://localhost:3000](http://localhost:3000) | 파이프라인 모니터링 및 데이터 플랫폼 UI |
| Iceberg Time Travel UI | [http://localhost:3000/time-travel](http://localhost:3000/time-travel) | Iceberg 스냅샷 조회 및 비교 UI |
| FastAPI Swagger UI | [http://localhost:8000/docs](http://localhost:8000/docs) | REST API 문서 및 테스트 |
| Keycloak | [http://localhost:8080](http://localhost:8080) | 로그인, Realm 및 사용자 인증 관리 |
| phpLDAPadmin | [http://localhost:8081](http://localhost:8081) | OpenLDAP 디렉터리 관리 UI |
| MinIO Console | [http://localhost:9002](http://localhost:9002) | 객체 스토리지 관리 UI |

### 브라우저에서 직접 접속할 수 없는 내부 서비스

| 서비스 | Docker 내부 주소 | 설명 |
|---|---|---|
| OpenLDAP | `openldap:389` | LDAP 디렉터리 서비스. 호스트에서는 `localhost:389` 포트로 연결 가능 |
| Kafka | `kafka:29092` | Docker 네트워크 내부 클라이언트용 Kafka 주소 |
| Kafka 외부 리스너 | `localhost:9092` | 호스트에서 실행하는 Kafka 클라이언트용 주소 |
| Iceberg Query API | `http://iceberg-query:8010` | FastAPI가 사용하는 내부 Iceberg 조회 서비스 |
| Spark | `spark` | Structured Streaming 파이프라인 실행 서비스 |
| Security Agent | `security-agent` | Kafka 보안 이벤트를 소비하는 보안 탐지 서비스 |

> `http://iceberg-query:8010`은 Docker Compose 네트워크 내부 주소입니다. 현재 Compose 설정에는 호스트용 `ports` 매핑이 아니라 `expose: 8010`이 있으므로 일반 브라우저에서 `localhost:8010`으로 접속할 수 없습니다.

## 3. Docker Compose 서비스와 역할

| 서비스 | 주요 역할 | 연결 관계 |
|---|---|---|
| `openldap` | 사용자 디렉터리 및 LDAP 인증 데이터 저장 | Keycloak, FastAPI |
| `openldap-init` | `people.ldif`를 사용해 LDAP 초기 OU 구성 | OpenLDAP |
| `phpldapadmin` | LDAP 디렉터리 브라우저 UI 제공 | OpenLDAP |
| `keycloak` | Realm 기반 인증 및 토큰 발급 | OpenLDAP, Frontend, FastAPI |
| `keycloak-ldap-init` | `hdaic` Realm의 LDAP User Federation 설정 초기화 | Keycloak, OpenLDAP |
| `kafka` | 이벤트 스트리밍 메시지 브로커 | FastAPI, Spark, Security Agent |
| `kafka-init` | Kafka 토픽 생성 | Kafka |
| `minio` | Iceberg 데이터 파일과 체크포인트 저장 | Spark, Iceberg Query API |
| `spark` | Kafka 이벤트 처리 및 Iceberg 테이블 적재 | Kafka, MinIO |
| `iceberg-query` | Iceberg 테이블 조회 및 스냅샷 비교 기능 제공 | Spark 설정, MinIO, FastAPI |
| `api` | 인증 검증, 이벤트 API, 파이프라인 상태 및 조회 API | Keycloak, Kafka, LDAP, Iceberg Query API |
| `frontend` | 사용자 인터페이스 | FastAPI |
| `security-agent` | 보안 이벤트 소비 및 임계값 기반 탐지 | Kafka |

## 4. 데이터 파이프라인

```text
[Frontend :3000]
       |
       | HTTP API / 인증 토큰
       v
[FastAPI :8000] <------> [Keycloak :8080]
       |                         |
       |                         v
       |                    [OpenLDAP :389]
       |
       +---- 이벤트 발행 ----> [Kafka]
       |                         |
       |                         v
       |               [Spark Structured Streaming]
       |                         |
       |                         v
       |                 [Iceberg Table]
       |                         |
       |                         v
       |                      [MinIO]
       |
       +---- 조회 요청 ----> [Iceberg Query API :8010]

[Kafka: security-events] ----> [Security Agent]
[Kafka: pipeline-status] ----> [FastAPI] ----> [Frontend]
```

### Kafka 토픽

| 토픽 | 목적 |
|---|---|
| `usage-events` | 사용량 이벤트를 Spark 파이프라인으로 전달 |
| `pipeline-status` | 파이프라인 처리 상태를 전달 |
| `security-events` | 보안 관련 이벤트 전달 |

Compose 설정상 각 토픽은 파티션 3개, 복제 계수 1로 생성됩니다.

### Iceberg 및 MinIO 설정

- Iceberg 테이블: `local.usage_db.usage_events`
- Warehouse 경로: `s3a://warehouse/`
- MinIO 내부 엔드포인트: `http://minio:9000`
- MinIO 호스트 API: `http://localhost:9000`
- MinIO Console: `http://localhost:9002`
- Spark 체크포인트: `s3a://warehouse/checkpoints/full_usage_pipeline`

## 5. 실행 및 상태 확인

프로젝트 루트(`03_dataplatform`)에서 실행합니다.

- .env-example 파일을 .env 파일로 변경후 실행

```bash
# Compose 설정 검증
docker compose config

# 서비스 이미지 빌드 및 백그라운드 실행
docker compose up -d --build

# 서비스 상태 확인
docker compose ps

# 전체 로그 확인
docker compose logs -f

# 특정 서비스 로그 확인
docker compose logs -f api
docker compose logs -f frontend
docker compose logs -f spark
docker compose logs -f iceberg-query
docker compose logs -f kafka

# 서비스 중지
docker compose down
```

#### Default User 
- ID : kim
- PW : test1234
- Roll : user, ldap-admin

