## Phase 2-2. Docker로 Kafka 실행하기

Phase 2-2의 목표는 **Kafka를 Docker 컨테이너로 실행하고, FastAPI와 Kafka가 같은 Docker 네트워크에서 통신할 수 있는 상태**를 만드는 것입니다.

현재 프로젝트는 다음과 같습니다.

```text
keycloak-lab/
├── docker-compose.yml
├── keycloak/
│   └── hdaic-realm.json
└── api/
    ├── Dockerfile
    ├── requirements.txt
    └── main.py
```

이번 단계에서는 **FastAPI 코드는 아직 수정하지 않습니다.**

---

# 1. Phase 2-2에서 만들 구조

기존:

```text
                    ┌──────────────┐
                    │   Keycloak   │
                    │    :8080     │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │   FastAPI    │
                    │    :8000     │
                    └──────────────┘
```

Phase 2-2 완료 후:

```text
                    ┌──────────────┐
                    │   Keycloak   │
                    │    :8080     │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │   FastAPI    │
                    │    :8000     │
                    └──────┬───────┘
                           │
                           │ 나중에 사용
                           ▼
                    ┌──────────────┐
                    │    Kafka     │
                    │    :9092     │
                    └──────────────┘
```

핵심은 **Kafka를 Docker Compose에 추가하는 것**입니다.

---

# 2. Kafka는 왜 `9092`인가?

Kafka에서 많이 사용하는 기본 포트가 `9092`입니다.

Docker 내부에서는 다음처럼 접근합니다.

```text
FastAPI
   │
   │ kafka:9092
   ▼
Kafka
```

여기서 중요한 점이 있습니다.

### `localhost:9092`가 아닙니다.

FastAPI 컨테이너에서:

```text
localhost
```

는 **FastAPI 컨테이너 자신**을 의미합니다.

반면:

```text
kafka:9092
```

는 Docker Compose의 서비스 이름을 이용해서 Kafka 컨테이너에 접근합니다.

즉,

```text
FastAPI container
      │
      │ kafka:9092
      ▼
Kafka container
```

입니다.

---

# 3. docker-compose.yml 수정

현재 `docker-compose.yml`에 Kafka 서비스를 추가합니다.

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


  kafka:
    image: apache/kafka:3.9.0
    container_name: kafka

    environment:
      KAFKA_NODE_ID: 1

      KAFKA_PROCESS_ROLES: broker,controller

      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093

      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092

      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: >
        CONTROLLER:PLAINTEXT,
        PLAINTEXT:PLAINTEXT

      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093

      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER

      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT

      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1

      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1

      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0

      KAFKA_NUM_PARTITIONS: 3

    ports:
      - "9092:9092"


volumes:
  keycloak_data:
```

여기서 Kafka 부분이 새로 추가된 것입니다.

---

# 4. 설정을 하나씩 이해하기

처음 보면 설정이 많아 보이지만 핵심은 몇 개입니다.

### `KAFKA_NODE_ID`

```yaml
KAFKA_NODE_ID: 1
```

Kafka 서버의 ID입니다.

현재는 Kafka 서버가 하나뿐이므로:

```text
Kafka Broker 1
```

이라고 생각하면 됩니다.

---

### `KAFKA_PROCESS_ROLES`

```yaml
KAFKA_PROCESS_ROLES: broker,controller
```

현재 Kafka는 **KRaft 모드**로 실행합니다.

예전 Kafka는 ZooKeeper를 사용하는 구조가 일반적이었지만, 여기서는 ZooKeeper 없이 Kafka 자체가 메타데이터를 관리하는 KRaft 방식을 사용합니다.

따라서 하나의 Kafka 컨테이너가:

```text
Broker
+
Controller
```

역할을 함께 수행합니다.

학습용 단일 Kafka 환경에서는 이런 구성이 편리합니다.

---

# 5. Kafka Listener가 가장 중요합니다

다음 부분을 봅니다.

```yaml
KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
```

Kafka가 두 개의 통신 포트를 사용합니다.

```text
9092 → 일반 Kafka 데이터 통신
9093 → Controller 통신
```

우리가 실제 Producer/Consumer에서 사용하는 것은:

```text
9092
```

입니다.

---

# 6. `ADVERTISED_LISTENERS`가 중요한 이유

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
```

이 설정은 상당히 중요합니다.

Kafka는 단순히 연결만 받는 것이 아니라, 클라이언트에게:

> "Kafka에 계속 연결하려면 이 주소를 사용하세요."

라고 자신의 주소를 알려줍니다.

Docker 네트워크에서는:

```text
kafka:9092
```

가 올바른 주소입니다.

따라서 나중에 FastAPI에서:

```python
bootstrap_servers="kafka:9092"
```

라고 설정하게 됩니다.

---

# 7. 왜 `localhost:9092`가 아닌가?

이것이 Phase 2-2에서 가장 중요한 부분 중 하나입니다.

Docker 내부:

```text
┌──────────────────┐
│ FastAPI          │
│                  │
│ kafka:9092 ──────┼──────▶ Kafka
└──────────────────┘
```

Windows PC에서:

```text
Windows
   │
   │ localhost:9092
   ▼
Docker Kafka
```

는 다른 이야기입니다.

현재 단계에서는 **Kafka CLI도 Kafka 컨테이너 내부에서 실행**해서 단순하게 학습하겠습니다.

즉,

```text
FastAPI       → kafka:9092
Kafka CLI     → Kafka 컨테이너 내부
```

로 진행합니다.

---

# 8. Kafka 실행

PowerShell에서 프로젝트 디렉터리로 이동합니다.

```powershell
cd <현재 keycloak-lab 디렉터리>
```

그리고:

```powershell
docker compose up -d
```

실행합니다.

확인:

```powershell
docker compose ps
```

정상이라면 대략:

```text
NAME          STATUS
keycloak      Up
usage-api     Up
kafka         Up
```

처럼 나옵니다.

---

# 9. Kafka 로그 확인

```powershell
docker logs kafka
```

또는:

```powershell
docker logs -f kafka
```

정상적으로 시작되면 Kafka가 실행되고 있다는 로그를 확인할 수 있습니다.

종료하려면:

```text
Ctrl + C
```

입니다.

---

# 10. Kafka 컨테이너에 들어가기

이제 Kafka 컨테이너 내부로 들어가 보겠습니다.

```powershell
docker exec -it kafka bash
```

프롬프트가 바뀌면 Kafka 컨테이너 안에 들어온 것입니다.

```text
root@....:/#
```

또는 비슷한 형태가 됩니다.

---

# 11. Kafka Topic 확인

Kafka에는 메시지를 저장할 **Topic**이 필요합니다.

우리가 사용할 Topic 이름은:

```text
usage-events
```

입니다.

먼저 Topic 목록을 확인합니다.

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list
```

처음에는 아무것도 나오지 않을 수 있습니다.

---

# 12. `usage-events` Topic 생성

다음 명령을 실행합니다.

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --topic usage-events \
  --partitions 3 \
  --replication-factor 1
```

성공하면:

```text
Created topic usage-events.
```

와 같은 메시지가 나옵니다.

---

# 13. 왜 Partition을 3개 만들었나?

앞의 Phase 2-1에서 배웠던 개념입니다.

```text
usage-events
    │
    ├── Partition 0
    ├── Partition 1
    └── Partition 2
```

입니다.

현재는 Kafka Broker가 하나뿐이지만 Topic 내부에는 3개의 Partition을 만들었습니다.

나중에 Consumer Group을 공부할 때:

```text
Consumer 1 ──▶ Partition 0
Consumer 2 ──▶ Partition 1
Consumer 3 ──▶ Partition 2
```

와 같은 실습을 할 수 있습니다.

---

# 14. Topic 상세 정보 확인

다음 명령을 실행합니다.

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic usage-events
```

대략 다음과 같이 나옵니다.

```text
Topic: usage-events
PartitionCount: 3
ReplicationFactor: 1

Topic: usage-events
Partition: 0
Leader: 1

Topic: usage-events
Partition: 1
Leader: 1

Topic: usage-events
Partition: 2
Leader: 1
```

이것을 보면:

```text
Topic
 └── usage-events

      ├── Partition 0
      ├── Partition 1
      └── Partition 2
```

구조를 확인할 수 있습니다.

---

# 15. 여기까지 하면 무엇을 한 것인가?

현재까지:

```text
Docker Compose
      │
      ├── Keycloak
      │
      ├── FastAPI
      │
      └── Kafka
             │
             └── usage-events
                    ├── P0
                    ├── P1
                    └── P2
```

가 만들어졌습니다.

아직 FastAPI가 Kafka에 메시지를 보내지는 않습니다.

---

# 16. Phase 2-2에서 꼭 기억할 것

이번 단계에서는 아래 5가지만 확실히 이해하면 됩니다.

| 개념           | 현재 설정          |
| ------------ | -------------- |
| Kafka Broker | `kafka` 컨테이너   |
| Kafka 주소     | `kafka:9092`   |
| Topic        | `usage-events` |
| Partition    | 3개             |
| Replication  | 1개             |

특히:

```text
FastAPI → kafka:9092
```

라는 Docker 내부 통신 구조를 기억해 두세요.

---

## 전체 흐름

현재까지는:

```text
┌─────────────┐
│   Keycloak  │
│    :8080    │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   FastAPI   │
│    :8000    │
└──────┬──────┘
       │
       │ 아직 연결하지 않음
       ▼
┌─────────────────────────┐
│          Kafka          │
│        kafka:9092       │
│                         │
│     usage-events        │
│     ┌───┬───┬───┐       │
│     │P0 │P1 │P2 │       │
│     └───┴───┴───┘       │
└─────────────────────────┘
```

**Phase 2-3에서는 이 `usage-events` Topic에 실제 메시지를 넣고 꺼내보는 실습**을 합니다.

즉,

```text
Producer
   ↓
usage-events
   ↓
Consumer
```

를 직접 실행하면서 **Kafka의 메시지 흐름, Partition, Offset**을 눈으로 확인하게 됩니다.
