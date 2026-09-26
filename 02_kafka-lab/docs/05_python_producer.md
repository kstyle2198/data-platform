좋습니다. **Phase 2-5에서는 Kafka CLI 대신 Python으로 Producer를 만들어 `usage-events`에 JSON 이벤트를 전송**해보겠습니다.

이번 단계의 핵심은:

```text
Python Producer
      │
      │ JSON
      ▼
Kafka
      │
      ▼
usage-events
```

입니다.

아직 **FastAPI와 연결하지 않습니다.** 먼저 Python Kafka Producer 자체가 정상적으로 동작하는지 확인합니다.

---

# 1. Phase 2-5 목표

앞에서는 CLI로:

```text
hello kafka
```

를 보냈습니다.

이번에는 Python에서:

```python
{
    "user_id": "kim",
    "service": "api",
    "usage_type": "request",
    "quantity": 10
}
```

형태의 데이터를 Kafka로 보내겠습니다.

최종적으로는:

```text
Python
   │
   │ JSON Event
   ▼
Kafka
   │
   └── usage-events
```

가 됩니다.

---

# 2. Producer 디렉터리 만들기

현재 프로젝트를 다음과 같이 확장합니다.

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
└── producer/
    ├── Dockerfile
    ├── requirements.txt
    └── producer.py
```

이번 단계에서는 `producer`를 별도의 Docker 컨테이너로 실행해 보겠습니다.

이렇게 하는 이유는 나중에 실제 구조와 비슷하게 만들기 위해서입니다.

```text
FastAPI Container
       │
       ▼
Kafka Container
```

나중에는:

```text
FastAPI
   │
   │ Kafka Producer
   ▼
Kafka
```

로 합쳐집니다.

---

# 3. producer 디렉터리 생성

PowerShell에서 프로젝트 루트에서:

```powershell
mkdir producer
```

그리고 파일 3개를 만듭니다.

```text
producer/
├── Dockerfile
├── requirements.txt
└── producer.py
```

---

# 4. requirements.txt

`producer/requirements.txt`:

```text
kafka-python
```

`kafka-python`은 Python에서 Kafka Producer/Consumer를 사용할 수 있도록 해주는 라이브러리입니다.

---

# 5. producer.py 작성

`producer/producer.py`:

```python
import json
import time

from kafka import KafkaProducer


# ============================================================
# Kafka Producer 생성
# ============================================================

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",

    value_serializer=lambda value:
        json.dumps(value).encode("utf-8")
)


# ============================================================
# Kafka Topic
# ============================================================

TOPIC = "usage-events"


# ============================================================
# Usage Event
# ============================================================

event = {
    "user_id": "kim",
    "service": "api",
    "usage_type": "request",
    "quantity": 10,
    "timestamp": "2026-09-26T16:00:00"
}


# ============================================================
# 메시지 전송
# ============================================================

print("Sending event...")

future = producer.send(
    TOPIC,
    value=event
)


# Kafka 전송 결과 확인
metadata = future.get(timeout=10)


print("Message sent successfully!")

print(f"topic     : {metadata.topic}")
print(f"partition : {metadata.partition}")
print(f"offset    : {metadata.offset}")


# Producer 종료
producer.flush()
producer.close()
```

---

# 6. 코드 하나씩 이해하기

## `KafkaProducer`

```python
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
)
```

Python 프로그램이 Kafka에 연결합니다.

여기서:

```text
kafka:9092
```

가 중요합니다.

Producer 컨테이너에서 Kafka 컨테이너로 연결하기 때문입니다.

```text
Producer Container
       │
       │ kafka:9092
       ▼
Kafka Container
```

---

# 7. JSON Serializer

다음 부분도 중요합니다.

```python
value_serializer=lambda value:
    json.dumps(value).encode("utf-8")
```

우리가 보내는 데이터는 Python Dictionary입니다.

```python
event = {
    "user_id": "kim",
    "service": "api",
    "quantity": 10
}
```

하지만 Kafka는 기본적으로 바이트 데이터를 주고받습니다.

그래서:

```text
Python Dictionary
       ↓
json.dumps()
       ↓
JSON 문자열
       ↓
UTF-8 bytes
       ↓
Kafka
```

과정을 거칩니다.

---

# 8. 실제 이벤트

```python
event = {
    "user_id": "kim",
    "service": "api",
    "usage_type": "request",
    "quantity": 10,
    "timestamp": "2026-09-26T16:00:00"
}
```

이것은 나중에 만들 **서비스 사용량 이벤트**의 기본 형태입니다.

예를 들어:

```json
{
  "user_id": "kim",
  "service": "api",
  "usage_type": "request",
  "quantity": 10,
  "timestamp": "2026-09-26T16:00:00"
}
```

입니다.

---

# 9. Kafka에 메시지 전송

핵심 코드입니다.

```python
future = producer.send(
    TOPIC,
    value=event
)
```

여기서:

```python
TOPIC = "usage-events"
```

이므로:

```text
Python Producer
      │
      │ event
      ▼
usage-events
```

로 전송합니다.

---

# 10. Partition 확인

다음 코드가 재미있는 부분입니다.

```python
metadata = future.get(timeout=10)
```

Kafka가 메시지를 정상적으로 저장하면 metadata를 반환합니다.

그리고:

```python
print(f"topic     : {metadata.topic}")
print(f"partition : {metadata.partition}")
print(f"offset    : {metadata.offset}")
```

를 통해:

```text
topic     : usage-events
partition : 1
offset    : 3
```

같은 결과를 볼 수 있습니다.

즉 Python Producer를 통해서도 우리가 앞에서 배운:

```text
Topic
Partition
Offset
```

을 직접 확인할 수 있습니다.

---

# 11. Producer Dockerfile

`producer/Dockerfile`:

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

CMD ["python", "producer.py"]
```

---

# 12. docker-compose.yml에 Producer 추가

현재 `docker-compose.yml`에 다음 서비스를 추가합니다.

```yaml
  producer:
    build: ./producer
    container_name: kafka-producer

    depends_on:
      - kafka
```

전체 구조는:

```text
services:

  keycloak:
    ...

  api:
    ...

  kafka:
    ...

  producer:
    build: ./producer
    container_name: kafka-producer
    depends_on:
      - kafka
```

입니다.

---

# 13. Producer 실행

다음 명령을 실행합니다.

```powershell
docker compose build producer
```

그 다음:

```powershell
docker compose run --rm producer
```

정상적으로 동작하면 대략:

```text
Sending event...

Message sent successfully!

topic     : usage-events
partition : 1
offset    : 0
```

같은 결과를 볼 수 있습니다.

Partition 번호는 상황에 따라 달라질 수 있습니다.

---

# 14. Consumer로 실제 메시지 확인

이제 Kafka Consumer를 실행합니다.

```powershell
docker exec -it kafka bash
```

그리고:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic usage-events \
  --group python-test \
  --from-beginning
```

그러면:

```json
{"user_id": "kim", "service": "api", "usage_type": "request", "quantity": 10, "timestamp": "2026-09-26T16:00:00"}
```

가 나타날 것입니다.

즉:

```text
Python Producer
       │
       │ Python dict
       ▼
 JSON Serializer
       │
       ▼
     Kafka
       │
       │ usage-events
       ▼
   Consumer
```

가 실제로 동작했습니다.

---

# 15. 여러 이벤트를 보내보자

이번에는 `producer.py`를 조금 변경해 보겠습니다.

```python
import json
import time

from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda value:
        json.dumps(value).encode("utf-8")
)


TOPIC = "usage-events"


events = [
    {
        "user_id": "kim",
        "service": "api",
        "usage_type": "request",
        "quantity": 10
    },
    {
        "user_id": "kim",
        "service": "search",
        "usage_type": "query",
        "quantity": 5
    },
    {
        "user_id": "lee",
        "service": "api",
        "usage_type": "request",
        "quantity": 20
    },
    {
        "user_id": "park",
        "service": "storage",
        "usage_type": "gb",
        "quantity": 100
    }
]


for event in events:

    future = producer.send(
        TOPIC,
        value=event
    )

    metadata = future.get(timeout=10)

    print(
        f"sent: "
        f"user={event['user_id']} "
        f"service={event['service']} "
        f"partition={metadata.partition} "
        f"offset={metadata.offset}"
    )

    time.sleep(1)


producer.flush()
producer.close()
```

다시:

```powershell
docker compose build producer
docker compose run --rm producer
```

실행합니다.

예:

```text
sent: user=kim service=api partition=0 offset=1
sent: user=kim service=search partition=2 offset=1
sent: user=lee service=api partition=1 offset=1
sent: user=park service=storage partition=0 offset=2
```

처럼 나올 수 있습니다.

---

# 16. 왜 Partition이 서로 다를까?

Kafka Producer는 메시지를 어느 Partition에 넣을지 결정해야 합니다.

현재:

```text
usage-events
 ├── P0
 ├── P1
 └── P2
```

이므로 메시지가:

```text
message A → P0
message B → P2
message C → P1
message D → P0
```

처럼 분산될 수 있습니다.

특히 **key를 지정하지 않은 경우** Kafka Producer가 Partition을 분배합니다.

---

# 17. Key를 사용하면?

Kafka에서는 메시지를 보낼 때 다음처럼 `key`를 지정할 수 있습니다.

```python
producer.send(
    TOPIC,
    key="kim".encode("utf-8"),
    value=event
)
```

이렇게 하면 Kafka는 key를 기반으로 Partition을 결정합니다.

개념적으로:

```text
key = kim
      ↓
  Partition 1

key = kim
      ↓
  Partition 1

key = kim
      ↓
  Partition 1
```

처럼 같은 key의 이벤트가 같은 Partition으로 들어가도록 할 수 있습니다.

이것은 나중에 **사용자별 이벤트 순서 보장** 등을 설계할 때 중요합니다.

---

# 18. 현재 프로젝트에서는 왜 `user_id`가 좋은 Key인가?

우리 프로젝트의 이벤트는:

```json
{
  "user_id": "kim",
  "service": "api",
  "quantity": 10
}
```

형태입니다.

만약 `user_id`를 Kafka key로 사용하면:

```text
kim
 ↓
Partition 1

kim
 ↓
Partition 1

kim
 ↓
Partition 1
```

처럼 같은 사용자 이벤트를 같은 Partition으로 보내는 설계를 할 수 있습니다.

그러면 해당 Partition 안에서는 이벤트 순서를 유지할 수 있습니다.

이 부분은 **Phase 2-7에서 직접 실습**하면 이해가 훨씬 쉽습니다.

---

# 19. 이번 Phase에서 가장 중요한 것

이번 단계에서는 다음 흐름을 이해하면 됩니다.

### CLI Producer

```text
Kafka CLI
    ↓
Kafka
```

에서

### Python Producer

```text
Python
    ↓
KafkaProducer
    ↓
JSON
    ↓
Kafka
```

로 발전했습니다.

그리고 우리가 최종적으로 만들려는 구조는:

```text
             Keycloak
                │
                │ JWT
                ▼
             FastAPI
                │
                │ Python KafkaProducer
                ▼
             Kafka
                │
         usage-events
```

입니다.

---

# 20. Phase 2-5 완료 체크

다음이 모두 성공하면 완료입니다.

* [ ] `producer/` 디렉터리 생성
* [ ] `kafka-python` 설치
* [ ] Python `KafkaProducer` 생성
* [ ] `usage-events`로 JSON 전송
* [ ] Kafka의 `partition` 확인
* [ ] Kafka의 `offset` 확인
* [ ] Consumer에서 JSON 이벤트 확인
* [ ] 여러 이벤트 전송
* [ ] Partition 분산 확인

---

## 현재까지의 전체 학습 흐름

```text
Phase 2-1
Kafka 개념
   │
   ▼
Phase 2-2
Kafka Docker 실행
   │
   ▼
Phase 2-3
CLI Producer / Consumer
   │
   ▼
Phase 2-4
Consumer Group / Partition
   │
   ▼
★ Phase 2-5
Python Producer
   │
   ▼
Phase 2-6
Python Consumer
   │
   ▼
Phase 2-7
Key / Partition / Offset 심화
   │
   ▼
Phase 2-8
FastAPI → Kafka
```

**다음 Phase 2-6에서는 반대로 Python `KafkaConsumer`를 만들어서 `usage-events`의 JSON을 읽고, `user_id`, `service`, `quantity`를 Python 객체로 꺼내 처리하는 실습**을 진행하면 됩니다.
