좋습니다. **Phase 2-6은 Python Consumer를 만드는 단계**입니다.

Phase 2-5에서:

```text
Python Producer
      │
      │ JSON
      ▼
Kafka
   └── usage-events
```

를 만들었으므로, 이번에는 반대 방향입니다.

```text
Kafka
   │
   │ JSON
   ▼
Python Consumer
   │
   ▼
Python Dictionary
   │
   ├── user_id
   ├── service
   ├── usage_type
   └── quantity
```

이번 단계가 끝나면 **Python ↔ Kafka 양방향 통신**이 완성됩니다.

---

# 1. Phase 2-6 목표

최종적으로 다음 구조를 만듭니다.

```text
┌─────────────────┐
│ Python Producer │
└────────┬────────┘
         │
         │ JSON
         ▼
┌────────────────────────┐
│         Kafka          │
│                        │
│      usage-events      │
│      ┌──┬──┬──┐        │
│      │P0│P1│P2│        │
│      └──┴──┴──┘        │
└────────┬───────────────┘
         │
         │ JSON
         ▼
┌─────────────────┐
│ Python Consumer │
└─────────────────┘
```

---

# 2. Consumer 디렉터리 만들기

현재 프로젝트:

```text
keycloak-lab/
├── docker-compose.yml
├── keycloak/
├── api/
└── producer/
```

여기에:

```text
consumer/
├── Dockerfile
├── requirements.txt
└── consumer.py
```

를 추가합니다.

PowerShell:

```powershell
mkdir consumer
```

---

# 3. requirements.txt

`consumer/requirements.txt`:

```text
kafka-python
```

Producer와 동일하게 `kafka-python`을 사용합니다.

---

# 4. consumer.py 작성

먼저 가장 단순한 Consumer를 만들어 보겠습니다.

```python
import json

from kafka import KafkaConsumer


# ============================================================
# Kafka Consumer
# ============================================================

consumer = KafkaConsumer(
    "usage-events",

    bootstrap_servers="kafka:9092",

    group_id="usage-consumer",

    auto_offset_reset="earliest",

    enable_auto_commit=True,

    value_deserializer=lambda value:
        json.loads(value.decode("utf-8"))
)


# ============================================================
# Message Receive
# ============================================================

print("Kafka Consumer started...")
print("Waiting for messages...")


for message in consumer:

    event = message.value

    print()
    print("================================")
    print("Received event")
    print("================================")

    print(f"topic     : {message.topic}")
    print(f"partition : {message.partition}")
    print(f"offset    : {message.offset}")

    print(f"user_id   : {event.get('user_id')}")
    print(f"service   : {event.get('service')}")
    print(f"usage_type: {event.get('usage_type')}")
    print(f"quantity  : {event.get('quantity')}")
```

---

# 5. 가장 중요한 부분

Consumer를 만드는 핵심 코드입니다.

```python
consumer = KafkaConsumer(
    "usage-events",
    bootstrap_servers="kafka:9092",
    group_id="usage-consumer"
)
```

하나씩 살펴보겠습니다.

---

## `usage-events`

```python
KafkaConsumer(
    "usage-events",
```

Consumer가 읽을 Topic입니다.

즉:

```text
Kafka
 │
 └── usage-events
```

를 읽겠다는 의미입니다.

---

# 6. `bootstrap_servers`

```python
bootstrap_servers="kafka:9092"
```

Docker 환경에서 Kafka의 주소입니다.

```text
Consumer Container
       │
       │ kafka:9092
       ▼
Kafka Container
```

---

# 7. `group_id`

```python
group_id="usage-consumer"
```

이 부분이 매우 중요합니다.

이 Consumer는:

```text
Consumer Group
└── usage-consumer
```

라는 그룹에 속합니다.

앞에서 배운:

```text
--group group-a
```

와 같은 개념입니다.

---

# 8. `auto_offset_reset`

```python
auto_offset_reset="earliest"
```

이 설정은 **Consumer Group에 Offset이 없을 때 어디서부터 읽을지** 결정합니다.

`earliest`:

```text
처음부터 읽기
```

입니다.

반대로:

```python
auto_offset_reset="latest"
```

이면:

```text
Consumer가 시작한 이후 새로 들어오는 메시지부터 읽기
```

에 가까운 동작을 합니다.

학습할 때는 `earliest`가 편리합니다.

---

# 9. JSON 역직렬화

이 부분도 중요합니다.

```python
value_deserializer=lambda value:
    json.loads(value.decode("utf-8"))
```

Producer에서는:

```text
Python dict
   ↓
JSON
   ↓
bytes
   ↓
Kafka
```

로 보냈습니다.

Consumer에서는 반대로:

```text
Kafka
   ↓
bytes
   ↓
UTF-8 문자열
   ↓
JSON
   ↓
Python dict
```

로 복원합니다.

그래서:

```python
event = message.value
```

를 실행하면 Python Dictionary가 됩니다.

---

# 10. Dockerfile

`consumer/Dockerfile`:

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

CMD ["python", "consumer.py"]
```

---

# 11. docker-compose.yml에 Consumer 추가

현재 Compose에 다음을 추가합니다.

```yaml
  consumer:
    build: ./consumer
    container_name: kafka-consumer

    depends_on:
      - kafka
```

따라서 구조는:

```yaml
services:

  keycloak:
    ...

  api:
    ...

  kafka:
    ...

  producer:
    ...

  consumer:
    build: ./consumer
    container_name: kafka-consumer
    depends_on:
      - kafka
```

입니다.

---

# 12. Consumer 실행

먼저 이미 만들어진 Consumer 이미지를 빌드합니다.

```powershell
docker compose build consumer
```

그리고 실행:

```powershell
docker compose run --rm consumer
```

정상적으로 실행되면:

```text
Kafka Consumer started...
Waiting for messages...
```

라고 나오면서 대기합니다.

이 상태로 놔두세요.

---

# 13. Producer 실행

새로운 PowerShell 창에서:

```powershell
docker compose run --rm producer
```

를 실행합니다.

그러면 Producer가 Kafka에 이벤트를 보냅니다.

예를 들어:

```text
sent: user=kim service=api partition=0 offset=10
sent: user=kim service=search partition=1 offset=5
sent: user=lee service=api partition=2 offset=7
sent: user=park service=storage partition=0 offset=11
```

같은 결과가 나옵니다.

---

# 14. Consumer 결과 확인

Consumer 창에서는:

```text
Kafka Consumer started...
Waiting for messages...

================================
Received event
================================
topic     : usage-events
partition : 0
offset    : 10
user_id   : kim
service   : api
usage_type: request
quantity  : 10
```

처럼 나타납니다.

다음 메시지도:

```text
================================
Received event
================================
topic     : usage-events
partition : 1
offset    : 5
user_id   : kim
service   : search
usage_type: query
quantity  : 5
```

처럼 출력됩니다.

---

# 15. 여기서 중요한 변화

Phase 2-5에서는:

```text
Python Producer
      │
      ▼
Kafka
```

까지만 만들었습니다.

이제:

```text
Python Producer
      │
      ▼
Kafka
      │
      ▼
Python Consumer
```

가 완성됐습니다.

즉:

```text
Producer
   │
   │ JSON
   ▼
 Kafka
   │
   │ JSON
   ▼
Consumer
```

입니다.

---

# 16. Consumer에서 Python 객체가 된다는 것

Kafka에 저장된 데이터는 JSON 형태입니다.

```json
{
  "user_id": "kim",
  "service": "api",
  "usage_type": "request",
  "quantity": 10
}
```

Consumer에서는:

```python
event = message.value
```

를 통해 Python Dictionary가 됩니다.

따라서 일반 Python 코드처럼:

```python
user_id = event["user_id"]
service = event["service"]
quantity = event["quantity"]
```

를 사용할 수 있습니다.

예를 들어:

```python
cost = quantity * 100

print(
    f"{user_id} 사용량={quantity}, "
    f"예상비용={cost}"
)
```

처럼 처리할 수도 있습니다.

---

# 17. 간단한 비용 계산 추가

Consumer 코드를 조금 바꿔보겠습니다.

```python
import json

from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "usage-events",

    bootstrap_servers="kafka:9092",

    group_id="usage-consumer",

    auto_offset_reset="earliest",

    enable_auto_commit=True,

    value_deserializer=lambda value:
        json.loads(value.decode("utf-8"))
)


print("Kafka Consumer started...")
print("Waiting for messages...")


for message in consumer:

    event = message.value

    user_id = event.get("user_id")
    service = event.get("service")
    usage_type = event.get("usage_type")
    quantity = event.get("quantity", 0)

    # 임시 단가
    unit_price = 100

    cost = quantity * unit_price

    print()
    print("================================")
    print("Usage Event")
    print("================================")

    print(f"user_id    : {user_id}")
    print(f"service    : {service}")
    print(f"usage_type : {usage_type}")
    print(f"quantity   : {quantity}")
    print(f"cost       : {cost}")
    print("--------------------------------")
    print(f"partition  : {message.partition}")
    print(f"offset     : {message.offset}")
```

예를 들어:

```text
================================
Usage Event
================================
user_id    : kim
service    : api
usage_type : request
quantity   : 10
cost       : 1000
--------------------------------
partition  : 0
offset     : 12
```

가 됩니다.

물론 `100`이라는 단가는 학습용 임시 값입니다.

---

# 18. Offset도 다시 확인

Consumer가 메시지를 처리하면:

```text
partition
offset
```

정보를 가지고 있습니다.

예:

```text
partition = 0
offset = 12
```

이면:

```text
usage-events
      │
      └── Partition 0

           ...
           offset 10
           offset 11
           offset 12  ← Consumer가 처리
           offset 13
           offset 14
```

같은 개념입니다.

Consumer Group은 자신이 어디까지 읽었는지를 Offset으로 관리합니다.

---

# 19. Consumer를 종료했다가 다시 실행

Consumer 창에서:

```text
Ctrl + C
```

로 종료합니다.

그리고 다시:

```powershell
docker compose run --rm consumer
```

를 실행해 보세요.

여기서 중요한 현상이 나타날 수 있습니다.

앞에서 이미 읽은 메시지는 다시 나오지 않고, **해당 Consumer Group의 Offset 이후부터 읽게 됩니다.**

왜냐하면:

```text
group_id="usage-consumer"
```

이기 때문입니다.

Kafka는:

```text
usage-consumer
      │
      └── "나는 여기까지 읽었다"
```

라는 Offset 정보를 관리합니다.

---

# 20. 새로운 Consumer Group으로 실행하면?

이번에는 코드의:

```python
group_id="usage-consumer"
```

를:

```python
group_id="usage-consumer-2"
```

로 변경해 보세요.

그러면 Kafka 입장에서는 완전히 새로운 Consumer Group입니다.

```text
usage-events
      │
      ├── usage-consumer
      │
      └── usage-consumer-2
```

새로운 Group이므로 `earliest` 설정에 따라 기존 이벤트부터 읽을 수 있습니다.

이것이 Phase 2-4에서 배운:

> **Consumer Group이 다르면 같은 Kafka 이벤트를 독립적으로 소비할 수 있다.**

는 내용입니다.

---

# 21. Producer + Consumer 전체 그림

현재까지 구현한 것을 합치면:

```text
                         Docker Network
                              │
          ┌───────────────────┴──────────────────┐
          │                                      │
          ▼                                      ▼
┌──────────────────┐                   ┌─────────────────┐
│ Python Producer  │                   │ Python Consumer │
└────────┬─────────┘                   └────────▲────────┘
         │                                      │
         │ JSON                                 │ JSON
         ▼                                      │
              ┌──────────────────────┐          │
              │        Kafka         │──────────┘
              │                      │
              │    usage-events      │
              │   ┌──┬──┬──┐         │
              │   │P0│P1│P2│         │
              │   └──┴──┴──┘         │
              └──────────────────────┘
```

---

# 22. Phase 2-6에서 배운 것

| 개념                   | 의미                       |
| -------------------- | ------------------------ |
| `KafkaConsumer`      | Python Kafka Consumer    |
| `group_id`           | Consumer Group 지정        |
| `auto_offset_reset`  | 초기 Offset 위치             |
| `value_deserializer` | Kafka 데이터를 Python 객체로 변환 |
| `message.value`      | 실제 이벤트 데이터               |
| `message.partition`  | 이벤트가 들어 있는 Partition     |
| `message.offset`     | Partition 내 이벤트 위치       |

특히 다음 두 줄을 기억하세요.

### Producer

```python
value_serializer=lambda value:
    json.dumps(value).encode("utf-8")
```

```text
Python → JSON → bytes → Kafka
```

### Consumer

```python
value_deserializer=lambda value:
    json.loads(value.decode("utf-8"))
```

```text
Kafka → bytes → JSON → Python
```

이 둘이 서로 반대 방향의 변환입니다.

---

# 23. 현재 프로젝트에서 가장 중요한 변화

이제 프로젝트가 단순한 Keycloak 실습에서 **이벤트 기반 구조**로 발전했습니다.

현재:

```text
Keycloak
    │
    │ JWT
    ▼
FastAPI
```

그리고 별도로:

```text
Python Producer
    │
    ▼
Kafka
    │
    ▼
Python Consumer
```

를 구축했습니다.

다음 단계에서는 이 둘을 연결합니다.

```text
                         Phase 2-8
                             │
                             ▼
┌──────────┐     JWT     ┌──────────┐
│ Keycloak │────────────▶│ FastAPI  │
└──────────┘             └────┬─────┘
                              │
                              │ Kafka Producer
                              ▼
                       ┌─────────────┐
                       │    Kafka    │
                       │ usage-events│
                       └──────┬──────┘
                              │
                              │ Kafka Consumer
                              ▼
                       ┌─────────────┐
                       │  Consumer   │
                       └─────────────┘
```

**Phase 2-7에서는 `Key`를 사용해서 `user_id=kim`의 이벤트가 왜 같은 Partition으로 가는지, 그리고 Partition/Offset을 직접 확인하는 실습**을 진행하면 Phase 2의 핵심 개념이 거의 완성됩니다.
