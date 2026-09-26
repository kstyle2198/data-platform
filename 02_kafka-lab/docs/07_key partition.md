좋습니다. **Phase 2-7에서는 Kafka의 `Key → Partition → Offset` 관계를 직접 실습**해보겠습니다.

이번 단계는 Phase 2에서 가장 중요한 실습 중 하나입니다.

---

# Phase 2-7. Key / Partition / Offset 심화

## 1. 이번 실습의 목표

앞에서는 Producer가 메시지를 보낼 때:

```text
message-1 → Partition 0
message-2 → Partition 2
message-3 → Partition 1
```

처럼 Kafka가 메시지를 분산시키는 것을 확인했습니다.

이번에는 **Key를 지정**합니다.

예를 들어:

```text
user_id = kim
```

을 Kafka Key로 사용합니다.

그러면:

```text
kim → Partition 1
kim → Partition 1
kim → Partition 1
```

처럼 **같은 Key의 메시지가 같은 Partition으로 들어가는 것**을 확인합니다.

---

# 2. 왜 Key가 중요한가?

우리 프로젝트의 사용량 이벤트를 생각해봅시다.

```json
{
  "user_id": "kim",
  "service": "api",
  "quantity": 10
}
```

사용자 `kim`의 이벤트가 여러 개 발생합니다.

```text
kim → API 10회
kim → API 20회
kim → Search 5회
kim → Storage 30GB
```

이 이벤트들을 여러 Partition에 무작위로 보내면:

```text
P0 → kim 이벤트
P1 → kim 이벤트
P2 → kim 이벤트
```

가 될 수 있습니다.

그러면 **kim의 이벤트 순서를 관리하기 어려워집니다.**

반면 `user_id`를 Key로 사용하면:

```text
key = kim
      ↓
   Partition 1

kim 이벤트 1 → P1
kim 이벤트 2 → P1
kim 이벤트 3 → P1
```

처럼 같은 사용자 이벤트를 같은 Partition으로 보낼 수 있습니다.

Kafka에서는 **같은 Partition 내부에서 메시지 순서가 보장**되기 때문에 이 구조가 중요합니다.

---

# 3. Producer 코드 수정

Phase 2-5에서 만든:

```text
producer/producer.py
```

를 수정합니다.

다음 코드로 바꿔보겠습니다.

```python
import json

from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="kafka:9092",

    key_serializer=lambda key:
        key.encode("utf-8"),

    value_serializer=lambda value:
        json.dumps(value).encode("utf-8")
)


TOPIC = "usage-events"


events = [
    {
        "user_id": "kim",
        "service": "api",
        "quantity": 10
    },
    {
        "user_id": "kim",
        "service": "search",
        "quantity": 5
    },
    {
        "user_id": "kim",
        "service": "storage",
        "quantity": 20
    },
    {
        "user_id": "lee",
        "service": "api",
        "quantity": 30
    },
    {
        "user_id": "lee",
        "service": "search",
        "quantity": 15
    },
    {
        "user_id": "park",
        "service": "api",
        "quantity": 50
    }
]


for event in events:

    key = event["user_id"]

    future = producer.send(
        TOPIC,
        key=key,
        value=event
    )

    metadata = future.get(timeout=10)

    print(
        f"user={key} "
        f"service={event['service']} "
        f"partition={metadata.partition} "
        f"offset={metadata.offset}"
    )


producer.flush()
producer.close()
```

---

# 4. 이번 코드에서 달라진 부분

가장 중요한 부분은 이것입니다.

```python
key = event["user_id"]
```

예를 들어:

```text
kim
```

을 Key로 사용합니다.

그리고:

```python
producer.send(
    TOPIC,
    key=key,
    value=event
)
```

로 전송합니다.

즉:

```text
Key
 ↓
kim
```

이 됩니다.

---

# 5. `key_serializer`

다음 코드도 새로 추가되었습니다.

```python
key_serializer=lambda key:
    key.encode("utf-8")
```

Kafka는 Key도 bytes 형태로 보낼 수 있기 때문에:

```text
"kim"
   ↓
UTF-8
   ↓
bytes
```

로 변환합니다.

Value는 기존과 마찬가지로:

```text
Python Dictionary
      ↓
JSON
      ↓
bytes
```

입니다.

---

# 6. Producer 실행

Producer 이미지를 다시 빌드합니다.

```powershell
docker compose build producer
```

그리고:

```powershell
docker compose run --rm producer
```

실행합니다.

예를 들어 다음과 같은 결과가 나올 수 있습니다.

```text
user=kim service=api partition=1 offset=10
user=kim service=search partition=1 offset=11
user=kim service=storage partition=1 offset=12
user=lee service=api partition=0 offset=20
user=lee service=search partition=0 offset=21
user=park service=api partition=2 offset=30
```

여기서 **아주 중요한 현상**을 발견할 수 있습니다.

```text
kim → P1
kim → P1
kim → P1

lee → P0
lee → P0

park → P2
```

처럼 같은 Key가 같은 Partition으로 들어갑니다.

---

# 7. 왜 같은 Partition인가?

Kafka는 Key를 기반으로 Partition을 결정합니다.

개념적으로:

```text
partition = hash(key) % partition_count
```

와 비슷한 방식으로 Partition을 결정합니다.

현재 Partition이 3개이므로:

```text
kim
 ↓
hash(kim)
 ↓
Partition 1
```

과 같은 방식입니다.

정확한 내부 구현을 직접 계산할 필요는 없습니다.

중요한 것은:

> **같은 Key → 같은 Partition**

이라는 것입니다.

---

# 8. 중요한 주의사항

다음 문장은 정확하게 기억해야 합니다.

> **같은 Key의 메시지는 일반적으로 같은 Partition으로 보내집니다.**

하지만 Partition 수를 변경하거나 Producer의 Partitioning 전략을 변경하면 결과가 달라질 수 있습니다.

현재 실습에서는:

```text
usage-events
 ├── P0
 ├── P1
 └── P2
```

로 고정되어 있으므로 쉽게 확인할 수 있습니다.

---

# 9. Consumer도 Key를 확인해보자

이번에는 Consumer에서 Key까지 출력해 보겠습니다.

`consumer/consumer.py`를 다음처럼 수정합니다.

```python
import json

from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "usage-events",

    bootstrap_servers="kafka:9092",

    group_id="key-test-consumer",

    auto_offset_reset="earliest",

    enable_auto_commit=True,

    key_deserializer=lambda key:
        key.decode("utf-8")
        if key
        else None,

    value_deserializer=lambda value:
        json.loads(value.decode("utf-8"))
)


print("Consumer started...")


for message in consumer:

    event = message.value

    print()
    print("================================")
    print("Received Event")
    print("================================")

    print(f"key       : {message.key}")
    print(f"user_id   : {event.get('user_id')}")
    print(f"service   : {event.get('service')}")
    print(f"quantity  : {event.get('quantity')}")

    print(f"partition : {message.partition}")
    print(f"offset    : {message.offset}")
```

---

# 10. Consumer 실행

먼저 빌드합니다.

```powershell
docker compose build consumer
```

그리고:

```powershell
docker compose run --rm consumer
```

실행합니다.

예:

```text
================================
Received Event
================================
key       : kim
user_id   : kim
service   : api
quantity  : 10
partition : 1
offset    : 10
```

다음:

```text
================================
Received Event
================================
key       : kim
user_id   : kim
service   : search
quantity  : 5
partition : 1
offset    : 11
```

그리고:

```text
================================
Received Event
================================
key       : kim
user_id   : kim
service   : storage
quantity  : 20
partition : 1
offset    : 12
```

를 확인할 수 있습니다.

---

# 11. 여기서 Offset을 자세히 보자

예를 들어:

```text
kim / api
Partition = 1
Offset = 10

kim / search
Partition = 1
Offset = 11

kim / storage
Partition = 1
Offset = 12
```

이라면:

```text
Partition 1
────────────────────────────
Offset 10 → kim / api
Offset 11 → kim / search
Offset 12 → kim / storage
```

입니다.

따라서 Partition 1 안에서는:

```text
10 → 11 → 12
```

순서가 유지됩니다.

---

# 12. 하지만 Partition이 다르면?

예를 들어:

```text
P0
 └── Offset 20 → lee / api

P1
 ├── Offset 10 → kim / api
 ├── Offset 11 → kim / search
 └── Offset 12 → kim / storage

P2
 └── Offset 30 → park / api
```

입니다.

중요한 점은:

```text
P0의 Offset 20
P1의 Offset 10
P2의 Offset 30
```

은 서로 비교해서 전체 순서를 정할 수 없다는 것입니다.

Kafka가 보장하는 것은:

> **하나의 Partition 내부의 순서**

입니다.

---

# 13. 이것이 왜 중요한가?

예를 들어 `kim`의 사용량 이벤트가:

```text
1. API 10회
2. API 20회
3. API 30회
```

발생했다고 합시다.

같은 Partition에 있다면:

```text
P1

Offset 100 → API 10
Offset 101 → API 20
Offset 102 → API 30
```

이므로 순서를 유지할 수 있습니다.

따라서 `user_id`를 Key로 사용하는 것이 의미가 있습니다.

---

# 14. 실습: 같은 사용자의 이벤트 10개 보내기

Producer의 이벤트를 다음처럼 바꿔보겠습니다.

```python
for i in range(1, 11):

    event = {
        "user_id": "kim",
        "service": "api",
        "quantity": i * 10
    }

    future = producer.send(
        TOPIC,
        key="kim",
        value=event
    )

    metadata = future.get(timeout=10)

    print(
        f"user={event['user_id']} "
        f"quantity={event['quantity']} "
        f"partition={metadata.partition} "
        f"offset={metadata.offset}"
    )
```

실행:

```powershell
docker compose build producer
docker compose run --rm producer
```

결과는 대략:

```text
user=kim quantity=10 partition=1 offset=20
user=kim quantity=20 partition=1 offset=21
user=kim quantity=30 partition=1 offset=22
user=kim quantity=40 partition=1 offset=23
...
user=kim quantity=100 partition=1 offset=29
```

처럼 됩니다.

**모든 `kim` 이벤트가 같은 Partition에 들어가는 것을 확인**할 수 있습니다.

---

# 15. 서로 다른 사용자 보내기

이번에는:

```python
users = ["kim", "lee", "park"]

for user in users:

    for i in range(3):

        event = {
            "user_id": user,
            "service": "api",
            "quantity": (i + 1) * 10
        }

        future = producer.send(
            TOPIC,
            key=user,
            value=event
        )

        metadata = future.get(timeout=10)

        print(
            f"user={user} "
            f"partition={metadata.partition} "
            f"offset={metadata.offset}"
        )
```

결과가 예를 들어:

```text
kim  → P1
kim  → P1
kim  → P1

lee  → P0
lee  → P0
lee  → P0

park → P2
park → P2
park → P2
```

처럼 나올 수 있습니다.

---

# 16. 실무에서는 이것을 어떻게 사용하는가?

우리 사용량 시스템에서는 다음과 같이 설계할 수 있습니다.

```text
                    Kafka
                      │
                usage-events
                      │
       ┌──────────────┼──────────────┐
       ▼              ▼              ▼
      P0             P1             P2
       │              │              │
     users          users          users
```

예를 들어:

```text
kim  → P1
lee  → P0
park → P2
```

그리고 Consumer Group에서는:

```text
Consumer 1 → P0
Consumer 2 → P1
Consumer 3 → P2
```

가 될 수 있습니다.

따라서:

```text
Consumer 1 → lee 이벤트
Consumer 2 → kim 이벤트
Consumer 3 → park 이벤트
```

처럼 병렬 처리할 수 있습니다.

---

# 17. 그런데 중요한 한계가 있습니다

예를 들어:

```text
kim
lee
park
choi
```

네 명의 사용자가 있는데 Partition이 3개라면:

```text
kim  → P1
lee  → P0
park → P2
choi → P1
```

처럼 여러 사용자가 같은 Partition을 사용할 수 있습니다.

즉:

```text
Partition 1
 ├── kim
 ├── kim
 ├── choi
 └── choi
```

가 가능합니다.

**Key와 Partition은 1:1 관계가 아닙니다.**

정확하게는:

```text
여러 Key
   ↓
하나의 Partition
```

이 가능합니다.

---

# 18. 오늘 실습의 핵심 그림

이번 Phase를 다음처럼 기억하면 됩니다.

```text
                  Key
                   │
                   ▼
              user_id
                   │
                   ▼
             Kafka Partitioner
                   │
          ┌────────┼────────┐
          ▼        ▼        ▼
         P0       P1       P2
          │        │        │
        lee       kim      park
        lee       kim      park
```

그리고 각 Partition 내부에는 Offset이 있습니다.

```text
P1
────────────────────────
Offset 10 → kim/api
Offset 11 → kim/search
Offset 12 → kim/storage
Offset 13 → kim/api
```

---

# 19. Phase 2-7 핵심 정리

| 개념           | 의미                               |
| ------------ | -------------------------------- |
| Key          | 메시지를 어떤 Partition에 보낼지 결정하는 데 사용 |
| `user_id`    | 우리 프로젝트에서 사용할 수 있는 좋은 Key 후보     |
| Partition    | 실제 메시지가 저장되는 단위                  |
| Offset       | Partition 내부 메시지 위치              |
| 같은 Key       | 일반적으로 같은 Partition               |
| 같은 Partition | 메시지 순서 보장                        |
| 다른 Partition | 서로 간의 전체 순서는 보장되지 않음             |
| 여러 Key       | 하나의 Partition에 들어갈 수 있음          |

가장 중요한 문장은 세 개입니다.

> **1. 같은 Key의 메시지는 같은 Partition으로 보내는 것이 일반적이다.**

> **2. Kafka가 보장하는 순서는 Partition 내부의 순서다.**

> **3. Consumer Group은 Partition을 나눠서 병렬 처리한다.**

---

# 20. Phase 2에서 지금까지 배운 것을 연결하면

```text
Phase 2-1
Kafka 개념
   │
   ▼
Phase 2-2
Kafka Docker
   │
   ▼
Phase 2-3
CLI Producer / Consumer
   │
   ▼
Phase 2-4
Consumer Group
   │
   ▼
Phase 2-5
Python Producer
   │
   ▼
Phase 2-6
Python Consumer
   │
   ▼
★ Phase 2-7
Key → Partition → Offset
   │
   ▼
Phase 2-8
FastAPI → Kafka
```

이제 Kafka 자체의 핵심 개념은 상당 부분 갖춰졌습니다.

**다음 Phase 2-8에서는 지금까지 별도로 만들어 놓은 `FastAPI + Keycloak`과 Kafka를 실제로 연결합니다.**

최종적으로:

```text
사용자
  │
  │ JWT
  ▼
Keycloak
  │
  ▼
FastAPI
  │
  │ user_id + service + usage
  ▼
Kafka
  │
  ▼
usage-events
```

가 되며, 여기서부터 **Keycloak의 `kim` 사용자 정보가 실제 Kafka 사용량 이벤트의 `user_id`로 들어가는 구조**를 구현하게 됩니다.
