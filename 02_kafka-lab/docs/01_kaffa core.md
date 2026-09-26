좋습니다. **Phase 2-1은 Kafka를 실제로 설치하기 전에 Kafka의 핵심 구조를 이해하는 단계**입니다.

이번 단계에서는 코드를 많이 작성하기보다 **Broker → Topic → Partition → Producer → Consumer → Offset → Consumer Group**의 관계를 이해하는 것이 중요합니다.

# Phase 2-1. Kafka 기본 개념

## 1. 먼저 전체 그림

Kafka를 아주 단순하게 보면 다음과 같습니다.

```text
Producer
   │
   │ 메시지
   ▼
┌──────────────────────┐
│        Kafka         │
│                      │
│   Topic: usage-events│
│                      │
│   Partition 0        │
│   Partition 1        │
│   Partition 2        │
└──────────┬───────────┘
           │
           ▼
       Consumer
```

우리 프로젝트에서는:

```text
FastAPI
   │
   │ 사용량 이벤트
   ▼
Kafka
   │
   │ 사용량 이벤트
   ▼
Consumer
```

가 됩니다.

---

# 2. Kafka Broker

**Broker는 Kafka 서버입니다.**

Docker 환경에서는 Kafka 컨테이너 하나가 하나의 Broker가 됩니다.

```text
┌─────────────────┐
│ Kafka Broker    │
│                 │
│ Kafka Server    │
└─────────────────┘
```

이번 학습에서는 처음에는 Kafka Broker 하나만 사용합니다.

```text
Docker
│
├── Keycloak
├── FastAPI
└── Kafka
```

Kafka를 실행하면 Kafka가 메시지를 받아서 저장하고 Consumer가 읽을 수 있도록 제공합니다.

---

# 3. Topic

Topic은 **메시지를 분류해서 저장하는 논리적인 이름**입니다.

예를 들어 우리 프로젝트에는:

```text
usage-events
```

라는 Topic을 만들 것입니다.

```text
Kafka
│
├── usage-events
├── login-events
└── error-events
```

이번에는 하나만 사용합니다.

```text
Kafka
│
└── usage-events
```

여기에 서비스 사용량 이벤트를 넣습니다.

예:

```json
{
  "username": "kim",
  "service": "llm",
  "quantity": 100
}
```

---

# 4. Producer

Producer는 **Kafka에 메시지를 보내는 프로그램**입니다.

우리 프로젝트에서는 **FastAPI가 Producer 역할**을 합니다.

```text
FastAPI
   │
   │ Producer
   │
   ▼
Kafka
```

예를 들어 `kim`이 LLM을 100 token 사용했다고 하면:

```json
{
  "username": "kim",
  "service": "llm",
  "quantity": 100
}
```

을 Kafka의 `usage-events` Topic에 보냅니다.

즉:

```text
FastAPI
   │
   │ send()
   ▼
usage-events
```

입니다.

---

# 5. Consumer

Consumer는 **Kafka에서 메시지를 읽는 프로그램**입니다.

```text
Kafka
   │
   │ message
   ▼
Consumer
```

예를 들어:

```text
Consumer

Received event:
username = kim
service  = llm
quantity = 100
```

처럼 처리할 수 있습니다.

우리 Phase 2에서는 Python Consumer를 직접 만들어 봅니다.

---

# 6. Producer와 Consumer 관계

가장 중요한 기본 구조입니다.

```text
              메시지 전송
FastAPI ──────────────────▶ Kafka
                              │
                              │
                              ▼
                          Consumer
```

여기서 중요한 것은 **FastAPI와 Consumer가 직접 통신하지 않는다는 것**입니다.

잘못 이해하면:

```text
FastAPI ─────────▶ Consumer
```

처럼 생각하기 쉽습니다.

Kafka를 사용하면:

```text
FastAPI ─────────▶ Kafka ─────────▶ Consumer
             저장/중계
```

가 됩니다.

이것이 Kafka를 사용하는 중요한 이유 중 하나입니다.

---

# 7. Partition

이제 Kafka에서 가장 중요한 개념 중 하나입니다.

Topic은 내부적으로 여러 개의 **Partition**으로 나눌 수 있습니다.

예를 들어:

```text
usage-events
│
├── Partition 0
├── Partition 1
└── Partition 2
```

입니다.

각 Partition에는 메시지가 순서대로 저장됩니다.

```text
Partition 0

event A
event B
event C
event D
```

여기서 중요한 것은:

> **하나의 Partition 안에서는 메시지 순서가 유지됩니다.**

---

# 8. 왜 Partition을 사용하는가?

메시지가 아주 많아지면 하나의 저장 공간만 사용하는 것은 부담이 됩니다.

예를 들어:

```text
100만 건의 이벤트
```

가 발생한다고 생각해 보겠습니다.

Partition 하나:

```text
Kafka
│
└── Partition 0
       ↑
    모든 메시지
```

보다는:

```text
Kafka
│
├── Partition 0
├── Partition 1
└── Partition 2
```

로 나누면 여러 Consumer가 병렬로 처리할 수 있습니다.

```text
Partition 0 ──▶ Consumer A
Partition 1 ──▶ Consumer B
Partition 2 ──▶ Consumer C
```

이것이 Kafka의 **확장성**과 관련된 중요한 개념입니다.

---

# 9. Offset

Kafka의 또 하나의 핵심 개념입니다.

Partition 안의 메시지는 번호를 가집니다.

```text
Partition 0

Offset 0 → event A
Offset 1 → event B
Offset 2 → event C
Offset 3 → event D
Offset 4 → event E
```

이 번호가 **Offset**입니다.

Consumer가:

```text
Offset 0
Offset 1
Offset 2
```

까지 읽었다면 어디까지 읽었는지 추적할 수 있습니다.

```text
Consumer
   │
   └── 현재 Offset = 3
```

따라서 Consumer가 잠시 중단되었다가 다시 시작해도 이어서 처리할 수 있습니다.

---

# 10. Kafka는 메시지를 바로 삭제하지 않는다

이 부분이 중요합니다.

일반적인 메시지 큐를 처음 접하면:

```text
Producer
   ↓
Queue
   ↓
Consumer
   ↓
메시지 삭제
```

라고 생각하기 쉽습니다.

Kafka는 기본적으로 이런 방식이 아닙니다.

```text
Producer
   ↓
Kafka
   ↓
Partition
   ↓
메시지 저장
```

Consumer가 읽었다고 해서 바로 메시지가 삭제되는 것이 아닙니다.

Kafka는 **로그처럼 메시지를 순서대로 저장**합니다.

그래서 Consumer는 자신의 Offset을 기준으로 메시지를 읽습니다.

---

# 11. Consumer Group

이제 Kafka의 매우 중요한 개념입니다.

Consumer가 여러 개 있다고 생각해 보겠습니다.

```text
Kafka
│
├── Partition 0
├── Partition 1
└── Partition 2
```

Consumer:

```text
Consumer A
Consumer B
Consumer C
```

그리고 이들을:

```text
Group: usage-consumers
```

로 묶을 수 있습니다.

그러면 Kafka가 Partition을 Consumer들에게 분배합니다.

```text
Partition 0 ──▶ Consumer A
Partition 1 ──▶ Consumer B
Partition 2 ──▶ Consumer C
```

즉 **같은 Consumer Group 안에서는 메시지를 나누어 처리**할 수 있습니다.

---

# 12. Consumer Group이 왜 필요한가?

사용량 이벤트가 매우 많다고 생각해 보겠습니다.

```text
Kafka
│
├── Partition 0
├── Partition 1
├── Partition 2
├── Partition 3
├── Partition 4
└── Partition 5
```

Consumer 하나만 사용하면:

```text
Kafka
   │
   ▼
Consumer A
```

입니다.

Consumer를 늘리면:

```text
Kafka
│
├── Consumer A
├── Consumer B
└── Consumer C
```

로 병렬 처리할 수 있습니다.

Consumer Group을 사용하면 Kafka가 Partition을 적절하게 분배합니다.

---

# 13. Partition 수와 Consumer 수의 관계

여기서 중요한 규칙이 있습니다.

예를 들어:

```text
Partition = 3
Consumer = 3
```

이면:

```text
P0 → C0
P1 → C1
P2 → C2
```

처럼 처리할 수 있습니다.

그런데:

```text
Partition = 3
Consumer = 5
```

이면 Consumer 5개 모두가 일을 할 수 없습니다.

대략:

```text
P0 → C0
P1 → C1
P2 → C2

C3 → 대기
C4 → 대기
```

따라서 **같은 Consumer Group에서는 Partition 수가 병렬 처리 수준의 중요한 상한**이 됩니다.

---

# 14. Consumer Group이 다르면?

이것도 매우 중요합니다.

예를 들어:

```text
Kafka
│
└── usage-events
       │
       ├── Billing Group
       │
       └── Analytics Group
```

두 Group이 있으면 각각 독립적으로 이벤트를 읽습니다.

```text
usage-events
      │
      ├──────────────▶ Billing Consumer
      │
      └──────────────▶ Analytics Consumer
```

예를 들어:

### Billing

```text
usage-events
      ↓
Billing Service
      ↓
사용량 비용 계산
```

### Analytics

```text
usage-events
      ↓
Analytics Service
      ↓
사용량 분석
```

동일한 이벤트를 **서로 다른 목적으로 독립적으로 소비**할 수 있습니다.

이것이 Kafka의 아주 강력한 특징입니다.

---

# 15. 우리 프로젝트에 적용하면

우리가 만들 시스템을 다시 보면:

```text
                   Keycloak
                      │
                      │ JWT
                      ▼
                    FastAPI
                      │
                      │ Producer
                      ▼
               ┌───────────────┐
               │     Kafka     │
               │               │
               │ usage-events  │
               │               │
               │ P0 P1 P2      │
               └───────┬───────┘
                       │
                       │
              ┌────────┴────────┐
              ▼                 ▼
       Billing Group      Analytics Group
              │                 │
              ▼                 ▼
        비용 계산           데이터 분석
```

이렇게 확장할 수 있습니다.

---

# 16. Phase 2-1에서 반드시 이해해야 할 것

이번 단계에서는 다음 7개를 정확히 이해하면 됩니다.

| 개념                 | 쉽게 말하면                        |
| ------------------ | ----------------------------- |
| **Broker**         | Kafka 서버                      |
| **Topic**          | 메시지 종류별 저장 공간                 |
| **Partition**      | Topic을 나눈 실제 저장/처리 단위         |
| **Producer**       | 메시지를 Kafka에 보내는 쪽             |
| **Consumer**       | Kafka에서 메시지를 읽는 쪽             |
| **Offset**         | Partition에서 메시지의 위치           |
| **Consumer Group** | 여러 Consumer를 하나의 처리 그룹으로 묶은 것 |

특히 다음 그림을 기억하시면 됩니다.

```text
                 Producer
                    │
                    │ message
                    ▼
              ┌─────────────┐
              │    Topic    │
              │ usage-events│
              └──────┬──────┘
                     │
            ┌────────┼────────┐
            ▼        ▼        ▼
           P0       P1       P2
            │        │        │
            └────────┼────────┘
                     │
                 Consumer
                 Group
```

그리고 **Phase 2-2부터 실제 Docker Kafka를 띄우고 이 구조를 직접 확인**하게 됩니다.

다음 단계에서는 이론을 더 늘리지 않고 **현재 Keycloak + FastAPI Docker Compose에 Kafka를 추가하는 `docker-compose.yml`을 만들고, `usage-events` Topic을 직접 생성하는 실습**으로 넘어가면 됩니다.
