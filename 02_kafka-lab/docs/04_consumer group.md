좋습니다. **Phase 2-4는 Kafka의 핵심인 Consumer Group과 Partition의 관계를 직접 확인하는 실습**입니다.

이번에는 단순히 메시지를 보내고 받는 것을 넘어,

> **같은 Consumer Group이면 메시지를 나눠서 처리하고, 다른 Consumer Group이면 같은 메시지를 각각 처리한다.**

를 직접 확인하겠습니다.

---

# Phase 2-4. Consumer Group + Partition 실습

## 1. 오늘 확인할 구조

현재 Topic은:

```text
usage-events
 ├── Partition 0
 ├── Partition 1
 └── Partition 2
```

입니다.

이번에는 Consumer를 2개 실행합니다.

```text
                    Kafka
                      │
                usage-events
               ┌──────┼──────┐
               ▼      ▼      ▼
              P0     P1     P2
               │      │      │
               └──┬───┘      │
                  │           │
                  ▼           ▼
             Consumer 1   Consumer 2
                  │           │
                  └─────┬─────┘
                        │
                   group-a
```

두 Consumer가 **같은 `group-a`**에 속하도록 만들겠습니다.

---

# 2. 먼저 기존 Consumer 종료

이전에 Consumer를 실행하고 있다면:

```text
Ctrl + C
```

로 종료합니다.

그리고 Kafka 컨테이너에 들어갑니다.

```powershell
docker exec -it kafka bash
```

---

# 3. Consumer 1 실행

첫 번째 터미널에서:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic usage-events \
  --group group-a
```

이 상태로 놔둡니다.

---

# 4. Consumer 2 실행

**PowerShell 창을 하나 더 열고** Kafka 컨테이너에 들어갑니다.

```powershell
docker exec -it kafka bash
```

그리고:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic usage-events \
  --group group-a
```

이제 다음 구조입니다.

```text
Consumer 1 ─┐
             ├── group-a
Consumer 2 ─┘
```

두 Consumer의 `--group` 이름이 똑같습니다.

---

# 5. Producer 실행

세 번째 PowerShell 창을 엽니다.

```powershell
docker exec -it kafka bash
```

그리고 Producer를 실행합니다.

```bash
/opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic usage-events
```

---

# 6. 메시지를 여러 개 전송

다음 메시지를 하나씩 입력합니다.

```text
message-1
message-2
message-3
message-4
message-5
message-6
message-7
message-8
message-9
```

그러면 재미있는 현상이 발생합니다.

Consumer 1과 Consumer 2가 **메시지를 나눠서 받습니다.**

예를 들어:

### Consumer 1

```text
message-1
message-3
message-5
message-7
message-9
```

### Consumer 2

```text
message-2
message-4
message-6
message-8
```

처럼 나올 수 있습니다.

단, **정확히 홀수/짝수로 나뉜다는 의미는 아닙니다.**

Kafka가 Partition을 Consumer들에게 배정하기 때문에 실제 결과는 다를 수 있습니다.

---

# 7. 핵심 개념

여기가 Phase 2-4의 가장 중요한 부분입니다.

Consumer 2개가:

```text
--group group-a
```

로 동일한 Group에 속해 있습니다.

따라서 Kafka는:

> "이 두 Consumer는 하나의 작업팀이다."

라고 생각합니다.

그래서 같은 메시지를 두 Consumer에게 모두 전달하지 않고 **Partition을 나눠줍니다.**

---

# 8. Partition 3개 + Consumer 2개

현재:

```text
Topic
 ├── P0
 ├── P1
 └── P2
```

Consumer:

```text
group-a
 ├── Consumer 1
 └── Consumer 2
```

라면 Kafka가 대략:

```text
Consumer 1 → P0, P2
Consumer 2 → P1
```

처럼 배정할 수 있습니다.

즉:

```text
              group-a
                 │
        ┌────────┴────────┐
        ▼                 ▼
   Consumer 1        Consumer 2
      │   │               │
      ▼   ▼               ▼
     P0   P2              P1
```

입니다.

---

# 9. Consumer를 하나 더 추가해보기

이번에는 세 번째 Consumer를 실행합니다.

```powershell
docker exec -it kafka bash
```

그리고:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic usage-events \
  --group group-a
```

이제:

```text
group-a

Consumer 1
Consumer 2
Consumer 3
```

3개의 Consumer가 있습니다.

Partition도 3개입니다.

```text
P0
P1
P2
```

따라서 이상적으로:

```text
Consumer 1 → P0
Consumer 2 → P1
Consumer 3 → P2
```

처럼 하나씩 담당할 수 있습니다.

이 과정을 **Consumer Group Rebalancing**이라고 합니다.

---

# 10. 메시지를 다시 보내보기

Producer에서:

```text
message-10
message-11
message-12
message-13
message-14
message-15
```

를 입력해 보세요.

3개의 Consumer 중 하나에서 메시지가 나타납니다.

중요한 점은:

```text
message-10
```

하나가 Consumer 1, 2, 3에게 **동시에 전달되는 것이 아니라는 것**입니다.

같은 Consumer Group에서는 하나의 메시지를 그룹 내 하나의 Consumer가 처리합니다.

---

# 11. Consumer 하나를 종료해보기

이번에는 Consumer 2에서:

```text
Ctrl + C
```

를 누릅니다.

그러면:

```text
group-a

Consumer 1
Consumer 3
```

만 남습니다.

Kafka는 Partition을 다시 배분합니다.

예를 들어:

```text
Consumer 1 → P0, P1
Consumer 3 → P2
```

같은 구조가 될 수 있습니다.

다시 메시지를 보내보세요.

```text
message-16
message-17
message-18
message-19
```

남아 있는 Consumer들이 메시지를 처리합니다.

이것이 Kafka의 **Consumer Group 기반 확장성과 장애 대응의 기본 원리**입니다.

---

# 12. 이번에는 다른 Consumer Group을 만들어보자

이제 중요한 비교를 해보겠습니다.

현재 `group-a`가 있습니다.

이번에는 새로운 Consumer를 실행합니다.

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic usage-events \
  --group group-b
```

이번에는:

```text
group-b
```

입니다.

따라서 구조가:

```text
                 usage-events
                      │
            ┌─────────┴─────────┐
            │                   │
            ▼                   ▼
        group-a              group-b
        /    \                   │
       C1    C2                  C3
```

가 됩니다.

---

# 13. 메시지를 보내보자

Producer에서:

```text
billing-test
```

를 입력합니다.

그러면 중요한 현상을 볼 수 있습니다.

### group-a

```text
billing-test
```

### group-b

```text
billing-test
```

**두 Group 모두 메시지를 받습니다.**

---

# 14. 왜 그런가?

Kafka에서:

```text
Consumer 1
Consumer 2
```

가 같은 Group이면:

```text
같은 작업팀
```

입니다.

따라서 메시지를 나눠서 처리합니다.

반면:

```text
group-a
group-b
```

는 서로 다른 작업팀입니다.

따라서 각각 독립적으로 Topic을 소비합니다.

그림으로 보면:

```text
                     Kafka
                       │
                usage-events
                       │
            ┌──────────┴──────────┐
            │                     │
            ▼                     ▼
       Consumer Group A      Consumer Group B
            │                     │
        Billing 처리           Analytics 처리
```

이 구조가 실제 데이터 플랫폼에서 매우 중요합니다.

---

# 15. 실제 사용량 시스템에 적용하면

우리 프로젝트에서는 나중에 다음처럼 만들 수 있습니다.

```text
                         Kafka
                           │
                     usage-events
                           │
             ┌─────────────┼─────────────┐
             │             │             │
             ▼             ▼             ▼
         Billing        Analytics      Monitoring
          Group           Group           Group
             │             │             │
             ▼             ▼             ▼
         비용 계산       통계 분석       모니터링
```

예를 들어 FastAPI가:

```json
{
  "user_id": "kim",
  "service": "api",
  "usage": 10
}
```

을 Kafka에 보내면,

**Billing Group**은:

```text
사용량 → 비용 계산
```

하고,

**Analytics Group**은:

```text
사용량 → 통계/분석
```

을 하고,

**Monitoring Group**은:

```text
사용량 → 모니터링
```

을 할 수 있습니다.

하나의 이벤트를 여러 시스템에서 독립적으로 사용할 수 있는 것입니다.

---

# 16. Consumer Group과 Partition 관계 정리

현재 Topic:

```text
usage-events
 ├── P0
 ├── P1
 └── P2
```

### Consumer 1개

```text
C1 → P0 + P1 + P2
```

### Consumer 2개

```text
C1 → P0 + P1
C2 → P2
```

### Consumer 3개

```text
C1 → P0
C2 → P1
C3 → P2
```

### Consumer 4개

```text
C1 → P0
C2 → P1
C3 → P2
C4 → 할 일 없음
```

따라서 **Consumer 수를 무조건 늘린다고 처리량이 계속 증가하는 것은 아닙니다.**

현재 Partition이 3개이므로 같은 Consumer Group에서는 최대 3개의 Consumer가 동시에 Partition을 담당할 수 있습니다.

---

# 17. Phase 2-4 핵심 정리

| 상황                       | 결과                        |
| ------------------------ | ------------------------- |
| 같은 Group + 여러 Consumer   | 메시지를 나눠서 처리               |
| 다른 Group                 | 각 Group이 같은 메시지를 독립적으로 처리 |
| Partition 3 + Consumer 3 | 최대한 1:1 배치 가능             |
| Partition 3 + Consumer 4 | Consumer 1개는 대기           |
| Consumer 장애              | Rebalancing 발생            |
| Consumer 추가              | Partition 재배분             |

가장 중요한 문장은 이것입니다.

> **Kafka에서 병렬 처리의 기본 단위는 Partition이고, Consumer Group은 그 Partition들을 나눠서 처리하는 작업 단위입니다.**

---

## Phase 2 전체 흐름에서 현재 위치

```text
Phase 2-1
Kafka 개념
   ↓
Phase 2-2
Kafka Docker 실행
   ↓
Phase 2-3
Producer / Consumer 기본 실습
   ↓
★ Phase 2-4
Consumer Group + Partition
   ↓
Phase 2-5
Python Producer
   ↓
Phase 2-6
Python Consumer
   ↓
Phase 2-7
Consumer Group 심화
   ↓
Phase 2-8
FastAPI → Kafka 연결
```

다음 **Phase 2-5에서는 Kafka CLI가 아니라 Python 코드로 `usage-events`에 JSON 사용량 이벤트를 보내는 Producer**를 만들어 보겠습니다.
