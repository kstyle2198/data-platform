좋습니다. **Phase 2-3에서는 `usage-events` Topic에 실제 메시지를 넣고(Producer), 다시 읽어보는 것(Consumer)**까지 실습하겠습니다.

이번 실습을 끝내면 다음 구조를 직접 확인하게 됩니다.

```text
Producer
   │
   │ 메시지 전송
   ▼
Kafka
   │
   └── usage-events
         ├── Partition 0
         ├── Partition 1
         └── Partition 2
               │
               ▼
            Consumer
```

### 1. Kafka 컨테이너 접속

PowerShell에서:

```powershell
docker exec -it kafka bash
```

그리고 Topic이 있는지 확인합니다.

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list
```

다음이 보여야 합니다.

```text
usage-events
```

---

## 2. Consumer 먼저 실행

Kafka 컨테이너 안에서 다음을 실행합니다.

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic usage-events
```

그러면 화면이 멈춘 것처럼 보입니다.

```text
$
```

메시지가 들어오기를 기다리는 상태입니다.

**이 창은 그대로 두세요.**

---

## 3. 새로운 PowerShell 창을 하나 더 엽니다

두 번째 PowerShell에서 다시 Kafka 컨테이너에 접속합니다.

```powershell
docker exec -it kafka bash
```

이제 두 개의 터미널이 있습니다.

```text
터미널 1
┌──────────────────────────────┐
│ Kafka Consumer               │
│                              │
│ 메시지를 기다리는 중...       │
└──────────────────────────────┘


터미널 2
┌──────────────────────────────┐
│ Kafka Producer               │
│                              │
│ 메시지를 전송할 예정          │
└──────────────────────────────┘
```

---

# 4. Producer 실행

터미널 2에서:

```bash
/opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic usage-events
```

그러면 다음과 같이 메시지를 입력할 수 있습니다.

```text
>
```

여기에:

```text
hello kafka
```

입력하고 Enter를 누릅니다.

```text
> hello kafka
```

---

# 5. Consumer를 확인

터미널 1을 보면:

```text
hello kafka
```

가 나타납니다.

즉,

```text
Producer
   │
   │ "hello kafka"
   ▼
Kafka
   │
   │
   ▼
Consumer
   │
   ▼
"hello kafka"
```

가 실제로 동작한 것입니다.

---

# 6. 여러 메시지를 보내보기

Producer 터미널에서 다음을 차례대로 입력해 보세요.

```text
user=kim, service=api, amount=10
```

```text
user=kim, service=search, amount=20
```

```text
user=lee, service=api, amount=15
```

```text
user=park, service=storage, amount=30
```

Consumer 터미널에서는:

```text
user=kim, service=api, amount=10
user=kim, service=search, amount=20
user=lee, service=api, amount=15
user=park, service=storage, amount=30
```

처럼 보입니다.

---

# 7. 여기서 중요한 점

현재 입력한 메시지는 단순한 문자열입니다.

예를 들어:

```text
user=kim, service=api, amount=10
```

Kafka 입장에서는 이것을 특별히 해석하지 않습니다.

Kafka는 기본적으로:

> "이 데이터를 받아서 저장하고 전달한다."

정도의 역할을 합니다.

나중에는 이것을 JSON으로 바꿉니다.

예:

```json
{
  "user_id": "kim",
  "service": "api",
  "amount": 10
}
```

그리고 최종적으로는 실제 사용량 이벤트를 다음과 같이 만들 예정입니다.

```json
{
  "user_id": "kim",
  "service": "api",
  "usage_type": "request",
  "quantity": 10,
  "timestamp": "2026-09-26T14:00:00"
}
```

이 부분은 **Phase 2-8 이후 FastAPI와 Kafka를 연결할 때** 구현합니다.

---

# 8. Consumer를 다시 실행하면?

현재 Consumer를 `Ctrl+C`로 종료해 보겠습니다.

```text
Ctrl + C
```

그리고 다시:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic usage-events
```

이번에는 아까 입력했던 메시지가 안 보일 가능성이 높습니다.

왜 그럴까요?

Consumer가 이미 해당 메시지를 읽었고, Consumer Group의 offset이 진행되었기 때문입니다.

이것이 Kafka의 **Offset** 개념입니다.

---

# 9. Offset을 직접 확인해보기

이번에는 새로운 Consumer Group을 지정해서 실행해 보겠습니다.

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic usage-events \
  --group test-group \
  --from-beginning
```

여기서 중요한 옵션은:

```text
--group test-group
```

입니다.

그리고:

```text
--from-beginning
```

은 해당 Consumer Group 입장에서 처음부터 메시지를 읽겠다는 의미입니다.

그러면 앞에서 입력했던 메시지들이 다시 나타납니다.

```text
hello kafka
user=kim, service=api, amount=10
user=kim, service=search, amount=20
user=lee, service=api, amount=15
user=park, service=storage, amount=30
```

---

# 10. 이것이 Kafka의 핵심 특징

여기서 일반적인 메시지 큐와 다른 Kafka의 특징을 볼 수 있습니다.

```text
             Kafka
               │
               ▼
        usage-events
               │
       ┌───────┴────────┐
       │                │
       ▼                ▼
   Consumer A        Consumer B
   group-A           group-B
       │                │
       ▼                ▼
    offset            offset
```

Kafka에서는 메시지를 Consumer가 읽었다고 해서 바로 삭제하지 않습니다.

메시지는 Kafka에 남아 있고 Consumer Group이:

```text
"나는 여기까지 읽었다"
```

라는 **Offset**을 관리합니다.

---

# 11. Partition도 직접 확인해보기

Consumer를 종료합니다.

```text
Ctrl + C
```

그리고 Topic 정보를 확인합니다.

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic usage-events
```

앞에서 만들었듯이:

```text
Partition 0
Partition 1
Partition 2
```

가 있습니다.

---

# 12. 특정 Partition만 읽어보기

예를 들어 Partition 0을 직접 읽어볼 수 있습니다.

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic usage-events \
  --partition 0 \
  --from-beginning
```

다만 여기서 중요한 점이 있습니다.

우리가 메시지를 Producer에서 그냥 입력했기 때문에 Kafka가 메시지를 어느 Partition에 넣을지 결정합니다.

따라서 모든 메시지가 Partition 0에 있는 것은 아닙니다.

---

# 13. Offset을 직접 보려면

다음 명령도 실행해 보세요.

```bash
/opt/kafka/bin/kafka-run-class.sh \
  kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 \
  --topic usage-events
```

예를 들어:

```text
usage-events:0:2
usage-events:1:1
usage-events:2:1
```

처럼 나올 수 있습니다.

의미는:

```text
usage-events:0:2
               ↑
             offset 위치
```

즉 Partition마다 독립적으로 Offset을 관리합니다.

---

# 14. 오늘 실습에서 꼭 이해할 그림

이번 실습을 다음 그림으로 기억하면 됩니다.

```text
                  Producer
                     │
                     │
                     ▼
             ┌───────────────┐
             │     Kafka     │
             │               │
             │ usage-events  │
             │               │
             │ ┌───┐ ┌───┐ ┌───┐
             │ │ P0│ │ P1│ │ P2│
             │ └───┘ └───┘ └───┘
             └───────┬───────┘
                     │
                     ▼
                  Consumer
                     │
                     ▼
                  처리
```

그리고 Consumer Group을 추가하면:

```text
                     Kafka
                       │
                 usage-events
                       │
             ┌─────────┴─────────┐
             │                   │
             ▼                   ▼
        Consumer Group A    Consumer Group B
             │                   │
             ▼                   ▼
          Billing             Analytics
```

처럼 **같은 Kafka 데이터를 서로 다른 목적의 서비스가 각각 소비**할 수도 있습니다.

---

## Phase 2-3 완료 체크

다음 6개가 직접 동작하면 완료입니다.

* [x] `usage-events` Topic 생성
* [x] Console Producer 실행
* [x] Console Consumer 실행
* [x] 메시지 전송 확인
* [x] `--from-beginning`으로 기존 메시지 재조회
* [x] Partition / Offset 확인

### 다음 단계

**Phase 2-4에서는 조금 더 재미있는 실습을 합니다.**

```text
Kafka
 │
 └── usage-events
        │
        ├── Consumer Group A
        │      ├── Consumer 1
        │      └── Consumer 2
        │
        └── Consumer Group B
               └── Consumer 1
```

Consumer를 여러 개 띄워서 **"같은 Group이면 메시지를 나눠 갖고, 다른 Group이면 같은 메시지를 각각 받는다"**는 Kafka의 핵심 동작을 직접 확인하게 됩니다.
