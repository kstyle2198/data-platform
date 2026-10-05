import json
import os

from kafka import KafkaConsumer

from detector.brute_force import (
    BruteForceDetector,
)
from decision.risk_engine import (
    RiskEngine,
)

from decision.policy_engine import (
    PolicyEngine,
)



# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:29092",
)

SECURITY_TOPIC = os.getenv(
    "SECURITY_TOPIC",
    "security-events",
)

CONSUMER_GROUP = os.getenv(
    "SECURITY_CONSUMER_GROUP",
    "security-defense-agent",
)

BRUTE_FORCE_THRESHOLD = int(
    os.getenv(
        "BRUTE_FORCE_THRESHOLD",
        "5",
    )
)

BRUTE_FORCE_WINDOW_SECONDS = int(
    os.getenv(
        "BRUTE_FORCE_WINDOW_SECONDS",
        "60",
    )
)


# ============================================================
# Kafka Consumer
# ============================================================

consumer = KafkaConsumer(

    SECURITY_TOPIC,

    bootstrap_servers=(
        KAFKA_BOOTSTRAP_SERVERS
    ),

    group_id=CONSUMER_GROUP,

    auto_offset_reset="earliest",

    enable_auto_commit=True,

    value_deserializer=lambda value:
        json.loads(
            value.decode("utf-8")
        ),
)


# ============================================================
# Detector
# ============================================================

detector = BruteForceDetector(

    threshold=(
        BRUTE_FORCE_THRESHOLD
    ),

    window_seconds=(
        BRUTE_FORCE_WINDOW_SECONDS
    ),
)

risk_engine = RiskEngine()

policy_engine = PolicyEngine()

# ============================================================
# Start
# ============================================================

print(
    "========================================"
)

print(
    " Security Defense Agent"
)

print(
    "========================================"
)

print(
    f"Kafka     : "
    f"{KAFKA_BOOTSTRAP_SERVERS}"
)

print(
    f"Topic     : "
    f"{SECURITY_TOPIC}"
)

print(
    f"Threshold : "
    f"{BRUTE_FORCE_THRESHOLD}"
)

print(
    f"Window    : "
    f"{BRUTE_FORCE_WINDOW_SECONDS}s"
)

print(
    "========================================"
)


# ============================================================
# Consume
# ============================================================

for message in consumer:

    try:

        event = message.value

        print(
            "[Security Event] "
            f"topic={message.topic} "
            f"partition={message.partition} "
            f"offset={message.offset}"
        )

        print(
            f"  event_id="
            f"{event.get('event_id')}"
        )

        print(
            f"  type="
            f"{event.get('event_type')}"
        )

        print(
            f"  user="
            f"{event.get('username')}"
        )

        print(
            f"  ip="
            f"{event.get('ip_address')}"
        )

        incident = detector.process(event)

        if not incident:
            continue


        risk_result = risk_engine.calculate(
            incident
        )


        print(
            "[Risk] "
            f"user={risk_result['username']} "
            f"score={risk_result['risk_score']} "
            f"level={risk_result['risk_level']}"
        )

        decision = policy_engine.decide(
            risk_result
        )


        print(
            "[Policy] "
            f"action={decision['action']} "
            f"reason={decision['reason']}"
        )

        if decision["action"] == "DISABLE_USER":

            print(
                "[DRY RUN] "
                f"Would disable user: "
                f"{risk_result['username']}"
            )

    except Exception as exc:

        print(
            "[Agent Error] "
            f"{exc}"
        )