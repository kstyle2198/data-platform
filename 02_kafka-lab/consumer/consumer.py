import os
import json

from kafka import KafkaConsumer


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "usage-events"
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "usage-consumer"
)


# ============================================================
# Kafka Consumer
# ============================================================

consumer = KafkaConsumer(

    KAFKA_TOPIC,

    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

    group_id=KAFKA_GROUP_ID,

    auto_offset_reset="earliest",

    enable_auto_commit=True,

    # Kafka Key
    key_deserializer=lambda key:
        key.decode("utf-8")
        if key
        else None,

    # Kafka Value
    value_deserializer=lambda value:
        json.loads(
            value.decode("utf-8")
        )
)


# ============================================================
# Start
# ============================================================

print("========================================")
print("Kafka Consumer Started")
print("========================================")

print(f"bootstrap : {KAFKA_BOOTSTRAP_SERVERS}")
print(f"topic     : {KAFKA_TOPIC}")
print(f"group     : {KAFKA_GROUP_ID}")

print()
print("Waiting for messages...")
print()


# ============================================================
# Consume
# ============================================================

for message in consumer:

    event = message.value

    print()
    print("========================================")
    print("Received Usage Event")
    print("========================================")

    print(
        f"topic     : {message.topic}"
    )

    print(
        f"partition : {message.partition}"
    )

    print(
        f"offset    : {message.offset}"
    )

    print(
        f"key       : {message.key}"
    )

    print(
        f"user_id   : {event.get('user_id')}"
    )

    print(
        f"username  : {event.get('username')}"
    )

    print(
        f"service   : {event.get('service')}"
    )

    print(
        f"usage_type: {event.get('usage_type')}"
    )

    print(
        f"quantity  : {event.get('quantity')}"
    )

    print(
        f"timestamp : {event.get('timestamp')}"
    )