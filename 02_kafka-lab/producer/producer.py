import os
import json

from kafka import KafkaProducer


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


# ============================================================
# Producer
# ============================================================

producer = KafkaProducer(

    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

    key_serializer=lambda key:
        key.encode("utf-8"),

    value_serializer=lambda value:
        json.dumps(value).encode("utf-8")
)


# ============================================================
# Test Events
# ============================================================

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
        "usage_type": "request",
        "quantity": 5
    },

    {
        "user_id": "kim",
        "service": "storage",
        "usage_type": "gb",
        "quantity": 20
    },

    {
        "user_id": "lee",
        "service": "api",
        "usage_type": "request",
        "quantity": 30
    },

    {
        "user_id": "lee",
        "service": "search",
        "usage_type": "request",
        "quantity": 15
    },

    {
        "user_id": "park",
        "service": "api",
        "usage_type": "request",
        "quantity": 50
    }
]


# ============================================================
# Send
# ============================================================

print("========================================")
print("Kafka Producer Started")
print("========================================")


for event in events:

    key = event["user_id"]


    future = producer.send(

        KAFKA_TOPIC,

        key=key,

        value=event
    )


    metadata = future.get(
        timeout=10
    )


    print(
        f"user={key} "
        f"service={event['service']} "
        f"partition={metadata.partition} "
        f"offset={metadata.offset}"
    )


# ============================================================
# Finish
# ============================================================

producer.flush()

producer.close()

print()
print("All events sent successfully.")