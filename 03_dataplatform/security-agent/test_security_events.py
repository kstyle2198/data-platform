import json
import time
import uuid

from datetime import datetime, timezone

from kafka import KafkaProducer


producer = KafkaProducer(

    bootstrap_servers="localhost:9092",

    value_serializer=lambda value:
        json.dumps(value).encode("utf-8"),
)


username = "kim3ddd33"

ip_address = "10.10.10.100"


for i in range(20):

    event = {

        "event_id": str(
            uuid.uuid4()
        ),

        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),

        "source": "keycloak",

        "event_type": "LOGIN_FAILED",

        "username": username,

        "ip_address": ip_address,

        "client_id": "usage-api",

        "realm": "hdaic",
    }

    future = producer.send(
        "security-events",
        value=event,
    )

    metadata = future.get(
        timeout=10
    )

    print(
        f"[{i + 1}] "
        f"event_id={event['event_id']} "
        f"partition={metadata.partition} "
        f"offset={metadata.offset}"
    )

    time.sleep(1)


producer.flush()

producer.close()