# Usage Data Platform - Final Lab

## Architecture

Browser
  ↓
Keycloak
  ↓ JWT
FastAPI
  ↓
Kafka: usage-events
  ↓
Spark Structured Streaming
  ↓
Iceberg
  ↓
MinIO

Pipeline monitoring:

Spark / Iceberg / MinIO
  ↓
Kafka: pipeline-status
  ↓
FastAPI
  ↓
Browser UI


## 1. Start

PowerShell:

```powershell
cd D:\data_platform\03_iceberg

docker compose up -d --build
```

Check:

```powershell
docker ps
```

API:

http://localhost:8000

Keycloak:

http://localhost:8080

MinIO Console:

http://localhost:9002


## 2. Start Spark full pipeline

```powershell
docker exec spark `
  /opt/spark/bin/spark-submit `
  /opt/spark/work/3_12_full_pipeline.py
```

Keep this process running.


## 3. Login

UI:

http://localhost:8000

Username:

```text
kim
```

Password:

```text
test1234
```


## 4. Publish event

Example:

```text
service     = api
usage_type  = request
quantity    = 10
```

Click:

```text
이벤트 발행
```


## 5. Expected UI

```text
FastAPI ✓
   ↓
Kafka ✓
   ↓
Spark ✓
   ↓
Iceberg ✓
   ↓
MinIO ✓
```

Kafka shows:

```text
topic
partition
offset
```

Spark shows:

```text
batch_id
```

Iceberg shows:

```text
table
```

MinIO shows:

```text
s3a://warehouse/usage_db/usage_events/data/
```


## 6. Kafka check

```powershell
docker exec kafka `
  /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:9092 `
  --list
```

Expected:

```text
pipeline-status
usage-events
```


## 7. Check usage events

```powershell
docker exec kafka `
  /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:9092 `
  --topic usage-events `
  --from-beginning
```


## 8. Check pipeline status

```powershell
docker exec kafka `
  /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:9092 `
  --topic pipeline-status `
  --from-beginning
```


## 9. Check Iceberg

```powershell
docker exec spark `
  /opt/spark/bin/spark-submit `
  /opt/spark/work/3_9_metadata.py
```


## 10. Check MinIO

Browser:

http://localhost:9002

Login:

```text
Access Key: minioadmin
Secret Key: minioadmin123
```

Bucket:

```text
warehouse
```


## 11. Important container addresses

From Windows host:

```text
Keycloak = http://localhost:8080
FastAPI  = http://localhost:8000
Kafka    = localhost:9092
MinIO    = http://localhost:9000
MinIO UI = http://localhost:9002
```


Inside Docker network:

```text
Keycloak = http://keycloak:8080
Kafka    = kafka:9092
MinIO    = http://minio:9000
```


## 12. Important

Do not use:

```text
localhost:9092
```

from the API or Spark containers.

Use:

```text
kafka:9092
```


Do not use:

```text
localhost:9000
```

from Spark.

Use:

```text
http://minio:9000
```


## 13. Stop

```powershell
docker compose down
```

To remove data volumes too:

```powershell
docker compose down -v
```

This removes Keycloak and MinIO persisted data.
