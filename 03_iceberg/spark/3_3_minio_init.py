from minio import Minio

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET_NAME = "warehouse"


client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)


if client.bucket_exists(BUCKET_NAME):
    print(f"[MinIO] Bucket already exists: {BUCKET_NAME}")
else:
    client.make_bucket(BUCKET_NAME)
    print(f"[MinIO] Bucket created: {BUCKET_NAME}")