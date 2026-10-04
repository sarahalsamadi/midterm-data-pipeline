import os


SMALL_FILE_THRESHOLD_MB = 200
BATCH_SIZE = 5000

SPARK_LOCAL_DIR = os.getenv(
    "SPARK_LOCAL_DIR"
)

MONGODB_URI = os.getenv(
    "MONGODB_URI",
    "mongodb://localhost:27017/",
)

DATABASE_NAME = "midterm_data_pipeline"

RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"