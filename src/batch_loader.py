import csv
import time
from datetime import datetime, timezone
from uuid import uuid4

from pymongo import MongoClient

from config.settings import (
    BATCH_SIZE,
    DATABASE_NAME,
    MONGODB_URI,
    RAW_COLLECTION,
)


def load_csv_to_raw(file_path):
    run_id = str(uuid4())
    client = MongoClient(MONGODB_URI)

    database = client[DATABASE_NAME]
    raw_collection = database[RAW_COLLECTION]

    batch = []
    total_rows = 0
    batch_number = 0

    start_time = time.time()

    try:
        with open(file_path, "r", encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)

            for row_number, row in enumerate(reader, start=2):
                raw_document = {
                    "run_id": run_id,
                    "source_file": file_path,
                    "source_row_number": row_number,
                    "ingested_at": datetime.now(timezone.utc),
                    "engine_used": "python_batch",
                    "raw_record": row,
                }

                batch.append(raw_document)

                if len(batch) >= BATCH_SIZE:
                    batch_number += 1
                    batch_start = time.time()

                    raw_collection.insert_many(batch)

                    batch_seconds = time.time() - batch_start
                    total_rows += len(batch)

                    print(
                        f"Batch {batch_number}: "
                        f"{len(batch)} rows, "
                        f"{batch_seconds:.2f} seconds"
                    )

                    batch = []

            if batch:
                batch_number += 1
                batch_start = time.time()

                raw_collection.insert_many(batch)

                batch_seconds = time.time() - batch_start
                total_rows += len(batch)

                print(
                    f"Batch {batch_number}: "
                    f"{len(batch)} rows, "
                    f"{batch_seconds:.2f} seconds"
                )

        elapsed = time.time() - start_time

        print("\n=== Batch Load Result ===")
        print(f"Run ID: {run_id}")
        print(f"Loaded raw rows: {total_rows}")
        print(f"Total batches: {batch_number}")
        print(f"Elapsed seconds: {elapsed:.2f}")

        if elapsed > 0:
            print(f"Throughput: {total_rows / elapsed:.2f} rows/second")

        return run_id, total_rows

    finally:
        client.close()