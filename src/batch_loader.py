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


def insert_batch_with_metrics(raw_collection, batch, batch_number):
    batch_start = time.time()

    try:
        raw_collection.insert_many(batch)

    except Exception as exc:
        batch_seconds = time.time() - batch_start

        print(
            f"Batch {batch_number} FAILED: "
            f"{len(batch)} rows, "
            f"{batch_seconds:.2f} seconds"
        )
        print(
            f"Failure reason: "
            f"{type(exc).__name__}: {exc}"
        )

        raise

    batch_seconds = time.time() - batch_start

    batch_throughput = 0.0
    if batch_seconds > 0:
        batch_throughput = len(batch) / batch_seconds

    print(
        f"Batch {batch_number}: "
        f"{len(batch)} rows, "
        f"{batch_seconds:.2f} seconds, "
        f"{batch_throughput:.2f} rows/second"
    )

    return len(batch)


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
        with open(
            file_path,
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as source:

            reader = csv.DictReader(source)

            for row_number, row in enumerate(
                reader,
                start=2,
            ):
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

                    inserted_rows = insert_batch_with_metrics(
                        raw_collection,
                        batch,
                        batch_number,
                    )

                    total_rows += inserted_rows
                    batch = []

            if batch:
                batch_number += 1

                inserted_rows = insert_batch_with_metrics(
                    raw_collection,
                    batch,
                    batch_number,
                )

                total_rows += inserted_rows

        elapsed = time.time() - start_time

        throughput = 0.0
        if elapsed > 0:
            throughput = total_rows / elapsed

        print("\n=== Batch Load Result ===")
        print(f"Run ID: {run_id}")
        print(f"Loaded raw rows: {total_rows}")
        print(f"Total batches: {batch_number}")
        print(f"Elapsed seconds: {elapsed:.2f}")
        print(
            f"Throughput: "
            f"{throughput:.2f} rows/second"
        )

        return {
            "run_id": run_id,
            "raw_count": total_rows,
            "batch_count": batch_number,
            "elapsed_seconds": elapsed,
            "throughput": throughput,
        }

    finally:
        client.close()
