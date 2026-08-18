from pymongo import (
    MongoClient,
    UpdateOne,
)

from config.settings import (
    DATABASE_NAME,
    MONGODB_URI,
    VALIDATED_COLLECTION,
)


UPSERT_BATCH_SIZE = 1000


BUSINESS_FIELDS = [
    "order_id",
    "order_date",
    "status",
    "customer_id",
    "customer_name",
    "customer_phone",
    "customer_email",
    "city",
    "district",
    "delivery_type",
    "delivery_cost",
    "payment_method",
    "payment_status",
    "payment_amount",
    "currency",
    "total_amount",
    "items",
    "quality_status",
    "corrections",
]


def normalize_document(
    document,
):
    result = {}

    for field in BUSINESS_FIELDS:
        if field in document:
            result[field] = document[
                field
            ]

    return result


def business_documents_equal(
    existing_document,
    incoming_document,
):
    existing_business = (
        normalize_document(
            existing_document
        )
    )

    incoming_business = (
        normalize_document(
            incoming_document
        )
    )

    return (
        existing_business
        == incoming_business
    )


def process_batch(
    collection,
    records,
):
    if not records:
        return {
            "inserted": 0,
            "updated": 0,
            "unchanged": 0,
        }

    order_ids = [
        record["order_id"]
        for record in records
    ]

    existing_documents = (
        collection.find(
            {
                "order_id": {
                    "$in": order_ids
                }
            }
        )
    )

    existing_by_order_id = {
        document["order_id"]: document
        for document
        in existing_documents
    }

    operations = []

    inserted = 0
    updated = 0
    unchanged = 0

    for record in records:
        order_id = record[
            "order_id"
        ]

        existing = (
            existing_by_order_id.get(
                order_id
            )
        )

        if existing is None:
            inserted += 1

            operations.append(
                UpdateOne(
                    {
                        "order_id": order_id
                    },
                    {
                        "$set": record
                    },
                    upsert=True,
                )
            )

            continue

        if business_documents_equal(
            existing,
            record,
        ):
            unchanged += 1
            continue

        updated += 1

        operations.append(
            UpdateOne(
                {
                    "order_id": order_id
                },
                {
                    "$set": record
                },
                upsert=True,
            )
        )

    if operations:
        collection.bulk_write(
            operations,
            ordered=False,
        )

    return {
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
    }


def upsert_and_count_partition(
    rows,
):
    client = None

    inserted_total = 0
    updated_total = 0
    unchanged_total = 0

    try:
        client = MongoClient(
            MONGODB_URI,
            serverSelectionTimeoutMS=10000,
        )

        database = client[
            DATABASE_NAME
        ]

        collection = database[
            VALIDATED_COLLECTION
        ]

        batch = []

        for row in rows:
            record = (
                row.asDict(
                    recursive=True
                )
            )

            batch.append(
                record
            )

            if len(
                batch
            ) >= UPSERT_BATCH_SIZE:
                result = process_batch(
                    collection,
                    batch,
                )

                inserted_total += (
                    result[
                        "inserted"
                    ]
                )

                updated_total += (
                    result[
                        "updated"
                    ]
                )

                unchanged_total += (
                    result[
                        "unchanged"
                    ]
                )

                batch = []

        if batch:
            result = process_batch(
                collection,
                batch,
            )

            inserted_total += (
                result[
                    "inserted"
                ]
            )

            updated_total += (
                result[
                    "updated"
                ]
            )

            unchanged_total += (
                result[
                    "unchanged"
                ]
            )

        yield (
            inserted_total,
            updated_total,
            unchanged_total,
        )

    finally:
        if client is not None:
            client.close()


def distributed_upsert(
    validated_dataframe,
    partitions=32,
):
    repartitioned = (
        validated_dataframe
        .repartition(
            partitions,
            "order_id",
        )
    )

    partition_results = (
        repartitioned
        .rdd
        .mapPartitions(
            upsert_and_count_partition
        )
        .collect()
    )

    inserted = sum(
        result[0]
        for result
        in partition_results
    )

    updated = sum(
        result[1]
        for result
        in partition_results
    )

    unchanged = sum(
        result[2]
        for result
        in partition_results
    )

    return {
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "upsert_partitions": partitions,
    }