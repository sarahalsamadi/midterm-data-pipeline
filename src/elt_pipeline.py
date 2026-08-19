from pymongo import MongoClient

from config.settings import (
    DATABASE_NAME,
    MONGODB_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
)

from src.quality_rules import (
    add_correction,
    clean_number,
    normalize_currency,
    normalize_date,
    normalize_email,
    normalize_phone,
    normalize_status,
    recompute_total,
    validate_items,
)


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


def normalize_business_document(
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
    return (
        normalize_business_document(
            existing_document
        )
        ==
        normalize_business_document(
            incoming_document
        )
    )


def process_raw_run(run_id):
    client = MongoClient(
        MONGODB_URI
    )

    database = client[
        DATABASE_NAME
    ]

    raw_collection = database[
        RAW_COLLECTION
    ]

    validated_collection = database[
        VALIDATED_COLLECTION
    ]

    quarantine_collection = database[
        QUARANTINE_COLLECTION
    ]

    valid_count = 0
    corrected_count = 0
    quarantine_count = 0

    inserted_count = 0
    updated_count = 0
    unchanged_count = 0

    error_counts = {}

    try:
        raw_records = raw_collection.find(
            {
                "run_id": run_id
            }
        )

        for raw_document in raw_records:
            record = raw_document[
                "raw_record"
            ]

            corrections = []
            errors = []

            order_id = record.get(
                "order_id"
            )

            if order_id is not None:
                order_id = str(
                    order_id
                ).strip()

            customer_id = record.get(
                "customer_id"
            )

            if customer_id is not None:
                customer_id = str(
                    customer_id
                ).strip()

            if not order_id:
                errors.append(
                    "ID_ORDER_MISSING"
                )

            if not customer_id:
                errors.append(
                    "ID_CUSTOMER_MISSING"
                )

            original_date = record.get(
                "order_date"
            )

            cleaned_date = normalize_date(
                original_date
            )

            if not cleaned_date:
                errors.append(
                    "DATE_INVALID_IMPOSSIBLE"
                )

            add_correction(
                corrections,
                "order_date",
                original_date,
                cleaned_date,
                "DATE_NORMALIZE",
            )

            items, items_error = (
                validate_items(
                    record.get(
                        "items_json"
                    )
                )
            )

            if items_error:
                errors.append(
                    items_error
                )

            original_phone = record.get(
                "customer_phone"
            )

            cleaned_phone = normalize_phone(
                original_phone
            )

            if (
                original_phone
                and not cleaned_phone
            ):
                errors.append(
                    "PHONE_INVALID"
                )

            add_correction(
                corrections,
                "customer_phone",
                original_phone,
                cleaned_phone,
                "PHONE_NORMALIZE",
            )

            original_email = record.get(
                "customer_email"
            )

            cleaned_email = normalize_email(
                original_email
            )

            if (
                original_email
                and not cleaned_email
            ):
                errors.append(
                    "EMAIL_INVALID"
                )

            add_correction(
                corrections,
                "customer_email",
                original_email,
                cleaned_email,
                "EMAIL_REPEATED_SYMBOLS",
            )

            original_status = record.get(
                "status"
            )

            cleaned_status = normalize_status(
                original_status
            )

            add_correction(
                corrections,
                "status",
                original_status,
                cleaned_status,
                "STATUS_ALIAS",
            )

            original_payment_status = (
                record.get(
                    "payment_status"
                )
            )

            cleaned_payment_status = (
                normalize_status(
                    original_payment_status
                )
            )

            add_correction(
                corrections,
                "payment_status",
                original_payment_status,
                cleaned_payment_status,
                "PAYMENT_STATUS_ALIAS",
            )

            original_currency = record.get(
                "currency"
            )

            cleaned_currency = (
                normalize_currency(
                    original_currency
                )
            )

            add_correction(
                corrections,
                "currency",
                original_currency,
                cleaned_currency,
                "CURRENCY_NORMALIZE",
            )

            original_total = record.get(
                "total_amount"
            )

            cleaned_total = clean_number(
                original_total
            )

            if cleaned_total is None:
                errors.append(
                    "PRICE_UNKNOWN"
                )

            add_correction(
                corrections,
                "total_amount",
                original_total,
                cleaned_total,
                "NUMBER_NORMALIZE",
            )

            original_payment_amount = (
                record.get(
                    "payment_amount"
                )
            )

            cleaned_payment_amount = (
                clean_number(
                    original_payment_amount
                )
            )

            if (
                cleaned_payment_amount
                is None
            ):
                errors.append(
                    "PAYMENT_AMOUNT_UNKNOWN"
                )

            add_correction(
                corrections,
                "payment_amount",
                original_payment_amount,
                cleaned_payment_amount,
                "NUMBER_NORMALIZE",
            )

            original_delivery_cost = (
                record.get(
                    "delivery_cost"
                )
            )

            cleaned_delivery_cost = (
                clean_number(
                    original_delivery_cost
                )
            )

            if (
                cleaned_delivery_cost
                is None
            ):
                errors.append(
                    "DELIVERY_COST_UNKNOWN"
                )

            add_correction(
                corrections,
                "delivery_cost",
                original_delivery_cost,
                cleaned_delivery_cost,
                "NUMBER_NORMALIZE",
            )

            recomputed_total = (
                recompute_total(
                    items,
                    cleaned_delivery_cost,
                )
            )

            if (
                recomputed_total
                is not None
                and cleaned_total
                is not None
                and abs(
                    recomputed_total
                    - cleaned_total
                ) > 0.01
            ):
                add_correction(
                    corrections,
                    "total_amount",
                    cleaned_total,
                    recomputed_total,
                    "TOTAL_RECOMPUTE",
                )

                cleaned_total = (
                    recomputed_total
                )

            if errors:
                quarantine_document = {
                    "run_id": run_id,
                    "order_id": order_id,
                    "error_codes": errors,
                    "error_details": errors,
                    "raw_record": record,
                }

                quarantine_collection.insert_one(
                    quarantine_document
                )

                quarantine_count += 1

                for error in errors:
                    error_counts[
                        error
                    ] = (
                        error_counts.get(
                            error,
                            0,
                        )
                        + 1
                    )

                continue

            if corrections:
                quality_status = (
                    "corrected"
                )

                corrected_count += 1

            else:
                quality_status = (
                    "valid"
                )

                valid_count += 1

            final_document = {
                "order_id": order_id,
                "order_date": cleaned_date,
                "status": cleaned_status,
                "customer_id": customer_id,
                "customer_name": record.get(
                    "customer_name"
                ),
                "customer_phone": cleaned_phone,
                "customer_email": cleaned_email,
                "city": record.get(
                    "city"
                ),
                "district": record.get(
                    "district"
                ),
                "delivery_type": record.get(
                    "delivery_type"
                ),
                "delivery_cost": (
                    cleaned_delivery_cost
                ),
                "payment_method": record.get(
                    "payment_method"
                ),
                "payment_status": (
                    cleaned_payment_status
                ),
                "payment_amount": (
                    cleaned_payment_amount
                ),
                "currency": (
                    cleaned_currency
                ),
                "total_amount": (
                    cleaned_total
                ),
                "items": items,
                "quality_status": (
                    quality_status
                ),
                "corrections": (
                    corrections
                ),
                "last_run_id": (
                    run_id
                ),
            }

            existing = (
                validated_collection.find_one(
                    {
                        "order_id": order_id
                    }
                )
            )

            if existing is None:
                validated_collection.update_one(
                    {
                        "order_id": order_id
                    },
                    {
                        "$set": final_document
                    },
                    upsert=True,
                )

                inserted_count += 1

            elif business_documents_equal(
                existing,
                final_document,
            ):
                unchanged_count += 1

            else:
                validated_collection.update_one(
                    {
                        "order_id": order_id
                    },
                    {
                        "$set": final_document
                    },
                    upsert=True,
                )

                updated_count += 1

        return {
            "valid": valid_count,
            "corrected": corrected_count,
            "quarantine": quarantine_count,
            "inserted": inserted_count,
            "updated": updated_count,
            "unchanged": unchanged_count,
            "errors": error_counts,
        }

    finally:
        client.close()