from pymongo import (
    ASCENDING,
    MongoClient,
)

from config.settings import (
    MONGODB_URI,
    DATABASE_NAME,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)


VALIDATED_SCHEMA = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [
            "order_id",
            "quality_status",
        ],
        "properties": {
            "order_id": {
                "bsonType": "string",
            },

            "order_date": {
                "bsonType": [
                    "string",
                    "null",
                ],
            },

            "status": {
                "bsonType": [
                    "string",
                    "null",
                ],
            },

            "customer_id": {
                "bsonType": [
                    "string",
                    "null",
                ],
            },

            "customer_name": {
                "bsonType": [
                    "string",
                    "null",
                ],
            },

            "customer_phone": {
                "bsonType": [
                    "string",
                    "null",
                ],
            },

            "customer_email": {
                "bsonType": [
                    "string",
                    "null",
                ],
            },

            "city": {
                "bsonType": [
                    "string",
                    "null",
                ],
            },

            "district": {
                "bsonType": [
                    "string",
                    "null",
                ],
            },

            "delivery_type": {
                "bsonType": [
                    "string",
                    "null",
                ],
            },

            "delivery_cost": {
                "bsonType": [
                    "double",
                    "int",
                    "long",
                    "decimal",
                    "null",
                ],
            },

            "payment_method": {
                "bsonType": [
                    "string",
                    "null",
                ],
            },

            "payment_status": {
                "bsonType": [
                    "string",
                    "null",
                ],
            },

            "payment_amount": {
                "bsonType": [
                    "double",
                    "int",
                    "long",
                    "decimal",
                    "null",
                ],
            },

            "currency": {
                "bsonType": [
                    "string",
                    "null",
                ],
            },

            "total_amount": {
                "bsonType": [
                    "double",
                    "int",
                    "long",
                    "decimal",
                    "null",
                ],
            },

            "items": {
                "bsonType": [
                    "array",
                    "null",
                ],
            },

            "quality_status": {
                "enum": [
                    "valid",
                    "corrected",
                ],
            },

            "corrections": {
                "bsonType": [
                    "array",
                    "null",
                ],
            },

            "last_run_id": {
                "bsonType": [
                    "string",
                    "null",
                ],
            },
        },
    }
}


def get_database():
    client = MongoClient(
        MONGODB_URI,
        serverSelectionTimeoutMS=10000,
    )

    client.admin.command(
        "ping"
    )

    database = client[
        DATABASE_NAME
    ]

    return (
        client,
        database,
    )


def ensure_collections(
    database,
):
    existing = (
        database.list_collection_names()
    )

    if RAW_COLLECTION not in existing:
        database.create_collection(
            RAW_COLLECTION
        )

    if QUARANTINE_COLLECTION not in existing:
        database.create_collection(
            QUARANTINE_COLLECTION
        )

    if VALIDATED_COLLECTION not in existing:
        database.create_collection(
            VALIDATED_COLLECTION,
            validator=VALIDATED_SCHEMA,
            validationLevel="strict",
            validationAction="error",
        )

    else:
        database.command({
            "collMod": VALIDATED_COLLECTION,
            "validator": VALIDATED_SCHEMA,
            "validationLevel": "strict",
            "validationAction": "error",
        })


def ensure_indexes(
    database,
):
    collection = database[
        VALIDATED_COLLECTION
    ]

    indexes = (
        collection.index_information()
    )

    for index_name, index_info in indexes.items():
        keys = index_info.get(
            "key",
            []
        )

        is_unique = index_info.get(
            "unique",
            False
        )

        if (
            keys
            == [("order_id", 1)]
            and is_unique
        ):
            print(
                f"Unique order_id index "
                f"already exists: "
                f"{index_name}"
            )
            return

    collection.create_index(
        [
            (
                "order_id",
                ASCENDING,
            )
        ],
        unique=True,
        name="uq_order_id",
    )

    print(
        "Unique order_id index created."
    )

def initialize_database():
    client, database = (
        get_database()
    )

    try:
        ensure_collections(
            database
        )

        ensure_indexes(
            database
        )

        print(
            "\n=== MongoDB Setup ==="
        )

        print(
            "Schema validation: enabled"
        )

        print(
            "Unique order_id index: enabled"
        )

    finally:
        client.close()


def show_database_info():
    client, database = (
        get_database()
    )

    try:
        print(
            "\n=== MongoDB Connection ==="
        )

        print(
            "Connection: successful"
        )

        print(
            f"Database: "
            f"{DATABASE_NAME}"
        )

        print(
            f"Raw collection: "
            f"{RAW_COLLECTION}"
        )

        print(
            f"Validated collection: "
            f"{VALIDATED_COLLECTION}"
        )

        print(
            f"Quarantine collection: "
            f"{QUARANTINE_COLLECTION}"
        )

    finally:
        client.close()


def main():
    initialize_database()

    show_database_info()


if __name__ == "__main__":
    main()