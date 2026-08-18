from pymongo import MongoClient

from config.settings import (
    MONGODB_URI,
    DATABASE_NAME,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)


def get_database():
    client = MongoClient(MONGODB_URI)

    client.admin.command("ping")

    database = client[DATABASE_NAME]

    return client, database


def show_database_info():
    client, database = get_database()

    try:
        print("\n=== MongoDB Connection ===")
        print("Connection: successful")
        print(f"Database: {DATABASE_NAME}")
        print(f"Raw collection: {RAW_COLLECTION}")
        print(f"Validated collection: {VALIDATED_COLLECTION}")
        print(f"Quarantine collection: {QUARANTINE_COLLECTION}")

    finally:
        client.close()