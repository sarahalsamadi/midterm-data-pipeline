from pprint import pprint

from pymongo import ASCENDING, DESCENDING, MongoClient

from config.settings import DATABASE_NAME, MONGODB_URI


INDEX_DEFINITIONS = [
    {
        "name": "idx_city",
        "keys": [
            ("city", ASCENDING),
        ],
        "purpose": "Speed up city-based order lookup.",
    },
    {
        "name": "idx_customer_id",
        "keys": [
            ("customer_id", ASCENDING),
        ],
        "purpose": "Speed up customer order history lookup.",
    },
    {
        "name": "idx_payment_status_total",
        "keys": [
            ("payment_status", ASCENDING),
            ("total_amount", DESCENDING),
        ],
        "purpose": (
            "Speed up paid-order filtering and "
            "high-value sorting/range queries."
        ),
    },
]


EXPLAIN_QUERIES = {
    "orders_by_city": {
        "filter": {
            "city": "تعز",
        },
        "sort": None,
    },
    "customer_orders": {
        "filter": {
            "customer_id": "عميل-1",
        },
        "sort": None,
    },
    "high_value_paid_orders": {
        "filter": {
            "payment_status": "paid",
            "total_amount": {
                "$gte": 100000
            },
        },
        "sort": {
            "total_amount": -1
        },
    },
}


def get_collection():
    client = MongoClient(MONGODB_URI)
    database = client[DATABASE_NAME]
    return client, database["orders_validated"]


def create_indexes():
    client, collection = get_collection()

    try:
        created = []

        for definition in INDEX_DEFINITIONS:
            name = collection.create_index(
                definition["keys"],
                name=definition["name"],
            )

            created.append(
                {
                    "name": name,
                    "purpose": definition["purpose"],
                }
            )

        return created

    finally:
        client.close()


def drop_phase2_indexes():
    client, collection = get_collection()

    try:
        existing = {
            index["name"]
            for index in collection.list_indexes()
        }

        dropped = []

        for definition in INDEX_DEFINITIONS:
            name = definition["name"]

            if name in existing:
                collection.drop_index(name)
                dropped.append(name)

        return dropped

    finally:
        client.close()


def list_indexes():
    client, collection = get_collection()

    try:
        return [
            dict(index)
            for index in collection.list_indexes()
        ]

    finally:
        client.close()


def explain_query(name):
    if name not in EXPLAIN_QUERIES:
        raise ValueError(
            f"Unknown explain query: {name}"
        )

    client, collection = get_collection()

    try:
        spec = EXPLAIN_QUERIES[name]

        command = {
            "explain": {
                "find": collection.name,
                "filter": spec["filter"],
            },
            "verbosity": "executionStats",
        }

        if spec["sort"]:
            command["explain"]["sort"] = spec["sort"]

        result = collection.database.command(command)

        stats = result["executionStats"]

        summary = {
            "query": name,
            "nReturned": stats.get("nReturned"),
            "executionTimeMillis": stats.get(
                "executionTimeMillis"
            ),
            "totalKeysExamined": stats.get(
                "totalKeysExamined"
            ),
            "totalDocsExamined": stats.get(
                "totalDocsExamined"
            ),
            "winningPlan": result[
                "queryPlanner"
            ].get("winningPlan"),
        }

        return summary

    finally:
        client.close()


if __name__ == "__main__":
    print("Phase 2 index definitions:")

    for definition in INDEX_DEFINITIONS:
        print(
            definition["name"],
            "->",
            definition["keys"],
        )
