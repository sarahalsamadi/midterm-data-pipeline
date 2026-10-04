import os
from pymongo import MongoClient

from config.settings import DATABASE_NAME, MONGODB_URI


QUERY_NAMES = [
    "orders_by_city",
    "orders_by_status",
    "customer_orders",
    "orders_by_date_range",
    "high_value_paid_orders",
]


def get_collection():
    client = MongoClient(MONGODB_URI)
    database = client[DATABASE_NAME]
    return client, database["orders_validated"]


def orders_by_city(city, limit=20):
    client, collection = get_collection()

    try:
        return list(
            collection.find(
                {"city": city},
                {"_id": 0},
            ).limit(limit)
        )
    finally:
        client.close()


def orders_by_status(status, limit=20):
    client, collection = get_collection()

    try:
        return list(
            collection.find(
                {"status": status},
                {"_id": 0},
            ).limit(limit)
        )
    finally:
        client.close()


def customer_orders(customer_id, limit=20):
    client, collection = get_collection()

    try:
        return list(
            collection.find(
                {"customer_id": customer_id},
                {"_id": 0},
            ).limit(limit)
        )
    finally:
        client.close()


def orders_by_date_range(start_date, end_date, limit=20):
    client, collection = get_collection()

    try:
        query = {
            "order_date": {
                "$gte": start_date,
                "$lte": end_date,
            }
        }

        return list(
            collection.find(
                query,
                {"_id": 0},
            )
            .sort("order_date", 1)
            .limit(limit)
        )
    finally:
        client.close()


def high_value_paid_orders(min_total, limit=20):
    client, collection = get_collection()

    try:
        query = {
            "payment_status": "paid",
            "total_amount": {
                "$gte": float(min_total)
            },
        }

        return list(
            collection.find(
                query,
                {"_id": 0},
            )
            .sort("total_amount", -1)
            .limit(limit)
        )
    finally:
        client.close()


def list_queries():
    return QUERY_NAMES


if __name__ == "__main__":
    print("Available queries:")
    for name in QUERY_NAMES:
        print("-", name)
