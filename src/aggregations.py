from pymongo import MongoClient

from config.settings import DATABASE_NAME, MONGODB_URI


AGGREGATION_NAMES = [
    "sales_by_city",
    "top_products",
    "top_customers",
    "sales_by_period",
    "orders_by_status",
]


def get_collection():
    client = MongoClient(MONGODB_URI)
    database = client[DATABASE_NAME]
    return client, database["orders_validated"]


def sales_by_city(limit=20):
    """
    Total order value and order count by city.
    """
    client, collection = get_collection()

    try:
        pipeline = [
            {
                "$match": {
                    "city": {"$nin": [None, ""]},
                    "total_amount": {"$type": "number"},
                }
            },
            {
                "$group": {
                    "_id": "$city",
                    "order_count": {"$sum": 1},
                    "total_sales": {"$sum": "$total_amount"},
                    "average_order_value": {"$avg": "$total_amount"},
                }
            },
            {
                "$sort": {
                    "total_sales": -1
                }
            },
            {
                "$limit": limit
            },
            {
                "$project": {
                    "_id": 0,
                    "city": "$_id",
                    "order_count": 1,
                    "total_sales": {"$round": ["$total_sales", 2]},
                    "average_order_value": {
                        "$round": ["$average_order_value", 2]
                    },
                }
            },
        ]

        return list(collection.aggregate(pipeline))

    finally:
        client.close()


def top_products(limit=10):
    """
    Top products based on revenue from the items array.
    """
    client, collection = get_collection()

    try:
        pipeline = [
            {
                "$unwind": "$items"
            },
            {
                "$match": {
                    "items.sku": {"$nin": [None, ""]},
                    "items.total": {"$type": "number"},
                }
            },
            {
                "$group": {
                    "_id": {
                        "sku": "$items.sku",
                        "name": "$items.name",
                    },
                    "quantity_sold": {"$sum": "$items.qty"},
                    "revenue": {"$sum": "$items.total"},
                    "order_occurrences": {"$sum": 1},
                }
            },
            {
                "$sort": {
                    "revenue": -1
                }
            },
            {
                "$limit": limit
            },
            {
                "$project": {
                    "_id": 0,
                    "sku": "$_id.sku",
                    "product_name": "$_id.name",
                    "quantity_sold": {
                        "$round": ["$quantity_sold", 2]
                    },
                    "revenue": {
                        "$round": ["$revenue", 2]
                    },
                    "order_occurrences": 1,
                }
            },
        ]

        return list(collection.aggregate(pipeline))

    finally:
        client.close()


def top_customers(limit=10):
    """
    Top customers based on total order value.
    """
    client, collection = get_collection()

    try:
        pipeline = [
            {
                "$match": {
                    "customer_id": {"$nin": [None, ""]},
                    "total_amount": {"$type": "number"},
                }
            },
            {
                "$group": {
                    "_id": {
                        "customer_id": "$customer_id",
                        "customer_name": "$customer_name",
                    },
                    "order_count": {"$sum": 1},
                    "total_spent": {"$sum": "$total_amount"},
                    "average_order_value": {"$avg": "$total_amount"},
                }
            },
            {
                "$sort": {
                    "total_spent": -1
                }
            },
            {
                "$limit": limit
            },
            {
                "$project": {
                    "_id": 0,
                    "customer_id": "$_id.customer_id",
                    "customer_name": "$_id.customer_name",
                    "order_count": 1,
                    "total_spent": {
                        "$round": ["$total_spent", 2]
                    },
                    "average_order_value": {
                        "$round": ["$average_order_value", 2]
                    },
                }
            },
        ]

        return list(collection.aggregate(pipeline))

    finally:
        client.close()


def sales_by_period(limit=50):
    """
    Daily sales report derived from ISO-formatted order_date strings.
    """
    client, collection = get_collection()

    try:
        pipeline = [
            {
                "$match": {
                    "order_date": {
                        "$type": "string"
                    },
                    "total_amount": {
                        "$type": "number"
                    },
                }
            },
            {
                "$addFields": {
                    "parsed_order_date": {
                        "$convert": {
                            "input": "$order_date",
                            "to": "date",
                            "onError": None,
                            "onNull": None,
                        }
                    }
                }
            },
            {
                "$match": {
                    "parsed_order_date": {
                        "$ne": None
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$parsed_order_date",
                        }
                    },
                    "order_count": {"$sum": 1},
                    "total_sales": {"$sum": "$total_amount"},
                    "average_order_value": {"$avg": "$total_amount"},
                }
            },
            {
                "$sort": {
                    "_id": 1
                }
            },
            {
                "$limit": limit
            },
            {
                "$project": {
                    "_id": 0,
                    "date": "$_id",
                    "order_count": 1,
                    "total_sales": {
                        "$round": ["$total_sales", 2]
                    },
                    "average_order_value": {
                        "$round": ["$average_order_value", 2]
                    },
                }
            },
        ]

        return list(collection.aggregate(pipeline))

    finally:
        client.close()


def orders_by_status():
    """
    Distribution of orders by status.
    """
    client, collection = get_collection()

    try:
        pipeline = [
            {
                "$group": {
                    "_id": "$status",
                    "order_count": {"$sum": 1},
                    "total_value": {"$sum": "$total_amount"},
                }
            },
            {
                "$sort": {
                    "order_count": -1
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "status": "$_id",
                    "order_count": 1,
                    "total_value": {
                        "$round": ["$total_value", 2]
                    },
                }
            },
        ]

        return list(collection.aggregate(pipeline))

    finally:
        client.close()


def list_aggregations():
    return AGGREGATION_NAMES


def run_aggregation(name, limit=None):
    if name == "sales_by_city":
        return sales_by_city(limit or 20)

    if name == "top_products":
        return top_products(limit or 10)

    if name == "top_customers":
        return top_customers(limit or 10)

    if name == "sales_by_period":
        return sales_by_period(limit or 50)

    if name == "orders_by_status":
        return orders_by_status()

    raise ValueError(
        f"Unknown aggregation: {name}. "
        f"Available: {', '.join(AGGREGATION_NAMES)}"
    )


if __name__ == "__main__":
    print("Available aggregation reports:")

    for name in AGGREGATION_NAMES:
        print("-", name)
