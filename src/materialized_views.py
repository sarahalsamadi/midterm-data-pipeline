from datetime import datetime, timezone

from pymongo import MongoClient, ReplaceOne

from config.settings import DATABASE_NAME, MONGODB_URI


DAILY_VIEW = "daily_sales_summary"
PRODUCT_VIEW = "top_products_summary"
METADATA_COLLECTION = "mv_refresh_metadata"

VIEW_NAMES = [
    DAILY_VIEW,
    PRODUCT_VIEW,
]


def get_database():
    client = MongoClient(MONGODB_URI)
    return client, client[DATABASE_NAME]


def _utc_now():
    return datetime.now(timezone.utc)


def _latest_run_id(db):
    """
    Return the run_id of the most recent ingestion run.

    orders_raw is append-only for every ingestion, so its newest
    document reliably identifies the latest pipeline run. Using
    orders_validated._id is incorrect for updated orders because
    an upsert update preserves the existing MongoDB _id.
    """
    doc = db["orders_raw"].find_one(
        {"run_id": {"$exists": True, "$ne": None}},
        sort=[("_id", -1)],
        projection={"run_id": 1},
    )

    if not doc:
        return None

    return doc.get("run_id")


def _metadata(db, view_name):
    return db[METADATA_COLLECTION].find_one(
        {"view_name": view_name}
    )


def _save_metadata(
    db,
    view_name,
    mode,
    run_id,
    source_documents,
    output_documents,
):
    db[METADATA_COLLECTION].update_one(
        {"view_name": view_name},
        {
            "$set": {
                "view_name": view_name,
                "last_refresh_at": _utc_now(),
                "last_run_id": run_id,
                "last_refresh_mode": mode,
                "source_documents_processed": source_documents,
                "output_documents_written": output_documents,
            }
        },
        upsert=True,
    )


def _daily_pipeline(match_stage=None):
    pipeline = []

    if match_stage:
        pipeline.append({"$match": match_stage})

    pipeline.extend(
        [
            {
                "$match": {
                    "order_date": {"$type": "string"},
                    "total_amount": {"$type": "number"},
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
                    "parsed_order_date": {"$ne": None}
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
                "$project": {
                    "_id": 0,
                    "date": "$_id",
                    "order_count": 1,
                    "total_sales": {"$round": ["$total_sales", 2]},
                    "average_order_value": {
                        "$round": ["$average_order_value", 2]
                    },
                }
            },
        ]
    )

    return pipeline


def _product_pipeline(match_stage=None):
    pipeline = []

    if match_stage:
        pipeline.append({"$match": match_stage})

    pipeline.extend(
        [
            {"$unwind": "$items"},
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
                "$project": {
                    "_id": 0,
                    "sku": "$_id.sku",
                    "product_name": "$_id.name",
                    "quantity_sold": {
                        "$round": ["$quantity_sold", 2]
                    },
                    "revenue": {"$round": ["$revenue", 2]},
                    "order_occurrences": 1,
                }
            },
        ]
    )

    return pipeline


def _full_refresh_daily(db, run_id):
    source = db["orders_validated"]
    target = db[DAILY_VIEW]

    rows = list(source.aggregate(_daily_pipeline()))

    target.delete_many({})

    if rows:
        target.insert_many(rows)

    target.create_index(
        [("date", 1)],
        unique=True,
        name="uq_daily_sales_date",
    )

    _save_metadata(
        db,
        DAILY_VIEW,
        "initial",
        run_id,
        source.count_documents({}),
        len(rows),
    )

    return {
        "view": DAILY_VIEW,
        "mode": "initial",
        "run_id": run_id,
        "source_documents_processed": source.count_documents({}),
        "output_documents_written": len(rows),
    }


def _full_refresh_products(db, run_id):
    source = db["orders_validated"]
    target = db[PRODUCT_VIEW]

    rows = list(source.aggregate(_product_pipeline()))

    target.delete_many({})

    if rows:
        target.insert_many(rows)

    target.create_index(
        [("sku", 1)],
        unique=True,
        name="uq_top_products_sku",
    )

    _save_metadata(
        db,
        PRODUCT_VIEW,
        "initial",
        run_id,
        source.count_documents({}),
        len(rows),
    )

    return {
        "view": PRODUCT_VIEW,
        "mode": "initial",
        "run_id": run_id,
        "source_documents_processed": source.count_documents({}),
        "output_documents_written": len(rows),
    }


def _incremental_daily(db, run_id):
    source = db["orders_validated"]
    target = db[DAILY_VIEW]

    changed = list(
        source.find(
            {"last_run_id": run_id},
            {"order_date": 1,
            "previous_mv_keys.date": 1,
            "_id": 0,},
        )
    )

    affected_dates = set()

    for row in changed:
        value = row.get("order_date")

        if isinstance(value, str) and len(value) >= 10:
            affected_dates.add(value[:10])

        previous_date = (
            row.get(
                "previous_mv_keys",
                {},
            ).get("date")
        )

        if previous_date:
            affected_dates.add(
                previous_date
            )

    written = 0

    for date_value in sorted(affected_dates):
        rows = list(
            source.aggregate(
                _daily_pipeline(
                    {
                        "order_date": {
                            "$regex": f"^{date_value}"
                        }
                    }
                )
            )
        )

        if rows:
            target.replace_one(
                {"date": date_value},
                rows[0],
                upsert=True,
            )
            written += 1
        else:
            target.delete_one({"date": date_value})

    _save_metadata(
        db,
        DAILY_VIEW,
        "incremental",
        run_id,
        len(changed),
        written,
    )

    return {
        "view": DAILY_VIEW,
        "mode": "incremental",
        "run_id": run_id,
        "changed_documents": len(changed),
        "affected_groups": len(affected_dates),
        "output_documents_written": written,
        "full_rebuild": False,
    }


def _incremental_products(db, run_id):
    source = db["orders_validated"]
    target = db[PRODUCT_VIEW]

    changed = list(
        source.find(
            {"last_run_id": run_id},
            {
                "items.sku": 1,
                "previous_mv_keys.skus": 1,
                "_id": 0,
            },
        )
    )

    affected_skus = set()

    for row in changed:
        for item in row.get("items", []):
            sku = item.get("sku")

            if sku:
                affected_skus.add(sku)
        previous_skus = (
            row.get(
                "previous_mv_keys",
                {},
            ).get(
                "skus",
                [],
            )
        )
    

    for sku in previous_skus:
        if sku:
            affected_skus.add(sku)

    written = 0

    for sku in sorted(affected_skus):
        rows = list(
            source.aggregate(
                _product_pipeline(
                    {
                        "items": {
                            "$elemMatch": {
                                "sku": sku
                            }
                        }
                    }
                )
            )
        )

        matching = [
            row
            for row in rows
            if row.get("sku") == sku
        ]

        if matching:
            target.replace_one(
                {"sku": sku},
                matching[0],
                upsert=True,
            )
            written += 1
        else:
            target.delete_one({"sku": sku})

    _save_metadata(
        db,
        PRODUCT_VIEW,
        "incremental",
        run_id,
        len(changed),
        written,
    )

    return {
        "view": PRODUCT_VIEW,
        "mode": "incremental",
        "run_id": run_id,
        "changed_documents": len(changed),
        "affected_groups": len(affected_skus),
        "output_documents_written": written,
        "full_rebuild": False,
    }


def refresh_materialized_view(view_name, run_id=None):
    client, db = get_database()

    try:
        if view_name not in VIEW_NAMES:
            raise ValueError(
                f"Unknown materialized view: {view_name}"
            )

        current_run_id = run_id or _latest_run_id(db)
        metadata = _metadata(db, view_name)

        if metadata is None:
            if view_name == DAILY_VIEW:
                return _full_refresh_daily(
                    db,
                    current_run_id,
                )

            return _full_refresh_products(
                db,
                current_run_id,
            )

        if metadata.get("last_run_id") == current_run_id:
            return {
                "view": view_name,
                "mode": "incremental",
                "run_id": current_run_id,
                "changed_documents": 0,
                "affected_groups": 0,
                "output_documents_written": 0,
                "full_rebuild": False,
                "message": "No new pipeline run since last refresh.",
            }

        if view_name == DAILY_VIEW:
            return _incremental_daily(
                db,
                current_run_id,
            )

        return _incremental_products(
            db,
            current_run_id,
        )

    finally:
        client.close()


def refresh_all_materialized_views(run_id=None):
    return [
        refresh_materialized_view(
            view_name,
            run_id=run_id,
        )
        for view_name in VIEW_NAMES
    ]


def list_materialized_views():
    client, db = get_database()

    try:
        result = []

        for name in VIEW_NAMES:
            result.append(
                {
                    "name": name,
                    "documents": db[name].count_documents({}),
                    "metadata": _metadata(db, name),
                }
            )

        return result

    finally:
        client.close()


if __name__ == "__main__":
    for result in refresh_all_materialized_views():
        print(result)
