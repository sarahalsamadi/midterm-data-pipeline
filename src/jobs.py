from datetime import datetime, timezone
import traceback

from pymongo import MongoClient

from config.settings import (
    DATABASE_NAME,
    MONGODB_URI,
)

from src.aggregations import (
    AGGREGATION_NAMES,
    run_aggregation,
)

from src.materialized_views import (
    refresh_all_materialized_views,
)


JOB_RUNS_COLLECTION = "job_runs"
REPORT_SNAPSHOTS_COLLECTION = "report_snapshots"


JOB_DEFINITIONS = {
    "refresh_materialized_views": {
        "description": (
            "Incrementally refresh all "
            "materialized views."
        ),
        "schedule": "Every 30 minutes",
    },
    "generate_reports": {
        "description": (
            "Generate and persist the latest "
            "aggregation report snapshots."
        ),
        "schedule": "Every day at 01:00",
    },
}


def _utc_now():
    return datetime.now(timezone.utc)


def get_database():
    client = MongoClient(MONGODB_URI)
    return client, client[DATABASE_NAME]


def list_jobs():
    result = []

    for name, definition in JOB_DEFINITIONS.items():
        result.append(
            {
                "name": name,
                "description": definition[
                    "description"
                ],
                "schedule": definition[
                    "schedule"
                ],
            }
        )

    return result


def refresh_materialized_views_job():
    return {
        "views": (
            refresh_all_materialized_views()
        )
    }


def generate_reports_job():
    client, db = get_database()

    try:
        generated_at = _utc_now()
        snapshots = []

        for name in AGGREGATION_NAMES:
            rows = run_aggregation(name)

            snapshot = {
                "report_name": name,
                "generated_at": generated_at,
                "row_count": len(rows),
                "data": rows,
            }

            db[
                REPORT_SNAPSHOTS_COLLECTION
            ].update_one(
                {
                    "report_name": name,
                },
                {
                    "$set": snapshot,
                },
                upsert=True,
            )

            snapshots.append(
                {
                    "report_name": name,
                    "row_count": len(rows),
                }
            )

        return {
            "reports_generated": len(
                snapshots
            ),
            "reports": snapshots,
        }

    finally:
        client.close()


def _execute_job(job_name):
    if job_name == "refresh_materialized_views":
        return refresh_materialized_views_job()

    if job_name == "generate_reports":
        return generate_reports_job()

    raise ValueError(
        f"Unknown job: {job_name}. "
        f"Available: "
        f"{', '.join(JOB_DEFINITIONS)}"
    )


def run_job(job_name, trigger="manual"):
    if job_name not in JOB_DEFINITIONS:
        raise ValueError(
            f"Unknown job: {job_name}. "
            f"Available: "
            f"{', '.join(JOB_DEFINITIONS)}"
        )

    client, db = get_database()
    runs = db[JOB_RUNS_COLLECTION]

    started_at = _utc_now()

    run_document = {
        "job_name": job_name,
        "trigger": trigger,
        "started_at": started_at,
        "finished_at": None,
        "duration_seconds": None,
        "status": "running",
        "result": None,
        "error": None,
    }

    inserted = runs.insert_one(
        run_document
    )

    run_id = inserted.inserted_id

    try:
        result = _execute_job(job_name)

        finished_at = _utc_now()

        duration = (
            finished_at - started_at
        ).total_seconds()

        runs.update_one(
            {
                "_id": run_id,
            },
            {
                "$set": {
                    "finished_at": finished_at,
                    "duration_seconds": round(
                        duration,
                        4,
                    ),
                    "status": "success",
                    "result": result,
                }
            },
        )

        return {
            "run_id": str(run_id),
            "job_name": job_name,
            "trigger": trigger,
            "status": "success",
            "started_at": (
                started_at.isoformat()
            ),
            "finished_at": (
                finished_at.isoformat()
            ),
            "duration_seconds": round(
                duration,
                4,
            ),
            "result": result,
        }

    except Exception as exc:
        finished_at = _utc_now()

        duration = (
            finished_at - started_at
        ).total_seconds()

        error = {
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }

        runs.update_one(
            {
                "_id": run_id,
            },
            {
                "$set": {
                    "finished_at": finished_at,
                    "duration_seconds": round(
                        duration,
                        4,
                    ),
                    "status": "failure",
                    "error": error,
                }
            },
        )

        raise

    finally:
        client.close()


def recent_job_runs(limit=20):
    client, db = get_database()

    try:
        rows = list(
            db[
                JOB_RUNS_COLLECTION
            ].find(
                {},
                {
                    "_id": 1,
                    "job_name": 1,
                    "trigger": 1,
                    "started_at": 1,
                    "finished_at": 1,
                    "duration_seconds": 1,
                    "status": 1,
                },
            )
            .sort("started_at", -1)
            .limit(limit)
        )

        for row in rows:
            row["_id"] = str(row["_id"])

        return rows

    finally:
        client.close()


if __name__ == "__main__":
    from pprint import pprint

    print("=== AVAILABLE JOBS ===")

    for job in list_jobs():
        pprint(job)