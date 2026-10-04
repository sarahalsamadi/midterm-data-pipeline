import os
from datetime import date, datetime
from decimal import Decimal

from bson import ObjectId
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from src.aggregations import list_aggregations, run_aggregation
from src.indexes import create_indexes, list_indexes
from src.jobs import list_jobs, recent_job_runs, run_job
from src.main import run_pipeline
from src.materialized_views import (
    list_materialized_views,
    refresh_all_materialized_views,
)
from src.queries import (
    customer_orders,
    high_value_paid_orders,
    list_queries,
    orders_by_city,
    orders_by_date_range,
    orders_by_status,
)


app = FastAPI(
    title="Big Data Pipeline API",
    description=(
        "Unified REST API for the Big Data Phase 2 project. "
        "It exposes the existing ingestion pipeline, indexes, queries, "
        "aggregations, materialized views, and scheduled jobs."
    ),
    version="2.0.0",
)


class IngestRequest(BaseModel):
    input_file: str


class RefreshMVRequest(BaseModel):
    run_id: str | None = None


def json_safe(value):
    """Recursively convert MongoDB/Python values to JSON-safe values."""

    if isinstance(value, ObjectId):
        return str(value)

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, dict):
        return {
            str(key): json_safe(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]

    return value


@app.get("/health", tags=["System"])
def health():
    return {
        "status": "ok",
        "service": "big-data-pipeline-api",
        "version": "2.0.0",
    }


@app.post("/ingest", tags=["Pipeline"])
def ingest(request: IngestRequest):
    """
    Run the same ingestion pipeline and router used by the Midterm project.
    """

    input_file = request.input_file

    if not os.path.isfile(input_file):
        raise HTTPException(
            status_code=404,
            detail=f"Input file not found: {input_file}",
        )

    try:
        result = run_pipeline(input_file)

        return {
            "status": "success",
            "result": json_safe(result),
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc


@app.post("/indexes", tags=["Indexes"])
def indexes():
    """Create the Phase 2 indexes."""

    try:
        result = create_indexes()

        return {
            "status": "success",
            "result": json_safe(result),
            "indexes": json_safe(list_indexes()),
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc


@app.get("/queries", tags=["Queries"])
def queries():
    """List all available practical queries."""

    return {
        "queries": list_queries(),
    }


@app.get("/queries/{name}", tags=["Queries"])
def query_by_name(
    name: str,
    city: str | None = None,
    status: str | None = None,
    customer_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    min_total: float | None = None,
    limit: int = Query(default=20, ge=1, le=1000),
):
    """Execute one named practical query."""

    try:
        if name == "orders_by_city":
            if not city:
                raise HTTPException(
                    status_code=400,
                    detail="city parameter is required.",
                )

            result = orders_by_city(
                city,
                limit=limit,
            )

        elif name == "orders_by_status":
            if not status:
                raise HTTPException(
                    status_code=400,
                    detail="status parameter is required.",
                )

            result = orders_by_status(
                status,
                limit=limit,
            )

        elif name == "customer_orders":
            if not customer_id:
                raise HTTPException(
                    status_code=400,
                    detail="customer_id parameter is required.",
                )

            result = customer_orders(
                customer_id,
                limit=limit,
            )

        elif name == "orders_by_date_range":
            if not start_date or not end_date:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "start_date and end_date "
                        "parameters are required."
                    ),
                )

            result = orders_by_date_range(
                start_date,
                end_date,
                limit=limit,
            )

        elif name == "high_value_paid_orders":
            if min_total is None:
                raise HTTPException(
                    status_code=400,
                    detail="min_total parameter is required.",
                )

            result = high_value_paid_orders(
                min_total,
                limit=limit,
            )

        else:
            raise HTTPException(
                status_code=404,
                detail=f"Unknown query: {name}",
            )

        return {
            "query": name,
            "count": len(result),
            "data": json_safe(result),
        }

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc


@app.get("/aggregations", tags=["Aggregations"])
def aggregations():
    """List all available aggregation reports."""

    return {
        "aggregations": list_aggregations(),
    }


@app.get("/aggregations/{name}", tags=["Aggregations"])
def aggregation_by_name(
    name: str,
    limit: int | None = Query(
        default=None,
        ge=1,
        le=1000,
    ),
):
    """Execute one named aggregation report."""

    if name not in list_aggregations():
        raise HTTPException(
            status_code=404,
            detail=f"Unknown aggregation: {name}",
        )

    try:
        result = run_aggregation(
            name,
            limit=limit,
        )

        return {
            "aggregation": name,
            "count": len(result),
            "data": json_safe(result),
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc


@app.post("/refresh-mv", tags=["Materialized Views"])
def refresh_mv(
    request: RefreshMVRequest | None = None,
):
    """
    Incrementally refresh all materialized views.
    """

    try:
        run_id = request.run_id if request else None

        result = refresh_all_materialized_views(
            run_id=run_id,
        )

        return {
            "status": "success",
            "views": json_safe(
                list_materialized_views()
            ),
            "result": json_safe(result),
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc


@app.get("/jobs", tags=["Scheduled Jobs"])
def jobs():
    """List jobs and recent execution logs."""

    return {
        "jobs": json_safe(list_jobs()),
        "recent_runs": json_safe(
            recent_job_runs(limit=20)
        ),
    }


@app.post(
    "/jobs/{name}/run",
    tags=["Scheduled Jobs"],
)
def run_named_job(name: str):
    """Manually execute one scheduled job."""

    try:
        result = run_job(
            name,
            trigger="manual",
        )

        return {
            "status": "success",
            "result": json_safe(result),
        }

    except ValueError as exc:
        raise HTTPException(
            status_code=404,
            detail=str(exc),
        ) from exc

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc
