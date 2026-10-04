import time
from datetime import datetime, timezone

from src.jobs import run_job


MV_INTERVAL_SECONDS = 30 * 60
REPORT_HOUR_UTC = 1


def utc_now():
    return datetime.now(timezone.utc)


def scheduled_mv_refresh():
    print(
        "Running scheduled job: "
        "refresh_materialized_views"
    )

    result = run_job(
        "refresh_materialized_views",
        trigger="scheduled",
    )

    print(
        "Completed:",
        result["status"],
    )

    return result


def scheduled_report_generation():
    print(
        "Running scheduled job: "
        "generate_reports"
    )

    result = run_job(
        "generate_reports",
        trigger="scheduled",
    )

    print(
        "Completed:",
        result["status"],
    )

    return result


def print_schedule():
    print("=== SCHEDULED JOBS ===")
    print(
        "refresh_materialized_views: "
        "every 30 minutes"
    )
    print(
        "generate_reports: "
        "daily at 01:00 UTC"
    )


def run_scheduler():
    last_mv_run = None
    last_report_date = None

    print_schedule()

    print()
    print(
        "Scheduler started. "
        "Press Ctrl+C to stop."
    )

    while True:
        now = utc_now()

        if (
            last_mv_run is None
            or (
                now - last_mv_run
            ).total_seconds()
            >= MV_INTERVAL_SECONDS
        ):
            try:
                scheduled_mv_refresh()
            except Exception as exc:
                print(
                    "Scheduled MV refresh failed:",
                    exc,
                )

            last_mv_run = now

        if (
            now.hour == REPORT_HOUR_UTC
            and last_report_date
            != now.date()
        ):
            try:
                scheduled_report_generation()
            except Exception as exc:
                print(
                    "Scheduled report generation "
                    "failed:",
                    exc,
                )

            last_report_date = now.date()

        time.sleep(30)


if __name__ == "__main__":
    try:
        run_scheduler()

    except KeyboardInterrupt:
        print()
        print("Scheduler stopped.")
