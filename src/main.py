import argparse
import os

from config.settings import (
    BATCH_SIZE,
)

from src.batch_loader import (
    load_csv_to_raw,
)

from src.elt_pipeline import (
    process_raw_run,
)

from src.file_router import (
    choose_engine,
)

from src.metrics import (
    save_run_metrics,
)

from src.mongo_setup import (
    show_database_info,
)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Run the hybrid data pipeline."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to the CSV file.",
    )

    args = parser.parse_args()

    print(
        "\n=== Midterm Data Pipeline ==="
    )

    engine = choose_engine(
        args.input
    )

    show_database_info()

    if engine == "python_batch":

        load_result = (
            load_csv_to_raw(
                args.input
            )
        )

        run_id = (
            load_result[
                "run_id"
            ]
        )

        raw_count = (
            load_result[
                "raw_count"
            ]
        )

        result = (
            process_raw_run(
                run_id
            )
        )

        print(
            "\n=== ELT Result ==="
        )

        print(
            f"Raw: {raw_count}"
        )

        print(
            f"Valid: "
            f"{result['valid']}"
        )

        print(
            f"Corrected: "
            f"{result['corrected']}"
        )

        print(
            f"Quarantine: "
            f"{result['quarantine']}"
        )

        print(
            f"Inserted: "
            f"{result['inserted']}"
        )

        print(
            f"Updated: "
            f"{result['updated']}"
        )

        print(
            f"Unchanged: "
            f"{result['unchanged']}"
        )

        processed = (
            result["valid"]
            + result["corrected"]
            + result["quarantine"]
        )

        print(
            f"Consistency check: "
            f"{raw_count} = {processed}"
        )

        print(
            f"Error counts: "
            f"{result['errors']}"
        )

        file_size_mb = (
            os.path.getsize(
                args.input
            )
            / (1024 * 1024)
        )

        metrics = {
            "run_id": run_id,

            "file_name": (
                os.path.basename(
                    args.input
                )
            ),

            "file_size_mb": round(
                file_size_mb,
                2,
            ),

            "engine_used": engine,

            "read_rows": (
                raw_count
            ),

            "loaded_raw": (
                raw_count
            ),

            "count_valid": (
                result["valid"]
            ),

            "count_corrected": (
                result["corrected"]
            ),

            "count_quarantine": (
                result[
                    "quarantine"
                ]
            ),

            "seconds_elapsed": round(
                load_result[
                    "elapsed_seconds"
                ],
                4,
            ),

            "throughput": round(
                load_result[
                    "throughput"
                ],
                2,
            ),

            "batch_size": (
                BATCH_SIZE
            ),

            "batch_count": (
                load_result[
                    "batch_count"
                ]
            ),

            "counts_case_error": (
                result["errors"]
            ),

            "count_inserted": (
                result["inserted"]
            ),

            "count_updated": (
                result["updated"]
            ),

            "count_unchanged": (
                result["unchanged"]
            ),
        }

        save_run_metrics(
            metrics
        )

    else:
        print(
            "\nPySpark loader will handle "
            "this file."
        )


if __name__ == "__main__":
    main()