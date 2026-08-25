import argparse
import os
import time

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

from src.spark_loader import (
    load_large_csv_to_raw,
)

DEFAULT_INPUT_FILE = "data/orders_test_10.csv"

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Run the hybrid data pipeline."
        )
    )

    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_FILE,
        help="Path to the CSV file.",
    )

    args = parser.parse_args()

    print(
        "\n=== Midterm Data Pipeline ==="
    )

    pipeline_start = (
        time.time()
    )

    engine = choose_engine(
        args.input
    )

    show_database_info()

    file_size_mb = (
        os.path.getsize(
            args.input
        )
        / (
            1024
            * 1024
        )
    )

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

        result = process_raw_run(
            run_id
        )

        processed = (
            result[
                "valid"
            ]
            + result[
                "corrected"
            ]
            + result[
                "quarantine"
            ]
        )

        if (
            processed
            != raw_count
        ):
            raise RuntimeError(
                "Consistency check failed: "
                f"raw={raw_count}, "
                f"classified={processed}"
            )

        pipeline_elapsed = (
            time.time()
            - pipeline_start
        )

        pipeline_throughput = (
            raw_count
            / pipeline_elapsed
            if pipeline_elapsed > 0
            else 0.0
        )

        print(
            "\n=== ELT Result ==="
        )

        print(
            f"Raw: "
            f"{raw_count}"
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

        print(
            f"Consistency check: "
            f"{raw_count} = "
            f"{processed}"
        )

        print(
            f"Error counts: "
            f"{result['errors']}"
        )

        metrics = {
            "run_id": (
                run_id
            ),

            "file_name": (
                os.path.basename(
                    args.input
                )
            ),

            "file_size_mb": (
                round(
                    file_size_mb,
                    2,
                )
            ),

            "engine_used": (
                engine
            ),

            "read_rows": (
                raw_count
            ),

            "loaded_raw": (
                raw_count
            ),

            "count_valid": (
                result[
                    "valid"
                ]
            ),

            "count_corrected": (
                result[
                    "corrected"
                ]
            ),

            "count_quarantine": (
                result[
                    "quarantine"
                ]
            ),

            "classified_count": (
                processed
            ),

            "seconds_elapsed": (
                round(
                    pipeline_elapsed,
                    4,
                )
            ),

            "throughput": (
                round(
                    pipeline_throughput,
                    2,
                )
            ),

            "raw_load_seconds": (
                round(
                    load_result[
                        "elapsed_seconds"
                    ],
                    4,
                )
            ),

            "raw_load_throughput": (
                round(
                    load_result[
                        "throughput"
                    ],
                    2,
                )
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
                result[
                    "errors"
                ]
            ),

            "count_inserted": (
                result[
                    "inserted"
                ]
            ),

            "count_updated": (
                result[
                    "updated"
                ]
            ),

            "count_unchanged": (
                result[
                    "unchanged"
                ]
            ),
        }

        save_run_metrics(
            metrics
        )

    else:
        result = (
            load_large_csv_to_raw(
                args.input
            )
        )

        print(
            "\n=== PySpark Final Result ==="
        )

        print(
            f"Raw: "
            f"{result['raw_count']}"
        )

        print(
            f"Valid: "
            f"{result['valid_count']}"
        )

        print(
            f"Corrected: "
            f"{result['corrected_count']}"
        )

        print(
            f"Quarantine: "
            f"{result['quarantine_count']}"
        )

        print(
            f"Duplicate business keys: "
            f"{result['duplicate_count']}"
        )

        print(
            f"Validated unique: "
            f"{result['validated_unique_count']}"
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

        print(
            f"Partitions: "
            f"{result['partitions']}"
        )

        print(
            f"Error counts: "
            f"{result['error_counts']}"
        )

        metrics = {
            "run_id": (
                result[
                    "run_id"
                ]
            ),

            "file_name": (
                os.path.basename(
                    args.input
                )
            ),

            "file_size_mb": (
                round(
                    file_size_mb,
                    2,
                )
            ),

            "engine_used": (
                engine
            ),

            "read_rows": (
                result[
                    "raw_count"
                ]
            ),

            "loaded_raw": (
                result[
                    "raw_count"
                ]
            ),

            "count_valid": (
                result[
                    "valid_count"
                ]
            ),

            "count_corrected": (
                result[
                    "corrected_count"
                ]
            ),

            "count_quarantine": (
                result[
                    "quarantine_count"
                ]
            ),

            "classified_count": (
                result[
                    "classified_count"
                ]
            ),

            "duplicate_business_keys": (
                result[
                    "duplicate_count"
                ]
            ),

            "validated_unique_count": (
                result[
                    "validated_unique_count"
                ]
            ),

            "seconds_elapsed": (
                round(
                    result[
                        "elapsed_seconds"
                    ],
                    4,
                )
            ),

            "throughput": (
                round(
                    result[
                        "throughput"
                    ],
                    2,
                )
            ),

            "partitions": (
                result[
                    "partitions"
                ]
            ),

            "upsert_partitions": (
                result[
                    "upsert_partitions"
                ]
            ),

            "counts_case_error": (
                result[
                    "error_counts"
                ]
            ),

            "count_inserted": (
                result[
                    "inserted"
                ]
            ),

            "count_updated": (
                result[
                    "updated"
                ]
            ),

            "count_unchanged": (
                result[
                    "unchanged"
                ]
            ),
        }

        save_run_metrics(
            metrics
        )


if __name__ == "__main__":
    main()