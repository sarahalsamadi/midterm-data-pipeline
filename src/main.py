import argparse

from src.batch_loader import load_csv_to_raw
from src.elt_pipeline import process_raw_run
from src.file_router import choose_engine
from src.mongo_setup import show_database_info


def main():
    parser = argparse.ArgumentParser(
        description="Run the hybrid data pipeline."
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to the CSV file.",
    )

    args = parser.parse_args()

    print("\n=== Midterm Data Pipeline ===")

    engine = choose_engine(args.input)

    show_database_info()

    if engine == "python_batch":
        run_id, raw_count = load_csv_to_raw(
            args.input
        )

        result = process_raw_run(
            run_id
        )

        print("\n=== ELT Result ===")
        print(f"Raw: {raw_count}")
        print(f"Valid: {result['valid']}")
        print(f"Corrected: {result['corrected']}")
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

    else:
        print(
            "\nPySpark loader will handle "
            "this file."
        )


if __name__ == "__main__":
    main()