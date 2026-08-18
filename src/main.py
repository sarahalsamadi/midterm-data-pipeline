import argparse

from src.batch_loader import load_csv_to_raw
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
        load_csv_to_raw(args.input)

    else:
        print("\nPySpark loader will handle this file.")


if __name__ == "__main__":
    main()