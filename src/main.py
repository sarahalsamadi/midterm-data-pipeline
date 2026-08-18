import argparse

from src.file_router import choose_engine


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

    print(f"\nPipeline will use: {engine}")


if __name__ == "__main__":
    main()