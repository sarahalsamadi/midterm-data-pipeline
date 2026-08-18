import argparse
import csv


def create_sample(input_path, output_path, rows):
    count = 0

    with open(input_path, "r", encoding="utf-8-sig", newline="") as source:
        with open(output_path, "w", encoding="utf-8", newline="") as output:
            reader = csv.reader(source)
            writer = csv.writer(output)

            writer.writerow(next(reader))

            for row in reader:
                if count >= rows:
                    break

                writer.writerow(row)
                count += 1

    return count


def main():
    parser = argparse.ArgumentParser(
        description="Create a small sample from a large CSV file."
    )

    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--rows", type=int, default=100000)

    args = parser.parse_args()

    if args.rows <= 0:
        raise ValueError("Rows must be greater than zero.")

    count = create_sample(
        args.input,
        args.output,
        args.rows,
    )

    print(f"Sample created: {args.output}")
    print(f"Rows written: {count}")


if __name__ == "__main__":
    main()