import argparse
import csv
import os


def create_sample_by_rows(
    input_path,
    output_path,
    rows,
):
    count = 0

    with open(
        input_path,
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as source:
        with open(
            output_path,
            "w",
            encoding="utf-8",
            newline="",
        ) as output:
            reader = csv.reader(source)
            writer = csv.writer(output)

            header = next(reader)
            writer.writerow(header)

            for row in reader:
                if count >= rows:
                    break

                writer.writerow(row)
                count += 1

    return {
        "rows_written": count,
        "size_bytes": os.path.getsize(
            output_path
        ),
    }


def create_sample_by_size(
    input_path,
    output_path,
    size_gb,
):
    target_bytes = int(
        size_gb
        * 1024
        * 1024
        * 1024
    )

    rows_written = 0

    with open(
        input_path,
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as source:
        with open(
            output_path,
            "w",
            encoding="utf-8",
            newline="",
        ) as output:
            reader = csv.reader(source)
            writer = csv.writer(output)

            header = next(reader)
            writer.writerow(header)

            for row in reader:
                writer.writerow(row)
                rows_written += 1

                if (
                    output.tell()
                    >= target_bytes
                ):
                    break

    return {
        "rows_written": rows_written,
        "size_bytes": os.path.getsize(
            output_path
        ),
    }


def format_size(
    size_bytes,
):
    size_mb = (
        size_bytes
        / (
            1024
            * 1024
        )
    )

    size_gb = (
        size_bytes
        / (
            1024
            * 1024
            * 1024
        )
    )

    return (
        round(
            size_mb,
            2,
        ),
        round(
            size_gb,
            2,
        ),
    )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Create a CSV sample from "
            "a large source file."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help=(
            "Source CSV file."
        ),
    )

    parser.add_argument(
        "--output",
        required=True,
        help=(
            "Output sample CSV file."
        ),
    )

    group = parser.add_mutually_exclusive_group(
        required=True
    )

    group.add_argument(
        "--rows",
        type=int,
        help=(
            "Number of data rows "
            "to copy."
        ),
    )

    group.add_argument(
        "--size-gb",
        type=float,
        help=(
            "Approximate target "
            "file size in GB."
        ),
    )

    args = parser.parse_args()

    if (
        args.rows is not None
        and args.rows <= 0
    ):
        raise ValueError(
            "Rows must be greater than zero."
        )

    if (
        args.size_gb is not None
        and args.size_gb <= 0
    ):
        raise ValueError(
            "Size must be greater than zero."
        )

    if args.rows is not None:
        result = (
            create_sample_by_rows(
                args.input,
                args.output,
                args.rows,
            )
        )

        mode = (
            f"{args.rows} rows"
        )

    else:
        result = (
            create_sample_by_size(
                args.input,
                args.output,
                args.size_gb,
            )
        )

        mode = (
            f"{args.size_gb} GB target"
        )

    size_mb, size_gb = (
        format_size(
            result[
                "size_bytes"
            ]
        )
    )

    print(
        "\n=== Sample Creation ==="
    )

    print(
        f"Source: "
        f"{args.input}"
    )

    print(
        f"Output: "
        f"{args.output}"
    )

    print(
        f"Mode: "
        f"{mode}"
    )

    print(
        f"Rows written: "
        f"{result['rows_written']}"
    )

    print(
        f"Final size MB: "
        f"{size_mb}"
    )

    print(
        f"Final size GB: "
        f"{size_gb}"
    )


if __name__ == "__main__":
    main()