import os

from config.settings import SMALL_FILE_THRESHOLD_MB


def get_file_size_mb(file_path):
    size_bytes = os.path.getsize(file_path)
    size_mb = size_bytes / (1024 * 1024)
    return size_mb


def choose_engine(file_path):
    file_size_mb = get_file_size_mb(
        file_path
    )

    if file_size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = "python_batch"

        reason = (
            f"File size {file_size_mb:.2f} MB "
            f"is less than or equal to the "
            f"{SMALL_FILE_THRESHOLD_MB} MB threshold."
        )

    else:
        engine = "pyspark"

        reason = (
            f"File size {file_size_mb:.2f} MB "
            f"is greater than the "
            f"{SMALL_FILE_THRESHOLD_MB} MB threshold."
        )

    print(
        f"File size: {file_size_mb:.2f} MB"
    )

    print(
        f"Threshold: "
        f"{SMALL_FILE_THRESHOLD_MB} MB"
    )

    print(
        f"Selected engine: {engine}"
    )

    print(
        f"Reason: {reason}"
    )

    return engine