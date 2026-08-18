import os

from config.settings import SMALL_FILE_THRESHOLD_MB


def get_file_size_mb(file_path):
    size_bytes = os.path.getsize(file_path)
    size_mb = size_bytes / (1024 * 1024)
    return size_mb


def choose_engine(file_path):
    file_size_mb = get_file_size_mb(file_path)

    if file_size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = "python_batch"
    else:
        engine = "pyspark"

    print(f"File size: {file_size_mb:.2f} MB")
    print(f"Threshold: {SMALL_FILE_THRESHOLD_MB} MB")
    print(f"Selected engine: {engine}")

    return engine