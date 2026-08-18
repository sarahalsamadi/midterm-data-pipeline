import time
from datetime import (
    datetime,
    timezone,
)
from uuid import uuid4

from pyspark.sql import (
    SparkSession,
)
from pyspark.sql.functions import (
    col,
    lit,
    monotonically_increasing_id,
    row_number,
    struct,
)
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import (
    Window,
)

from config.settings import (
    DATABASE_NAME,
    MONGODB_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
)

from src.spark_transform import (
    transform_spark_raw,
)

from src.spark_upsert import (
    distributed_upsert,
)


MONGODB_SPARK_CONNECTOR = (
    "org.mongodb.spark:"
    "mongo-spark-connector_2.13:"
    "11.1.0"
)


ORDER_SCHEMA = StructType([
    StructField(
        "order_id",
        StringType(),
        True,
    ),
    StructField(
        "order_date",
        StringType(),
        True,
    ),
    StructField(
        "status",
        StringType(),
        True,
    ),
    StructField(
        "customer_id",
        StringType(),
        True,
    ),
    StructField(
        "customer_name",
        StringType(),
        True,
    ),
    StructField(
        "customer_phone",
        StringType(),
        True,
    ),
    StructField(
        "customer_email",
        StringType(),
        True,
    ),
    StructField(
        "city",
        StringType(),
        True,
    ),
    StructField(
        "district",
        StringType(),
        True,
    ),
    StructField(
        "delivery_type",
        StringType(),
        True,
    ),
    StructField(
        "delivery_cost",
        StringType(),
        True,
    ),
    StructField(
        "payment_method",
        StringType(),
        True,
    ),
    StructField(
        "payment_status",
        StringType(),
        True,
    ),
    StructField(
        "payment_amount",
        StringType(),
        True,
    ),
    StructField(
        "currency",
        StringType(),
        True,
    ),
    StructField(
        "total_amount",
        StringType(),
        True,
    ),
    StructField(
        "items_json",
        StringType(),
        True,
    ),
])


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName(
            "MidtermDataPipeline"
        )
        .master(
            "local[*]"
        )
        .config(
            "spark.jars.packages",
            MONGODB_SPARK_CONNECTOR,
        )
        .config(
            "spark.mongodb.write.connection.uri",
            MONGODB_URI,
        )
        .config(
            "spark.driver.memory",
            "6g",
        )
        .config(
            "spark.executor.memory",
            "6g",
        )
        .config(
            "spark.sql.shuffle.partitions",
            "64",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    return spark


def write_mongodb(
    dataframe,
    collection_name,
):
    (
        dataframe.write
        .format(
            "mongodb"
        )
        .mode(
            "append"
        )
        .option(
            "database",
            DATABASE_NAME,
        )
        .option(
            "collection",
            collection_name,
        )
        .save()
    )


def deduplicate_validated(
    validated_dataframe,
):
    window = (
        Window
        .partitionBy(
            "order_id"
        )
        .orderBy(
            col(
                "order_date"
            ).desc_nulls_last()
        )
    )

    return (
        validated_dataframe
        .withColumn(
            "_row_number",
            row_number().over(
                window
            ),
        )
        .filter(
            col(
                "_row_number"
            )
            == 1
        )
        .drop(
            "_row_number"
        )
    )


def load_large_csv_to_raw(
    file_path,
):
    run_id = str(
        uuid4()
    )

    spark = (
        create_spark_session()
    )

    start_time = (
        time.time()
    )

    raw_dataframe = None
    valid_candidates = None
    quarantine_dataframe = None
    validated_dataframe = None

    try:
        print(
            "\n=== PySpark Raw Load ==="
        )

        dataframe = (
            spark.read
            .option(
                "header",
                True,
            )
            .option(
                "mode",
                "PERMISSIVE",
            )
            .option(
                "quote",
                '"',
            )
            .option(
                "escape",
                '"',
            )
            .option(
                "multiLine",
                False,
            )
            .schema(
                ORDER_SCHEMA
            )
            .csv(
                file_path
            )
        )

        input_partitions = (
            dataframe
            .rdd
            .getNumPartitions()
        )

        print(
            f"Input partitions: "
            f"{input_partitions}"
        )

        raw_columns = [
            col(
                column_name
            )
            for column_name
            in dataframe.columns
        ]

        raw_dataframe = (
            dataframe
            .withColumn(
                "source_row_number",
                monotonically_increasing_id()
                + 2,
            )
            .withColumn(
                "run_id",
                lit(
                    run_id
                ),
            )
            .withColumn(
                "source_file",
                lit(
                    file_path
                ),
            )
            .withColumn(
                "ingested_at",
                lit(
                    datetime.now(
                        timezone.utc
                    ).isoformat()
                ),
            )
            .withColumn(
                "engine_used",
                lit(
                    "pyspark"
                ),
            )
            .withColumn(
                "raw_record",
                struct(
                    *raw_columns
                ),
            )
            .select(
                "run_id",
                "source_file",
                "source_row_number",
                "ingested_at",
                "engine_used",
                "raw_record",
            )
            .cache()
        )

        raw_count = (
            raw_dataframe
            .count()
        )

        write_mongodb(
            raw_dataframe,
            RAW_COLLECTION,
        )

        print(
            "\n=== Spark Quality ==="
        )

        (
            valid_candidates,
            quarantine_dataframe,
        ) = (
            transform_spark_raw(
                raw_dataframe
            )
        )

        valid_candidates = (
            valid_candidates
            .cache()
        )

        quarantine_dataframe = (
            quarantine_dataframe
            .cache()
        )

        quarantine_count = (
            quarantine_dataframe
            .count()
        )

        accepted_before_dedup = (
            valid_candidates
            .count()
        )

        validated_dataframe = (
            deduplicate_validated(
                valid_candidates
            )
            .cache()
        )

        validated_unique_count = (
            validated_dataframe
            .count()
        )

        duplicate_count = (
            accepted_before_dedup
            - validated_unique_count
        )

        corrected_count = (
            validated_dataframe
            .filter(
                col(
                    "quality_status"
                )
                == "corrected"
            )
            .count()
        )

        valid_count = (
            validated_dataframe
            .filter(
                col(
                    "quality_status"
                )
                == "valid"
            )
            .count()
        )

        classified_count = (
            accepted_before_dedup
            + quarantine_count
        )

        print(
            f"Valid: "
            f"{valid_count}"
        )

        print(
            f"Corrected: "
            f"{corrected_count}"
        )

        print(
            f"Quarantine: "
            f"{quarantine_count}"
        )

        print(
            f"Duplicate business keys: "
            f"{duplicate_count}"
        )

        print(
            f"Validated unique: "
            f"{validated_unique_count}"
        )

        print(
            f"Consistency check: "
            f"{raw_count} = "
            f"{classified_count}"
        )

        if quarantine_count > 0:
            write_mongodb(
                quarantine_dataframe,
                QUARANTINE_COLLECTION,
            )

        print(
            "\n=== Distributed MongoDB Upsert ==="
        )

        upsert_result = (
            distributed_upsert(
                validated_dataframe,
                partitions=32,
            )
        )

        print(
            f"Inserted: "
            f"{upsert_result['inserted']}"
        )

        print(
            f"Updated: "
            f"{upsert_result['updated']}"
        )

        print(
            f"Unchanged: "
            f"{upsert_result['unchanged']}"
        )

        print(
            f"Upsert partitions: "
            f"{upsert_result['upsert_partitions']}"
        )

        elapsed = (
            time.time()
            - start_time
        )

        throughput = 0.0

        if elapsed > 0:
            throughput = (
                raw_count
                / elapsed
            )

        print(
            f"\nRun ID: "
            f"{run_id}"
        )

        print(
            f"Loaded raw rows: "
            f"{raw_count}"
        )

        print(
            f"Input partitions: "
            f"{input_partitions}"
        )

        print(
            f"Elapsed seconds: "
            f"{elapsed:.2f}"
        )

        print(
            f"Throughput: "
            f"{throughput:.2f} "
            f"rows/second"
        )

        return {
            "run_id": (
                run_id
            ),

            "raw_count": (
                raw_count
            ),

            "valid_count": (
                valid_count
            ),

            "corrected_count": (
                corrected_count
            ),

            "quarantine_count": (
                quarantine_count
            ),

            "duplicate_count": (
                duplicate_count
            ),

            "validated_unique_count": (
                validated_unique_count
            ),

            "inserted": (
                upsert_result[
                    "inserted"
                ]
            ),

            "updated": (
                upsert_result[
                    "updated"
                ]
            ),

            "unchanged": (
                upsert_result[
                    "unchanged"
                ]
            ),

            "partitions": (
                input_partitions
            ),

            "upsert_partitions": (
                upsert_result[
                    "upsert_partitions"
                ]
            ),

            "elapsed_seconds": (
                elapsed
            ),

            "throughput": (
                throughput
            ),
        }

    finally:
        try:
            if (
                validated_dataframe
                is not None
            ):
                validated_dataframe.unpersist()

            if (
                valid_candidates
                is not None
            ):
                valid_candidates.unpersist()

            if (
                quarantine_dataframe
                is not None
            ):
                quarantine_dataframe.unpersist()

            if (
                raw_dataframe
                is not None
            ):
                raw_dataframe.unpersist()

        except Exception:
            pass

        try:
            spark.stop()
        except Exception:
            pass