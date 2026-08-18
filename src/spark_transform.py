from pyspark.sql.functions import (
    array,
    coalesce,
    col,
    concat,
    filter,
    from_json,
    lit,
    lower,
    regexp_replace,
    size,
    transform,
    translate,
    trim,
    try_to_timestamp,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


ITEM_SCHEMA = ArrayType(
    StructType([
        StructField("sku", StringType(), True),
        StructField("name", StringType(), True),
        StructField("qty", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total", DoubleType(), True),
    ])
)


def number_column(column):
    value = translate(
        column.cast("string"),
        "٠١٢٣٤٥٦٧٨٩٫٬",
        "0123456789.,",
    )

    value = regexp_replace(
        value,
        ",",
        "",
    )

    value = regexp_replace(
        value,
        r"[^0-9.\-]",
        "",
    )

    return (
        when(
            trim(value) == "",
            None,
        )
        .otherwise(
            value.cast("double")
        )
    )


def transform_spark_raw(raw_dataframe):
    raw = col("raw_record")

    df = raw_dataframe

    # --------------------------------
    # 1. Trim / basic normalization
    # --------------------------------

    df = (
        df
        .withColumn(
            "order_id_clean",
            trim(raw["order_id"]),
        )
        .withColumn(
            "customer_id_clean",
            trim(raw["customer_id"]),
        )
        .withColumn(
            "customer_name_clean",
            trim(raw["customer_name"]),
        )
        .withColumn(
            "city_clean",
            trim(raw["city"]),
        )
        .withColumn(
            "district_clean",
            trim(raw["district"]),
        )
    )

    # --------------------------------
    # 2. Safe date normalization
    # --------------------------------

    date_text = trim(
        raw["order_date"]
    )

    df = df.withColumn(
        "order_date_clean",
        coalesce(
            try_to_timestamp(
                date_text,
                lit("yyyy-MM-dd'T'HH:mm:ss"),
            ),
            try_to_timestamp(
                date_text,
                lit("yyyy-MM-dd HH:mm:ss"),
            ),
            try_to_timestamp(
                date_text,
                lit("dd-MM-yyyy HH:mm:ss"),
            ),
            try_to_timestamp(
                date_text,
                lit("dd/MM/yyyy HH:mm:ss"),
            ),
            try_to_timestamp(
                date_text,
                lit("yyyy/MM/dd HH:mm:ss"),
            ),
            try_to_timestamp(
                date_text,
                lit("yyyy-MM-dd"),
            ),
            try_to_timestamp(
                date_text,
                lit("dd/MM/yyyy"),
            ),
            try_to_timestamp(
                date_text,
                lit("yyyy/MM/dd"),
            ),
        ),
    )

    # --------------------------------
    # 3. Phone normalization
    # --------------------------------

    df = df.withColumn(
        "phone_clean",
        regexp_replace(
            raw["customer_phone"],
            r"\D",
            "",
        ),
    )

    df = df.withColumn(
        "phone_clean",
        when(
            col("phone_clean").startswith("967"),
            regexp_replace(
                col("phone_clean"),
                r"^967",
                "",
            ),
        ).otherwise(
            col("phone_clean")
        ),
    )

    # --------------------------------
    # 4. Email normalization
    # --------------------------------

    df = df.withColumn(
        "email_clean",
        lower(
            trim(
                regexp_replace(
                    regexp_replace(
                        raw["customer_email"],
                        r"@+",
                        "@",
                    ),
                    r"\.{2,}",
                    ".",
                )
            )
        ),
    )

    # --------------------------------
    # 5. Status aliases
    # --------------------------------

    df = df.withColumn(
        "status_clean",
        when(
            trim(raw["status"]) == "مؤكد",
            "confirmed",
        )
        .when(
            trim(raw["status"]) == "قيد الانتظار",
            "pending",
        )
        .when(
            trim(raw["status"]) == "مرتجع",
            "returned",
        )
        .otherwise(
            trim(raw["status"])
        ),
    )

    # --------------------------------
    # 6. Payment status aliases
    # --------------------------------

    df = df.withColumn(
        "payment_status_clean",
        when(
            trim(raw["payment_status"]).isin(
                "تم الدفع",
                "مدفوع",
            ),
            "paid",
        )
        .when(
            trim(raw["payment_status"]) == "بانتظار الدفع",
            "pending",
        )
        .otherwise(
            trim(raw["payment_status"])
        ),
    )

    # --------------------------------
    # 7. Currency normalization
    # --------------------------------

    df = df.withColumn(
        "currency_clean",
        when(
            trim(raw["currency"]).isin(
                "ريال",
                "ريال يمني",
            ),
            "YER",
        )
        .otherwise(
            trim(raw["currency"])
        ),
    )

    # --------------------------------
    # 8. Numeric normalization
    # --------------------------------

    df = (
        df
        .withColumn(
            "delivery_cost_clean",
            number_column(
                raw["delivery_cost"]
            ),
        )
        .withColumn(
            "payment_amount_clean",
            number_column(
                raw["payment_amount"]
            ),
        )
        .withColumn(
            "total_amount_clean",
            number_column(
                raw["total_amount"]
            ),
        )
    )

    # --------------------------------
    # 9. Parse items JSON
    # --------------------------------

    df = df.withColumn(
        "items_clean",
        from_json(
            raw["items_json"],
            ITEM_SCHEMA,
        ),
    )

    # --------------------------------
    # 10. Detect negative item values
    # --------------------------------

    df = df.withColumn(
        "negative_item_flags",
        when(
            col("items_clean").isNull(),
            array(),
        ).otherwise(
            transform(
                col("items_clean"),
                lambda item: when(
                    (item["qty"] < 0)
                    | (item["unit_price"] < 0),
                    lit(1),
                ).otherwise(
                    lit(0)
                ),
            )
        ),
    )

    df = df.withColumn(
        "negative_item_count",
        size(
            filter(
                col("negative_item_flags"),
                lambda x: x == 1,
            )
        ),
    )

    # --------------------------------
    # 11. Error codes
    # --------------------------------

    df = df.withColumn(
        "error_codes_raw",
        array(
            when(
                col("order_id_clean").isNull()
                | (col("order_id_clean") == ""),
                lit("ID_ORDER_MISSING"),
            ),

            when(
                col("customer_id_clean").isNull()
                | (col("customer_id_clean") == ""),
                lit("ID_CUSTOMER_MISSING"),
            ),

            when(
                col("order_date_clean").isNull(),
                lit("DATE_INVALID_IMPOSSIBLE"),
            ),

            when(
                raw["customer_phone"].isNotNull()
                & (
                    ~col("phone_clean").rlike(
                        r"^\d{9}$"
                    )
                ),
                lit("PHONE_INVALID"),
            ),

            when(
                raw["customer_email"].isNotNull()
                & (
                    ~col("email_clean").rlike(
                        r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
                    )
                ),
                lit("EMAIL_INVALID"),
            ),

            when(
                col("delivery_cost_clean").isNull(),
                lit("DELIVERY_COST_UNKNOWN"),
            ),

            when(
                col("payment_amount_clean").isNull(),
                lit("PAYMENT_AMOUNT_UNKNOWN"),
            ),

            when(
                col("total_amount_clean").isNull(),
                lit("PRICE_UNKNOWN"),
            ),

            when(
                raw["items_json"].isNull()
                | (
                    trim(raw["items_json"])
                    == ""
                ),
                lit("ITEMS_EMPTY"),
            ),

            when(
                raw["items_json"].isNotNull()
                & col("items_clean").isNull(),
                lit("JSON_ITEMS_CORRUPTED"),
            ),

            when(
                col("negative_item_count") > 0,
                lit("VALUE_NEGATIVE_AMBIGUOUS"),
            ),
        ),
    )

    df = df.withColumn(
        "error_codes",
        filter(
            col("error_codes_raw"),
            lambda x: x.isNotNull(),
        ),
    )

    # --------------------------------
    # 12. Audit trail
    # --------------------------------

    df = df.withColumn(
        "corrections_raw",
        array(
            when(
                ~trim(
                    raw["order_date"]
                ).rlike(
                    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"
                )
                & raw["order_date"].isNotNull()
                & col("order_date_clean").isNotNull(),
                concat(
                    lit("DATE_NORMALIZE: "),
                    raw["order_date"],
                    lit(" -> "),
                    col(
                        "order_date_clean"
                    ).cast("string"),
                ),
            ),

            when(
                raw["customer_phone"]
                != col("phone_clean"),
                concat(
                    lit("PHONE_NORMALIZE: "),
                    raw["customer_phone"],
                    lit(" -> "),
                    col("phone_clean"),
                ),
            ),

            when(
                lower(
                    trim(
                        raw["customer_email"]
                    )
                )
                != col("email_clean"),
                concat(
                    lit("EMAIL_NORMALIZE: "),
                    raw["customer_email"],
                    lit(" -> "),
                    col("email_clean"),
                ),
            ),

            when(
                trim(raw["status"])
                != col("status_clean"),
                concat(
                    lit("STATUS_ALIAS: "),
                    raw["status"],
                    lit(" -> "),
                    col("status_clean"),
                ),
            ),

            when(
                trim(raw["payment_status"])
                != col("payment_status_clean"),
                concat(
                    lit("PAYMENT_STATUS_ALIAS: "),
                    raw["payment_status"],
                    lit(" -> "),
                    col("payment_status_clean"),
                ),
            ),

            when(
                trim(raw["currency"])
                != col("currency_clean"),
                concat(
                    lit("CURRENCY_NORMALIZE: "),
                    raw["currency"],
                    lit(" -> "),
                    col("currency_clean"),
                ),
            ),

            when(
                raw["delivery_cost"]
                != col(
                    "delivery_cost_clean"
                ).cast("string"),
                lit(
                    "NUMBER_NORMALIZE: delivery_cost"
                ),
            ),

            when(
                raw["payment_amount"]
                != col(
                    "payment_amount_clean"
                ).cast("string"),
                lit(
                    "NUMBER_NORMALIZE: payment_amount"
                ),
            ),

            when(
                raw["total_amount"]
                != col(
                    "total_amount_clean"
                ).cast("string"),
                lit(
                    "NUMBER_NORMALIZE: total_amount"
                ),
            ),
        ),
    )

    df = df.withColumn(
        "corrections",
        filter(
            col("corrections_raw"),
            lambda x: x.isNotNull(),
        ),
    )

    # --------------------------------
    # 13. Quarantine
    # --------------------------------

    quarantine_df = (
        df
        .filter(
            size(
                col("error_codes")
            ) > 0
        )
        .select(
            "run_id",
            raw["order_id"].alias(
                "order_id"
            ),
            "error_codes",
            col("error_codes").alias(
                "error_details"
            ),
            raw.alias(
                "raw_record"
            ),
        )
    )

    # --------------------------------
    # 14. Valid / Corrected
    # --------------------------------

    valid_candidates_df = (
        df
        .filter(
            size(
                col("error_codes")
            ) == 0
        )
        .withColumn(
            "quality_status",
            when(
                size(
                    col("corrections")
                ) > 0,
                lit("corrected"),
            ).otherwise(
                lit("valid")
            ),
        )
        .select(
            col(
                "order_id_clean"
            ).alias(
                "order_id"
            ),

            col(
                "order_date_clean"
            ).cast(
                "string"
            ).alias(
                "order_date"
            ),

            col(
                "status_clean"
            ).alias(
                "status"
            ),

            col(
                "customer_id_clean"
            ).alias(
                "customer_id"
            ),

            col(
                "customer_name_clean"
            ).alias(
                "customer_name"
            ),

            col(
                "phone_clean"
            ).alias(
                "customer_phone"
            ),

            col(
                "email_clean"
            ).alias(
                "customer_email"
            ),

            col(
                "city_clean"
            ).alias(
                "city"
            ),

            col(
                "district_clean"
            ).alias(
                "district"
            ),

            raw[
                "delivery_type"
            ].alias(
                "delivery_type"
            ),

            col(
                "delivery_cost_clean"
            ).alias(
                "delivery_cost"
            ),

            raw[
                "payment_method"
            ].alias(
                "payment_method"
            ),

            col(
                "payment_status_clean"
            ).alias(
                "payment_status"
            ),

            col(
                "payment_amount_clean"
            ).alias(
                "payment_amount"
            ),

            col(
                "currency_clean"
            ).alias(
                "currency"
            ),

            col(
                "total_amount_clean"
            ).alias(
                "total_amount"
            ),

            col(
                "items_clean"
            ).alias(
                "items"
            ),

            "quality_status",
            "corrections",

            col(
                "run_id"
            ).alias(
                "last_run_id"
            ),
        )
    )

    return (
        valid_candidates_df,
        quarantine_df,
    )