from pyspark.sql.functions import (
    abs as spark_abs,
    aggregate,
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
        StructField(
            "sku",
            StringType(),
            True,
        ),
        StructField(
            "name",
            StringType(),
            True,
        ),
        StructField(
            "qty",
            IntegerType(),
            True,
        ),
        StructField(
            "unit_price",
            DoubleType(),
            True,
        ),
        StructField(
            "total",
            DoubleType(),
            True,
        ),
    ])
)


def number_column(column):
    text = trim(
        column.cast(
            "string"
        )
    )

    known_price = (
        when(
            text == "ألف",
            lit(1000.0),
        )
        .when(
            text == "ألفان",
            lit(2000.0),
        )
        .when(
            text == "ثلاثة آلاف",
            lit(3000.0),
        )
        .when(
            text == "أربعة آلاف",
            lit(4000.0),
        )
        .when(
            text == "خمسة آلاف",
            lit(5000.0),
        )
    )

    value = translate(
        text,
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

    numeric_value = (
        when(
            trim(value) == "",
            None,
        )
        .otherwise(
            value.cast(
                "double"
            )
        )
    )

    return coalesce(
        known_price,
        numeric_value,
    )


def transform_spark_raw(
    raw_dataframe,
):
    raw = col(
        "raw_record"
    )

    df = raw_dataframe

    df = (
        df
        .withColumn(
            "order_id_clean",
            trim(
                raw["order_id"]
            ),
        )
        .withColumn(
            "customer_id_clean",
            trim(
                raw[
                    "customer_id"
                ]
            ),
        )
        .withColumn(
            "customer_name_clean",
            trim(
                raw[
                    "customer_name"
                ]
            ),
        )
        .withColumn(
            "city_clean",
            trim(
                raw["city"]
            ),
        )
        .withColumn(
            "district_clean",
            trim(
                raw[
                    "district"
                ]
            ),
        )
    )

    date_text = trim(
        raw[
            "order_date"
        ]
    )

    df = df.withColumn(
        "order_date_clean",
        coalesce(
            try_to_timestamp(
                date_text,
                lit(
                    "yyyy-MM-dd'T'HH:mm:ss"
                ),
            ),
            try_to_timestamp(
                date_text,
                lit(
                    "yyyy-MM-dd HH:mm:ss"
                ),
            ),
            try_to_timestamp(
                date_text,
                lit(
                    "dd-MM-yyyy HH:mm:ss"
                ),
            ),
            try_to_timestamp(
                date_text,
                lit(
                    "dd/MM/yyyy HH:mm:ss"
                ),
            ),
            try_to_timestamp(
                date_text,
                lit(
                    "yyyy/MM/dd HH:mm:ss"
                ),
            ),
            try_to_timestamp(
                date_text,
                lit(
                    "yyyy-MM-dd"
                ),
            ),
            try_to_timestamp(
                date_text,
                lit(
                    "dd/MM/yyyy"
                ),
            ),
            try_to_timestamp(
                date_text,
                lit(
                    "yyyy/MM/dd"
                ),
            ),
        ),
    )

    df = df.withColumn(
        "phone_clean",
        regexp_replace(
            raw[
                "customer_phone"
            ],
            r"\D",
            "",
        ),
    )

    df = df.withColumn(
        "phone_clean",
        when(
            col(
                "phone_clean"
            ).startswith(
                "967"
            ),
            regexp_replace(
                col(
                    "phone_clean"
                ),
                r"^967",
                "",
            ),
        ).otherwise(
            col(
                "phone_clean"
            )
        ),
    )

    df = df.withColumn(
        "email_clean",
        lower(
            trim(
                regexp_replace(
                    regexp_replace(
                        raw[
                            "customer_email"
                        ],
                        r"@+",
                        "@",
                    ),
                    r"\.{2,}",
                    ".",
                )
            )
        ),
    )

    df = df.withColumn(
        "status_clean",
        when(
            trim(
                raw["status"]
            )
            == "مؤكد",
            "confirmed",
        )
        .when(
            trim(
                raw["status"]
            )
            == "قيد الانتظار",
            "pending",
        )
        .when(
            trim(
                raw["status"]
            )
            == "مرتجع",
            "returned",
        )
        .otherwise(
            trim(
                raw["status"]
            )
        ),
    )

    df = df.withColumn(
        "payment_status_clean",
        when(
            trim(
                raw[
                    "payment_status"
                ]
            ).isin(
                "تم الدفع",
                "مدفوع",
            ),
            "paid",
        )
        .when(
            trim(
                raw[
                    "payment_status"
                ]
            )
            == "بانتظار الدفع",
            "pending",
        )
        .otherwise(
            trim(
                raw[
                    "payment_status"
                ]
            )
        ),
    )

    df = df.withColumn(
        "currency_clean",
        when(
            trim(
                raw["currency"]
            ).isin(
                "ريال",
                "ريال يمني",
            ),
            "YER",
        )
        .otherwise(
            trim(
                raw["currency"]
            )
        ),
    )

    df = (
        df
        .withColumn(
            "delivery_cost_clean",
            number_column(
                raw[
                    "delivery_cost"
                ]
            ),
        )
        .withColumn(
            "payment_amount_clean",
            number_column(
                raw[
                    "payment_amount"
                ]
            ),
        )
        .withColumn(
            "total_amount_clean",
            number_column(
                raw[
                    "total_amount"
                ]
            ),
        )
    )

    df = df.withColumn(
        "items_clean",
        from_json(
            raw[
                "items_json"
            ],
            ITEM_SCHEMA,
        ),
    )

    df = df.withColumn(
        "negative_item_flags",
        when(
            col(
                "items_clean"
            ).isNull(),
            array(),
        ).otherwise(
            transform(
                col(
                    "items_clean"
                ),
                lambda item: when(
                    (
                        item[
                            "qty"
                        ] < 0
                    )
                    | (
                        item[
                            "unit_price"
                        ] < 0
                    )
                    | (
                        item[
                            "total"
                        ].isNotNull()
                        & (
                            item[
                                "total"
                            ] < 0
                        )
                    ),
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
                col(
                    "negative_item_flags"
                ),
                lambda x: (
                    x == 1
                ),
            )
        ),
    )

    df = df.withColumn(
        "items_total_recomputed",
        when(
            col(
                "items_clean"
            ).isNull(),
            None,
        ).otherwise(
            aggregate(
                col(
                    "items_clean"
                ),
                lit(0.0),
                lambda acc, item: (
                    acc
                    + coalesce(
                        item[
                            "total"
                        ],
                        (
                            item[
                                "qty"
                            ].cast(
                                "double"
                            )
                            * item[
                                "unit_price"
                            ]
                        ),
                    )
                ),
            )
        ),
    )

    df = df.withColumn(
        "total_recomputed",
        when(
            col(
                "items_total_recomputed"
            ).isNotNull()
            & col(
                "delivery_cost_clean"
            ).isNotNull()
            & (
                col(
                    "negative_item_count"
                ) == 0
            ),
            (
                col(
                    "items_total_recomputed"
                )
                + col(
                    "delivery_cost_clean"
                )
            ),
        ),
    )

    df = df.withColumn(
        "total_should_recompute",
        (
            col(
                "total_recomputed"
            ).isNotNull()
            & col(
                "total_amount_clean"
            ).isNotNull()
            & (
                spark_abs(
                    col(
                        "total_recomputed"
                    )
                    - col(
                        "total_amount_clean"
                    )
                )
                > lit(
                    0.01
                )
            )
        ),
    )

    df = df.withColumn(
        "total_amount_final",
        when(
            col(
                "total_should_recompute"
            ),
            col(
                "total_recomputed"
            ),
        ).otherwise(
            col(
                "total_amount_clean"
            )
        ),
    )

    df = df.withColumn(
        "error_codes_raw",
        array(
            when(
                col(
                    "order_id_clean"
                ).isNull()
                | (
                    col(
                        "order_id_clean"
                    )
                    == ""
                ),
                lit(
                    "ID_ORDER_MISSING"
                ),
            ),

            when(
                col(
                    "customer_id_clean"
                ).isNull()
                | (
                    col(
                        "customer_id_clean"
                    )
                    == ""
                ),
                lit(
                    "ID_CUSTOMER_MISSING"
                ),
            ),

            when(
                col(
                    "order_date_clean"
                ).isNull(),
                lit(
                    "DATE_INVALID_IMPOSSIBLE"
                ),
            ),

            when(
                raw[
                    "customer_phone"
                ].isNotNull()
                & (
                    ~col(
                        "phone_clean"
                    ).rlike(
                        r"^\d{9}$"
                    )
                ),
                lit(
                    "PHONE_INVALID"
                ),
            ),

            when(
                raw[
                    "customer_email"
                ].isNotNull()
                & (
                    ~col(
                        "email_clean"
                    ).rlike(
                        r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
                    )
                ),
                lit(
                    "EMAIL_INVALID"
                ),
            ),

            when(
                col(
                    "delivery_cost_clean"
                ).isNull(),
                lit(
                    "DELIVERY_COST_UNKNOWN"
                ),
            ),

            when(
                col(
                    "payment_amount_clean"
                ).isNull(),
                lit(
                    "PAYMENT_AMOUNT_UNKNOWN"
                ),
            ),

            when(
                col(
                    "total_amount_clean"
                ).isNull(),
                lit(
                    "PRICE_UNKNOWN"
                ),
            ),

            when(
                raw[
                    "items_json"
                ].isNull()
                | (
                    trim(
                        raw[
                            "items_json"
                        ]
                    )
                    == ""
                ),
                lit(
                    "ITEMS_EMPTY"
                ),
            ),

            when(
                raw[
                    "items_json"
                ].isNotNull()
                & col(
                    "items_clean"
                ).isNull(),
                lit(
                    "JSON_ITEMS_CORRUPTED"
                ),
            ),

            when(
                col(
                    "negative_item_count"
                ) > 0,
                lit(
                    "VALUE_NEGATIVE_AMBIGUOUS"
                ),
            ),
        ),
    )

    df = df.withColumn(
        "error_codes",
        filter(
            col(
                "error_codes_raw"
            ),
            lambda x: (
                x.isNotNull()
            ),
        ),
    )

    df = df.withColumn(
        "corrections_raw",
        array(
            when(
                ~trim(
                    raw[
                        "order_date"
                    ]
                ).rlike(
                    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"
                )
                & raw[
                    "order_date"
                ].isNotNull()
                & col(
                    "order_date_clean"
                ).isNotNull(),
                concat(
                    lit(
                        "DATE_NORMALIZE: "
                    ),
                    raw[
                        "order_date"
                    ],
                    lit(
                        " -> "
                    ),
                    col(
                        "order_date_clean"
                    ).cast(
                        "string"
                    ),
                ),
            ),

            when(
                raw[
                    "customer_phone"
                ]
                != col(
                    "phone_clean"
                ),
                concat(
                    lit(
                        "PHONE_NORMALIZE: "
                    ),
                    raw[
                        "customer_phone"
                    ],
                    lit(
                        " -> "
                    ),
                    col(
                        "phone_clean"
                    ),
                ),
            ),

            when(
                lower(
                    trim(
                        raw[
                            "customer_email"
                        ]
                    )
                )
                != col(
                    "email_clean"
                ),
                concat(
                    lit(
                        "EMAIL_NORMALIZE: "
                    ),
                    raw[
                        "customer_email"
                    ],
                    lit(
                        " -> "
                    ),
                    col(
                        "email_clean"
                    ),
                ),
            ),

            when(
                trim(
                    raw[
                        "status"
                    ]
                )
                != col(
                    "status_clean"
                ),
                concat(
                    lit(
                        "STATUS_ALIAS: "
                    ),
                    raw[
                        "status"
                    ],
                    lit(
                        " -> "
                    ),
                    col(
                        "status_clean"
                    ),
                ),
            ),

            when(
                trim(
                    raw[
                        "payment_status"
                    ]
                )
                != col(
                    "payment_status_clean"
                ),
                concat(
                    lit(
                        "PAYMENT_STATUS_ALIAS: "
                    ),
                    raw[
                        "payment_status"
                    ],
                    lit(
                        " -> "
                    ),
                    col(
                        "payment_status_clean"
                    ),
                ),
            ),

            when(
                trim(
                    raw[
                        "currency"
                    ]
                )
                != col(
                    "currency_clean"
                ),
                concat(
                    lit(
                        "CURRENCY_NORMALIZE: "
                    ),
                    raw[
                        "currency"
                    ],
                    lit(
                        " -> "
                    ),
                    col(
                        "currency_clean"
                    ),
                ),
            ),

            when(
                raw[
                    "delivery_cost"
                ]
                != col(
                    "delivery_cost_clean"
                ).cast(
                    "string"
                ),
                concat(
                    lit(
                        "NUMBER_NORMALIZE: "
                    ),
                    raw[
                        "delivery_cost"
                    ],
                    lit(
                        " -> "
                    ),
                    col(
                        "delivery_cost_clean"
                    ).cast(
                        "string"
                    ),
                ),
            ),

            when(
                raw[
                    "payment_amount"
                ]
                != col(
                    "payment_amount_clean"
                ).cast(
                    "string"
                ),
                concat(
                    lit(
                        "NUMBER_NORMALIZE: "
                    ),
                    raw[
                        "payment_amount"
                    ],
                    lit(
                        " -> "
                    ),
                    col(
                        "payment_amount_clean"
                    ).cast(
                        "string"
                    ),
                ),
            ),

            when(
                raw[
                    "total_amount"
                ]
                != col(
                    "total_amount_clean"
                ).cast(
                    "string"
                ),
                concat(
                    lit(
                        "NUMBER_NORMALIZE: "
                    ),
                    raw[
                        "total_amount"
                    ],
                    lit(
                        " -> "
                    ),
                    col(
                        "total_amount_clean"
                    ).cast(
                        "string"
                    ),
                ),
            ),

            when(
                col(
                    "total_should_recompute"
                ),
                concat(
                    lit(
                        "TOTAL_RECOMPUTE: "
                    ),
                    col(
                        "total_amount_clean"
                    ).cast(
                        "string"
                    ),
                    lit(
                        " -> "
                    ),
                    col(
                        "total_recomputed"
                    ).cast(
                        "string"
                    ),
                ),
            ),
        ),
    )

    df = df.withColumn(
        "corrections",
        filter(
            col(
                "corrections_raw"
            ),
            lambda x: (
                x.isNotNull()
            ),
        ),
    )

    quarantine_df = (
        df
        .filter(
            size(
                col(
                    "error_codes"
                )
            ) > 0
        )
        .select(
            "run_id",

            raw[
                "order_id"
            ].alias(
                "order_id"
            ),

            "error_codes",

            col(
                "error_codes"
            ).alias(
                "error_details"
            ),

            raw.alias(
                "raw_record"
            ),
        )
    )

    valid_candidates_df = (
        df
        .filter(
            size(
                col(
                    "error_codes"
                )
            ) == 0
        )
        .withColumn(
            "quality_status",
            when(
                size(
                    col(
                        "corrections"
                    )
                ) > 0,
                lit(
                    "corrected"
                ),
            ).otherwise(
                lit(
                    "valid"
                )
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
                "total_amount_final"
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