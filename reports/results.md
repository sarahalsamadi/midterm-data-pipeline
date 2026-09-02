# Data Pipeline Results Report

This report summarizes the verified execution results of the Midterm Data Pipeline project using both Python Batch and PySpark processing paths.

---

## 1. Python Batch Execution

A 5.01 MB CSV file containing 12,000 records was processed using the Python Batch engine.

### Router Decision

```text
File size: 5.01 MB
Threshold: 200 MB
Selected engine: python_batch
Reason: File size 5.01 MB is less than or equal to the 200 MB threshold.
```

### Raw Batch Loading

```text
Batch 1: 5000 rows, 0.08 seconds, 63614.83 rows/second
Batch 2: 5000 rows, 0.06 seconds, 82279.33 rows/second
Batch 3: 2000 rows, 0.02 seconds, 84290.68 rows/second

Loaded raw rows: 12000
Total batches: 3
Raw load elapsed time: 0.41 seconds
Raw load throughput: 29412.80 rows/second
```

The configured batch size was 5,000 records. The file was streamed and inserted into MongoDB in three batches without loading the complete CSV file into memory.

### ELT and Data Quality Results

```text
Raw: 12000
Valid: 0
Corrected: 11083
Quarantine: 917
Inserted: 10982
Updated: 94
Unchanged: 7
Consistency check: 12000 = 12000
```

The consistency condition was satisfied:

```text
Raw = Valid + Corrected + Quarantine
12000 = 0 + 11083 + 917
```

### Error Distribution

| Error Code | Count |
|---|---:|
| VALUE_NEGATIVE_AMBIGUOUS | 111 |
| ID_CUSTOMER_MISSING | 183 |
| JSON_ITEMS_CORRUPTED | 168 |
| PRICE_UNKNOWN | 168 |
| ID_ORDER_MISSING | 95 |
| PHONE_INVALID | 79 |
| ITEMS_EMPTY | 102 |
| EMAIL_INVALID | 94 |
| DATE_INVALID_IMPOSSIBLE | 77 |

A quarantined record may contain more than one error code; therefore, the sum of error-code occurrences does not have to equal the number of quarantined records.

---

## 2. PySpark Execution

A 209.20 MB CSV file containing 500,000 records was processed using PySpark.

### Router Decision

```text
File size: 209.20 MB
Threshold: 200 MB
Selected engine: pyspark
Reason: File size 209.20 MB is greater than the 200 MB threshold.
```

This demonstrates that the same pipeline entry point automatically selects the processing engine according to the configured 200 MB threshold.

### PySpark Results

```text
Raw: 500000
Valid: 1872
Corrected: 470254
Quarantine: 27874
Duplicate business keys: 3278
Validated unique: 468848

Inserted: 468848
Updated: 0
Unchanged: 0

Input partitions: 16
```

### Consistency Verification

```text
500000 = 1872 + 470254 + 27874
```

Therefore, every raw record was classified as Valid, Corrected, or Quarantined. No record was silently dropped.

The accepted records before duplicate handling were:

```text
1872 + 470254 = 472126
```

After duplicate business-key handling:

```text
472126 - 3278 = 468848
```

Therefore:

```text
Validated unique = 468848
```

---

## 3. Spark Partitioning and Shuffle Evidence

The 209.20 MB Spark input was initially processed using:

```text
Input partitions: 16
```

Spark SQL shuffle partitions were configured as:

```text
spark.sql.shuffle.partitions = 64
```

The validated data was explicitly repartitioned by `order_id` before the distributed MongoDB upsert:

```text
Upsert partitions: 8
Upsert batch size: 500
```

Spark UI was used during execution to inspect Jobs, Stages, Tasks, Executors, Storage, SQL/DataFrame executions, Shuffle Read, and Shuffle Write.

The Spark UI evidence is stored under:

```text
reports/screenshots/
```

---

## 4. Idempotency Verification

Idempotency was tested using the same 10-row input file.

### First Run

```text
Raw: 10
Corrected: 8
Quarantine: 2
Inserted: 8
Updated: 0
Unchanged: 0
```

Eight accepted business records were inserted into `orders_validated`.

### Second Run — Same Input

The exact same input was processed again.

```text
Raw: 10
Corrected: 8
Quarantine: 2
Inserted: 0
Updated: 0
Unchanged: 8
```

The number of validated business records did not increase.

This verifies that rerunning the same input does not create duplicate business records.

---

## 5. Upsert Update Verification

A controlled test was performed by changing the `customer_name` of one existing order while keeping the same `order_id`.

Result:

```text
Inserted: 0
Updated: 1
Unchanged: 7
Validated records: 8
```

This demonstrates that the validated collection uses business-key-based upsert behavior.

A changed existing record is updated rather than inserted as a duplicate.

---

## 6. MongoDB Verification

The pipeline uses three MongoDB collections:

```text
orders_raw
orders_validated
orders_quarantine
```

### orders_raw

Stores the original source record together with ingestion metadata such as:

- `run_id`
- `source_file`
- `source_row_number`
- `ingested_at`
- `engine_used`
- `raw_record`

Raw data is stored before quality correction.

### orders_validated

Stores accepted business records after validation and safe correction.

A unique index exists on:

```text
order_id
```

Index name:

```text
uq_order_id
```

The collection also uses MongoDB schema validation.

### orders_quarantine

Stores records that cannot be safely corrected together with:

- the original raw record;
- error codes;
- error details;
- run information.

This ensures that invalid records are preserved instead of silently discarded.

---

## 7. Python Batch vs PySpark

| Feature | Python Batch | PySpark |
|---|---|---|
| Intended workload | Small files | Large files |
| Router threshold | <= 200 MB | > 200 MB |
| Processing method | Streaming batches | Distributed DataFrame processing |
| Full-file Python loading | No | No |
| Raw-first ELT | Yes | Yes |
| Data quality rules | Yes | Yes |
| Quarantine | Yes | Yes |
| Audit trail | Yes | Yes |
| MongoDB final load | Upsert | Distributed Upsert |
| Idempotency | Yes | Yes |
| Metrics | Yes | Yes |

The hybrid design avoids Spark startup overhead for smaller files while using Spark parallelism for larger workloads.

---

## 8. Automated Tests

The final automated test suite completed successfully:

```text
Ran 21 tests in 0.009s

OK
```

The tests cover:

- file-size routing;
- 200 MB threshold behavior;
- consistency;
- idempotency;
- update detection;
- Arabic digit normalization;
- thousands separators;
- known price words;
- currency normalization;
- date normalization;
- phone validation;
- email validation;
- status aliases;
- corrupted JSON items;
- empty items;
- negative ambiguous values;
- total recomputation.

---

## 9. Evidence

Execution evidence is available in:

```text
reports/screenshots/
```

The screenshots include:

1. Python Batch router
2. MongoDB raw collection
3. Corrected records and audit trail
4. Quarantine records
5. Unique `order_id` index
6. MongoDB validator
7. First idempotency run
8. Second idempotency run
9. PySpark router
10. Spark Jobs
11. Spark SQL/DataFrame executions
12. Spark Executors
13. Spark Environment
14. Spark Storage
15. Spark Stages
16. Spark completed stages and shuffle evidence
17. PySpark final result
18. Upsert update proof

---

## 10. Conclusion

The execution results demonstrate that the pipeline successfully implements both required processing paths.

Small files are processed using memory-bounded Python Batch ingestion, while larger files are automatically routed to PySpark.

Both paths preserve raw data before transformation, apply data-quality rules, classify every record, quarantine unsafe records, maintain correction evidence, and load accepted records into MongoDB using idempotent business-key-based upsert behavior.

The consistency checks, MongoDB evidence, Spark UI evidence, idempotency rerun, controlled update test, metrics, and automated tests provide verification of the implemented pipeline behavior.