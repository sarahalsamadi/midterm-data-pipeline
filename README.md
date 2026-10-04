# Midterm Data Pipeline

End-to-end Big Data order-processing platform using **Python Batch**, **Apache Spark**, **MongoDB**, **ELT**, **Data Quality**, **Quarantine**, **Idempotent Upsert**, deterministic deduplication, **MongoDB Queries & Indexes**, **Aggregation Pipelines**, **Incremental Materialized Views**, **Scheduled Jobs**, and a unified **FastAPI REST API**.

This repository contains both phases of the project. **Phase 1** implements the scalable ingestion and data-quality pipeline, while **Phase 2** extends the same architecture with operational queries, indexing and performance analysis, analytical reports, incremental materialized views, scheduled processing, and API access.

The project is designed for mixed-quality e-commerce order data and follows one core rule:

> Every input record is loaded to the Raw layer first. No bad record is silently dropped.

The pipeline automatically selects the processing engine according to the input file size and then applies the same business-quality policy regardless of the selected engine.

---

## Table of Contents

1. [Project Goal](#project-goal)
2. [Architecture](#architecture)
3. [Project Structure](#project-structure)
4. [Engine Selection](#engine-selection)
5. [Python Batch Path](#python-batch-path)
6. [PySpark Path](#pyspark-path)
7. [ELT Raw-First Design](#elt-raw-first-design)
8. [Data Quality Rules](#data-quality-rules)
9. [Correction Audit Trail](#correction-audit-trail)
10. [Quarantine](#quarantine)
11. [MongoDB Design](#mongodb-design)
12. [Deduplication](#deduplication)
13. [Idempotency and Upsert](#idempotency-and-upsert)
14. [Metrics](#metrics)
15. [Environment Setup](#environment-setup)
16. [Run Commands](#run-commands)
17. [Generate Test Samples](#generate-test-samples)
18. [Automated Tests](#automated-tests)
19. [Recorded Results](#recorded-results)
20. [Execution Evidence](#execution-evidence)
21. [Batch vs PySpark Comparison](#batch-vs-pyspark-comparison)
22. [Data Integrity Policy](#data-integrity-policy)
23. [Repository Data Policy](#repository-data-policy)
24. [Phase 2 — Analytics and API Layer](#phase-2--analytics-and-api-layer)
25. [Phase 2 Practical Queries](#phase-2-practical-queries)
26. [Phase 2 Indexes](#phase-2-indexes)
27. [Explain Analysis — Before and After Indexes](#explain-analysis--before-and-after-indexes)
28. [Aggregation Reports](#aggregation-reports)
29. [Materialized Views](#materialized-views)
30. [Scheduled Jobs](#scheduled-jobs)
31. [Unified FastAPI](#unified-fastapi)
32. [Phase 2 Environment and Dependencies](#phase-2-environment-and-dependencies)
33. [Phase 2 Quick Evaluation Sequence](#phase-2-quick-evaluation-sequence)
34. [Phase 2 Validation](#phase-2-validation)
35. [Phase 2 Evidence Map](#phase-2-evidence-map)

---

## Project Goal

The project implements a reusable hybrid data pipeline for large mixed-quality order datasets.

The pipeline must:

- process small files using memory-safe Python batch loading;
- process large files using Apache Spark;
- load every source row into MongoDB Raw before cleaning;
- distinguish safe corrections from unsafe or ambiguous data;
- preserve a correction audit trail;
- quarantine records that cannot be corrected safely;
- prevent duplicate final business records;
- support safe reruns through idempotent upsert;
- record execution metrics and error counts;
- provide practical evidence using MongoDB Compass, Spark UI, and recorded results.

Every Raw record must end in exactly one classification:

```text
Valid
Corrected
Quarantine
```

The pipeline enforces the consistency rule:

```text
Raw = Valid + Corrected + Quarantine
```

---

## Architecture

```text
                               CSV File
                                  |
                                  v
                             File Router
                                  |
                    +-------------+-------------+
                    |                           |
                    v                           v
              Python Batch                   PySpark
               <= 200 MB                     > 200 MB
                    |                           |
                    +-------------+-------------+
                                  |
                                  v
                             orders_raw
                                  |
                                  v
                      Quality / Transformation
                                  |
                     +------------+------------+
                     |                         |
                     v                         v
              Valid / Corrected            Quarantine
                     |                         |
                     |                         v
                     |                 orders_quarantine
                     |
                     v
             Deterministic Deduplication
                     |
                     v
              Idempotent Upsert
                     |
                     v
              orders_validated
                     |
                     v
             reports/results.json
```

The architecture deliberately separates routing, loading, quality transformation, MongoDB setup, upsert, metrics, and testing.

---

## Project Structure

```text
midterm-data-pipeline/
|
|-- README.md
|-- requirements.txt
|-- .env.example
|-- .gitignore
|
|-- config/
|   `-- settings.py
|
|-- data/
|   `-- .gitkeep
|
|-- src/
|   |-- __init__.py
|   |-- main.py
|   |-- file_router.py
|   |-- create_small_sample.py
|   |-- batch_loader.py
|   |-- spark_loader.py
|   |-- spark_transform.py
|   |-- spark_upsert.py
|   |-- quality_rules.py
|   |-- elt_pipeline.py
|   |-- mongo_setup.py
|   |-- metrics.py
|   |-- queries.py
|   |-- indexes.py
|   |-- aggregations.py
|   |-- materialized_views.py
|   |-- jobs.py
|   |-- scheduler.py
|   `-- api.py
|
|-- tests/
|   |-- test_consistency.py
|   |-- test_file_router.py
|   |-- test_idempotency_logic.py
|   `-- test_quality_rules.py
|
|-- reports/
|   |-- results.json
|   |-- results.md
|   |-- explain_results.json
|   `-- screenshots/
|
`-- docs/
    `-- architecture.md
```

### Main responsibility of each source file

| File | Responsibility |
|---|---|
| `src/main.py` | Single pipeline entry point and overall orchestration |
| `src/file_router.py` | Selects Python Batch or PySpark according to file size |
| `src/create_small_sample.py` | Generates controlled test samples without manual editing |
| `src/batch_loader.py` | Streaming CSV to Raw MongoDB in configurable batches |
| `src/spark_loader.py` | Large-file Spark orchestration, Raw load, counts and metrics |
| `src/spark_transform.py` | Distributed DataFrame-based quality transformations |
| `src/spark_upsert.py` | Distributed idempotent MongoDB upsert |
| `src/quality_rules.py` | Deterministic Python quality and normalization rules |
| `src/elt_pipeline.py` | Python Raw to quality classification to validated/quarantine |
| `src/mongo_setup.py` | MongoDB collections, validator, indexes and connection checks |
| `src/metrics.py` | Appends successful run metrics to `reports/results.json` |
| `src/queries.py` | Five practical MongoDB queries |
| `src/indexes.py` | Phase 2 indexes and Explain analysis |
| `src/aggregations.py` | Five analytical aggregation reports |
| `src/materialized_views.py` | Incremental materialized-view refresh |
| `src/jobs.py` | Job definitions, execution, and persistent logging |
| `src/scheduler.py` | Automatic scheduled execution |
| `src/api.py` | Unified FastAPI interface |

---

## Engine Selection

The default threshold is:

```text
200 MB
```

Selection logic:

```text
File size <= 200 MB
    -> python_batch

File size > 200 MB
    -> pyspark
```

The decision is based on **file size**, not on the file name. The router prints file size, configured threshold, selected engine, and the reason for selection.

### Python Batch router evidence

![Python Batch Router](reports/screenshots/01-python-batch-router.png)

### PySpark router evidence

![PySpark Router](reports/screenshots/09-spark-router.png)

The Spark demonstration file was `orders_spark_test.csv` with size `209.20 MB`, therefore `209.20 MB > 200 MB` and the router selected `pyspark`.

---

## Python Batch Path

Files less than or equal to 200 MB are processed using the Python Batch path.

The loader uses `csv.DictReader` and does **not** load the full CSV into a Python list before Raw ingestion.

The default configured batch size is:

```text
5000 rows
```

A 12,000-row test is processed as:

```text
Batch 1: 5000
Batch 2: 5000
Batch 3: 2000
```

For each batch the implementation records the batch number, row count, elapsed batch-write time, and **per-batch throughput**. If `insert_many` fails, the loader prints the batch number, number of affected rows, elapsed time, exception type, and original failure reason, then re-raises the exception so the failure is never hidden. At run level it records loaded Raw rows, total batch count, Raw-load time, Raw-load throughput, total pipeline time, and overall pipeline throughput.

Example controlled output:

```text
Batch 1: 10 rows, 0.00 seconds
Loaded raw rows: 10
Total batches: 1
Elapsed seconds: 0.01
Throughput: 859.88 rows/second
```

The Python path loads Raw first, then the ELT pipeline processes only the records associated with the generated `run_id`.

---

## PySpark Path

Files larger than 200 MB are processed using Apache Spark.

The Spark implementation uses SparkSession, Spark DataFrame API, fixed input schema, String-based Raw fields, Spark partitions, MongoDB Spark Connector, DataFrame-based transformations, deterministic business-key deduplication, distributed MongoDB upsert, explicit persistence, and configurable SQL shuffle partitions.

**Pandas is not used for large-file processing.**

### Fixed Raw Schema

Raw fields are read using a fixed `StructType` schema. Sensitive dirty fields are kept as strings during Raw ingestion so malformed values are not destroyed by premature type coercion.

### Spark Session Configuration

```text
master = local[*]
spark.driver.memory = 6g
spark.executor.memory = 6g
spark.sql.shuffle.partitions = 64
spark.local.dir = /mnt/d/spark-temp
```

### Spark Input Partitions

A recorded 209.20 MB execution used `16` input partitions. A successful 5 GB execution used `41` input partitions.

### Shuffle Partitions

The pipeline configures `spark.sql.shuffle.partitions = 64`. Shuffle occurs during grouping, Window-based deduplication, sorting by business key, and explicit repartitioning before MongoDB upsert.

### Repartition Before MongoDB Upsert

The final validated Spark DataFrame is repartitioned by `order_id` into `8 partitions` before distributed MongoDB upsert. This controls concurrent MongoDB writers.

### Distributed MongoDB Upsert

```text
Upsert partitions: 8
Upsert batch size: 500
```

```text
Validated DataFrame
        |
        v
repartition(8, order_id)
        |
        v
mapPartitions
        |
        v
Bulk batches of 500
        |
        v
MongoDB orders_validated
```

---

## Spark Storage Strategy

Large reusable Spark DataFrames use `StorageLevel.DISK_ONLY`. Spark temporary storage is configured as `/mnt/d/spark-temp`.

```bash
mkdir -p /mnt/d/spark-temp
df -h /mnt/d/spark-temp
```

---

## ELT Raw-First Design

```text
Extract
   |
   v
Load Raw
   |
   v
Transform / Validate
```

Each Raw document preserves `run_id`, `source_file`, `source_row_number`, `ingested_at`, `engine_used`, and `raw_record`.

![MongoDB Raw](reports/screenshots/02-mongodb-raw.png)

---

## Data Quality Rules

The project implements more than the required minimum of eight deterministic correction rules.

| Rule | Purpose |
|---|---|
| Arabic digit normalization | Converts known Arabic digits to Latin digits |
| Thousands separator removal | Normalizes numeric text such as `125,000` |
| Known Arabic price-word conversion | Converts only explicitly supported known price words |
| Currency normalization | Standardizes known currency names/symbols |
| Phone normalization | Removes formatting and normalizes safe phone forms |
| Email normalization | Repairs clearly repeated symbols when deterministic |
| Date normalization | Parses supported valid date forms into a standard representation |
| Status alias normalization | Maps known status aliases to standard values |
| Trim normalization | Removes leading/trailing whitespace |
| Numeric normalization | Converts clean numeric values after safe normalization |
| Item validation | Parses and validates `items_json` |
| Total recomputation | Recalculates total when components are valid |

The pipeline never guesses ambiguous values. Unsafe records are quarantined.

---

## Correction Audit Trail

Every corrected final record preserves `field`, `original_value`, `corrected_value`, and `rule_code`.

![MongoDB Corrected](reports/screenshots/03-mongodb-corrected.png)

---

## Quarantine

Records that cannot be corrected safely are written to `orders_quarantine`. Each quarantine document preserves `run_id`, `order_id` when available, `error_codes`, `error_details`, and `raw_record`.

![MongoDB Quarantine](reports/screenshots/04-mongodb-quarantine.png)

Example codes include `ID_ORDER_MISSING`, `ID_CUSTOMER_MISSING`, `DATE_INVALID_IMPOSSIBLE`, `PHONE_INVALID`, `EMAIL_INVALID`, `PRICE_UNKNOWN`, `JSON_ITEMS_CORRUPTED`, `ITEMS_EMPTY`, and `VALUE_NEGATIVE_AMBIGUOUS`.

---

## Record Classification

The pipeline verifies:

```text
raw_count = valid_count + corrected_count + quarantine_count
```

### 500,000-row Spark run

```text
1,872 + 470,254 + 27,874 = 500,000
```

### 5 GB successful run

```text
45,335 + 11,443,532 + 682,812 = 12,171,679
```

---

## MongoDB Design

Database: `midterm_data_pipeline`

Collections:

```text
orders_raw
orders_validated
orders_quarantine
```

`orders_raw` keeps all input history without a business-key unique index that could reject dirty or duplicate Raw values.

`orders_validated` stores final accepted business records and uses the unique index `uq_order_id` on `order_id`.

![Validated Unique Index](reports/screenshots/05-mongodb-validated-index.png)

MongoDB schema validation is configured on `orders_validated`.

![Validated Schema Validator](reports/screenshots/06-mongodb-validator.png)

---

## Deduplication

The stable business key is `order_id`. Spark performs deterministic deduplication before the final upsert.

### 500,000-row example

```text
1,872 + 470,254 = 472,126 accepted before dedup
472,126 - 3,278 = 468,848 validated unique
```

### 5 GB example

```text
45,335 + 11,443,532 = 11,488,867 accepted before dedup
11,488,867 - 80,733 = 11,408,134 validated unique
```

---

## Idempotency and Upsert

```text
New order_id -> Inserted
Existing order_id + changed business data -> Updated
Existing order_id + same business state -> Unchanged
```

### First Run

```text
Inserted: 8
Updated: 0
Unchanged: 0
```

![Idempotency First Run](reports/screenshots/07-idempotency-first-run.png)

### Exact Same Input Again

```text
Inserted: 0
Updated: 0
Unchanged: 8
```

![Idempotency Second Run](reports/screenshots/08-idempotency-second-run.png)

### Controlled Update of One Existing Record

Target:

```text
order_id = طلب-100001
customer_name: علي حسين -> علي حسين UPDATED
```

Result:

```text
Inserted: 0
Updated: 1
Unchanged: 7
Validated records: 8
```

![Upsert Update Proof](reports/screenshots/18-upsert-update-proof.png)

---

## Metrics

Successful executions are appended to `reports/results.json`. Metrics include run ID, file name/size, engine, Raw/Valid/Corrected/Quarantine counts, elapsed time, throughput, partitions or batch settings, error counts, and Inserted/Updated/Unchanged counts.

---

## Environment Setup

```bash
cd "/mnt/d/4 Year/Big Data/midterm-data-pipeline"
source ~/midterm-venv/bin/activate
pip install -r requirements.txt
```

Find current WSL gateway:

```bash
ip route | grep default
```

Then:

```bash
export MONGODB_URI="mongodb://<CURRENT_GATEWAY_IP>:27017/"
```

Test MongoDB:

```bash
python - <<'PY_CHECK_MONGO'
import os
from pymongo import MongoClient
client = MongoClient(os.environ["MONGODB_URI"], serverSelectionTimeoutMS=5000)
print(client.admin.command("ping"))
client.close()
PY_CHECK_MONGO
```

Initialize MongoDB:

```bash
python -m src.mongo_setup
```

Prepare Spark storage:

```bash
mkdir -p /mnt/d/spark-temp
df -h /mnt/d/spark-temp
```

---

## Run Commands

```bash
python -m src.main --input "data/orders_test_10.csv"
python -m src.main --input "data/orders_batch_test.csv"
python -m src.main --input "data/orders_spark_test.csv"
python -m src.main --input "data/orders_large_1gb.csv"
python -m src.main --input "data/orders_large_5gb.csv"
```

---

## Generate Test Samples

```bash
python -m src.create_small_sample --input "/path/to/orders_huge_mixed_quality.csv" --output "data/orders_test.csv" --rows 10000
python -m src.create_small_sample --input "/path/to/orders_huge_mixed_quality.csv" --output "data/orders_large_1gb.csv" --size-gb 1
python -m src.create_small_sample --input "/path/to/orders_huge_mixed_quality.csv" --output "data/orders_large_5gb.csv" --size-gb 5
```

---

## Automated Tests

The repository contains automated tests for the core required behaviors:

```text
tests/test_quality_rules.py
tests/test_consistency.py
tests/test_file_router.py
tests/test_idempotency_logic.py
```

Run:

```bash
python -m unittest discover -s tests -v
```

Latest verified result:

```text
Ran 21 tests in 0.009s

OK
```

The 21 passing tests cover:

- Router boundary behavior, including 5 MB, 200 MB and 201 MB cases;
- Raw/classified consistency;
- same-record idempotency;
- changed-record update detection;
- Arabic digit normalization;
- thousands separator normalization;
- price-word normalization;
- currency normalization;
- date normalization;
- phone normalization;
- email normalization;
- status aliases;
- `items_json` validation;
- negative/ambiguous item handling;
- total recomputation.


## Recorded Results

### Python Batch — 12,000 Rows

```text
File size: 5.01 MB
Rows: 12,000
Engine: python_batch
Batch size: 5,000
Batch count: 3
```

### PySpark — 500,000 Rows

```text
File size: 209.20 MB
Raw: 500,000
Valid: 1,872
Corrected: 470,254
Quarantine: 27,874
Duplicate business rows removed: 3,278
Validated unique: 468,848
Inserted: 468,848
Updated: 0
Unchanged: 0
Input partitions: 16
Upsert partitions: 8
```

![Spark Final Result](reports/screenshots/17-spark-final-result.png)

### PySpark — 1 GB Successful Benchmark

```text
File size: 1,024 MB
Rows: 2,439,999
Valid: 8,970
Corrected: 2,294,249
Quarantine: 136,780
Duplicate business rows removed: 16,079
Validated unique: 2,287,140
Elapsed: 178.42 seconds
Throughput: 13,675.31 rows/second
Input partitions: 16
Upsert partitions: 8
```

### PySpark — 5 GB Successful Large-Scale Benchmark

Run ID:

```text
aed8bab5-0dc7-410c-9a35-fe8b5e08cc28
```

```text
File: orders_large_5gb.csv
File size: 5,120 MB
Rows read: 12,171,679
Engine: pyspark
Raw: 12,171,679
Valid: 45,335
Corrected: 11,443,532
Quarantine: 682,812
Duplicate business rows removed: 80,733
Validated unique: 11,408,134
Elapsed time: 1,588.8493 seconds (~26m 29s)
Throughput: 7,660.69 rows/second
Input partitions: 41
Upsert partitions: 8
```

Error counts:

```text
JSON_ITEMS_CORRUPTED:       170,398
ID_CUSTOMER_MISSING:        170,595
DATE_INVALID_IMPOSSIBLE:     85,536
ID_ORDER_MISSING:            85,333
PRICE_UNKNOWN:               85,002
VALUE_NEGATIVE_AMBIGUOUS:    85,289
EMAIL_INVALID:              170,146
PHONE_INVALID:               85,519
```

---

## Execution Evidence

### Spark Router

![Spark Router](reports/screenshots/09-spark-router.png)

### Spark Jobs

![Spark Jobs](reports/screenshots/10-spark-ui-jobs.png)

### Spark SQL / DataFrame Executions

![Spark SQL](reports/screenshots/11-spark-ui-sql-executions.png)

### Spark Executors

![Spark Executors](reports/screenshots/12-spark-ui-executors.png)

Recorded UI values include:

```text
Cores: 16
Active Tasks: 8
Complete Tasks: 391
Total Tasks: 399
Input: 884.5 MiB
Shuffle Read: 141.1 MiB
Shuffle Write: 248.4 MiB
```

### Spark Environment

![Spark Environment](reports/screenshots/13-spark-ui-environment.png)

### Spark Storage

![Spark Storage](reports/screenshots/14-spark-ui-storage.png)

### Spark Stages

![Spark Stages](reports/screenshots/15-spark-ui-stages.png)

The UI shows 8-task, 16-task and 64-task stages and real Shuffle Read/Write activity.

### Completed Spark Stages

![Completed Spark Stages](reports/screenshots/16-spark-ui-completed-stages.png)

---

## Batch vs PySpark Comparison

| Metric | Python Batch | PySpark Live Test | PySpark Large Benchmark |
|---|---:|---:|---:|
| Input file | `orders_batch_test.csv` | `orders_spark_test.csv` | `orders_large_5gb.csv` |
| File size | 5.01 MB | 209.20 MB | 5,120 MB |
| Rows | 12,000 | 500,000 | 12,171,679 |
| Selected engine | `python_batch` | `pyspark` | `pyspark` |
| Batch size | 5,000 | N/A | N/A |
| Batch count | 3 | N/A | N/A |
| Raw-load throughput | 29,412.80 rows/s | N/A | N/A |
| Overall pipeline throughput | 1,014.92 rows/s | recorded in run metrics | 7,660.69 rows/s |
| Input partitions | N/A | 16 | 41 |
| Upsert partitions | N/A | 8 | 8 |
| Execution model | Streaming batches | Spark DataFrame partitions | Spark DataFrame partitions |
| Raw-first ELT | Yes | Yes | Yes |
| Idempotent final upsert | Yes | Yes | Yes |

---

## Data Integrity Policy

The original source dataset is not manually modified to improve results. Controlled tests, including the one-record update test, are generated programmatically. No invalid source rows are removed before Raw loading.

---

## Repository Data Policy

Large generated CSV files are intentionally not required to be stored in Git. The repository contains source code, configuration, tests, documentation, reports and screenshots. Large test samples can be regenerated with `src/create_small_sample.py`.

---

## Practical Demo Sequence

```text
1. Run orders_test_10.csv -> python_batch
2. Show orders_raw before cleaning
3. Show corrected audit trail
4. Show quarantine reasons
5. Show unique index and validator
6. Rerun same 10-row input -> Unchanged 8
7. Run controlled update -> Updated 1, validated remains 8
8. Run orders_spark_test.csv -> pyspark
9. Show Spark Jobs / Stages / Tasks / Partitions / Shuffle
10. Show final Spark result
11. Show reports/results.json
12. Explain successful 5 GB run
```

---

# Phase 2 — Analytics and API Layer

Phase 2 extends the existing Midterm Data Pipeline in the **same repository**. The original ingestion architecture remains unchanged, while the project is extended with practical MongoDB queries, indexes and execution-plan analysis, aggregation reports, incremental materialized views, scheduled jobs, and a unified FastAPI interface.

The FastAPI `/ingest` endpoint reuses the existing `run_pipeline()` function from `src/main.py`. Therefore, API ingestion follows the same file-size router, Python Batch/PySpark processing paths, Raw-first ELT, data-quality rules, quarantine, deduplication, and idempotent upsert logic used by the original project.

### Phase 2 Components

| Component | Implementation |
|---|---|
| Practical Queries | `src/queries.py` |
| Indexes & Explain Analysis | `src/indexes.py` |
| Aggregation Reports | `src/aggregations.py` |
| Materialized Views | `src/materialized_views.py` |
| Scheduled Jobs | `src/jobs.py` |
| Automatic Scheduler | `src/scheduler.py` |
| Unified REST API | `src/api.py` |
| Existing Ingestion Entry Point | `src/main.py` |
| Explain Results | `reports/explain_results.json` |
| Environment Example | `.env.example` |

---

## Phase 2 Practical Queries

Five practical MongoDB queries are implemented in `src/queries.py` and operate on actual documents from `orders_validated`.

| Query | Purpose |
|---|---|
| `orders_by_city` | Retrieve orders for a selected city |
| `orders_by_status` | Retrieve orders with a selected status |
| `customer_orders` | Retrieve the order history of a customer |
| `orders_by_date_range` | Retrieve orders within a date range |
| `high_value_paid_orders` | Retrieve paid orders above a configurable value |

The queries accept runtime parameters and are also exposed through the API:

```text
GET /queries
GET /queries/{name}
```

Example:

```bash
curl -sG \
  --data-urlencode "city=إب" \
  --data-urlencode "limit=3" \
  http://localhost:8000/queries/orders_by_city
```

---

## Phase 2 Indexes

Three additional indexes are implemented for the Phase 2 query workload:

| Index | Definition | Purpose |
|---|---|---|
| `idx_city` | `{city: 1}` | Accelerates filtering by city |
| `idx_customer_id` | `{customer_id: 1}` | Accelerates customer-order lookup |
| `idx_payment_status_total` | `{payment_status: 1, total_amount: -1}` | Compound index for paid high-value orders |

The original unique business-key index remains in place:

```text
uq_order_id -> {order_id: 1}, unique
```

The compound index places `payment_status` first because it is used as an equality predicate and `total_amount` second to support the amount range and descending access pattern.

### Index Creation Evidence

![Phase 2 Indexes](reports/screenshots/22-phase2-indexes-created.png)

---

## Explain Analysis — Before and After Indexes

MongoDB `explain("executionStats")` was recorded for three representative queries before and after creating the Phase 2 indexes. The complete machine-readable results are stored in `reports/explain_results.json`.

| Query | Before Plan | Docs Examined Before | Time Before | After Plan | Docs Examined After | Keys Examined After | Time After |
|---|---|---:|---:|---|---:|---:|---:|
| `orders_by_city` | `COLLSCAN` | 10,990 | 6 ms | `IXSCAN` + `FETCH` | 1,154 | 1,154 | 2 ms |
| `customer_orders` | `COLLSCAN` | 10,990 | 8 ms | `IXSCAN` + `FETCH` | 1 | 1 | 1 ms |
| `high_value_paid_orders` | `COLLSCAN` + `SORT` | 10,990 | 13 ms | `IXSCAN` + `FETCH` | 2,172 | 2,172 | 4 ms |

The results show that the indexes substantially reduce unnecessary document examination. `customer_orders`, for example, drops from scanning 10,990 documents to examining one matching document. The compound index also replaces the previous collection-scan-plus-sort path for high-value paid orders.

### Before Indexes

![City Before Index](reports/screenshots/19-explain-city-before-index.png)

![Customer Before Index](reports/screenshots/20-explain-customer-value-before-index.png)

![High Value Before Index](reports/screenshots/21-explain-high-value-before-index.png)

### After Indexes

![City After Index](reports/screenshots/23-explain-city-after-index.jpg)

![Customer After Index](reports/screenshots/24-explain-customer-after-index.jpg)

![High Value After Index](reports/screenshots/25-explain-high-value-after-index.png)

---

## Aggregation Reports

Five independently executable MongoDB aggregation reports are implemented in `src/aggregations.py`.

| Aggregation | Purpose |
|---|---|
| `sales_by_city` | Analyze order count and sales by city |
| `top_products` | Analyze product sales and quantities |
| `top_customers` | Identify highest-value customers |
| `sales_by_period` | Analyze sales activity over time |
| `orders_by_status` | Analyze the distribution of order statuses |

They are exposed through:

```text
GET /aggregations
GET /aggregations/{name}
```

Example:

```bash
curl -s "http://localhost:8000/aggregations/sales_by_city?limit=5" | python -m json.tool
```

### Aggregation Evidence

![Sales and Products Aggregations](reports/screenshots/26-aggregation-sales-products.jpg)

![Customers and Period Aggregations](reports/screenshots/27-aggregation-customers-period.jpg)

![Order Status Aggregation](reports/screenshots/28-aggregation-order-status.jpg)

---

## Materialized Views

Two MongoDB materialized summaries are maintained by `src/materialized_views.py`:

| Materialized View | Purpose |
|---|---|
| `daily_sales_summary` | Daily order and sales summary |
| `top_products_summary` | Product-level sales summary |

Refresh metadata is stored in `mv_refresh_metadata`.

The first execution builds the initial summaries. Later refreshes use the existing pipeline `run_id` mechanism to identify changed validated records and update only affected aggregation groups rather than rebuilding all source data.

### Initial Refresh

![Materialized Views Initial Refresh](reports/screenshots/29-materialized-views-initial-refresh.jpg)

### Incremental Refresh with No New Changes

When no new source run requires processing, the refresh remains incremental and performs no unnecessary group rewrites.

![Materialized Views Incremental No Change](reports/screenshots/30-materialized-views-incremental-no-change.jpg)

### Real Incremental Update

A real existing-order update was passed through the original ingestion pipeline and then detected by the materialized-view refresh.

![Pipeline Real Update for Incremental MV](reports/screenshots/31-pipeline-real-update-for-incremental-mv.jpg)

The subsequent refresh processed the changed source record without a full rebuild.

![Materialized Views Real Incremental Refresh](reports/screenshots/32-materialized-views-real-incremental-refresh.jpg)

### Correct Handling of Old and New Groups

An update may move an order from one date or product group to another. Updating only the new group would leave the old group stale. To prevent this, the Python and Spark upsert paths preserve `previous_mv_keys` only when an existing business record actually changes. These keys contain the previous date and product SKUs.

A controlled test changed one existing order from one date to another and from one SKU to another:

![Pipeline MV Group Change](reports/screenshots/33-pipeline-mv-group-change.jpg)

The incremental refresh recalculated both old and new affected groups. The recorded execution showed one changed document, two affected groups, two output documents written, and `full_rebuild = false`.

![MV Old and New Groups Incremental](reports/screenshots/34-mv-old-new-groups-incremental.jpg)

Refresh through the API:

```bash
curl -s -X POST \
  -H "Content-Type: application/json" \
  -d '{}' \
  http://localhost:8000/refresh-mv | python -m json.tool
```

---

## Scheduled Jobs

Two operational jobs are defined in `src/jobs.py`:

| Job | Schedule | Purpose |
|---|---|---|
| `refresh_materialized_views` | Every 30 minutes | Incrementally refresh materialized summaries |
| `generate_reports` | Every day at 01:00 | Generate and persist aggregation report snapshots |

Each execution is recorded in the MongoDB `job_runs` collection with the job name, trigger type, start time, end time, duration, status, result, and error information. Aggregation snapshots are persisted in `report_snapshots`.

### Manual Job Execution and Logs

![Scheduled Jobs Manual Runs and Logs](reports/screenshots/35-scheduled-jobs-manual-runs-and-logs.jpg)

Jobs can also be executed through FastAPI:

```bash
curl -s -X POST http://localhost:8000/jobs/generate_reports/run | python -m json.tool
```

### Automatic Scheduler

Run the scheduler with:

```bash
python -m src.scheduler
```

The scheduler invokes the same `run_job()` implementation and records automatic executions with `trigger = scheduled`.

![Scheduled Jobs Automatic Trigger Log](reports/screenshots/36-scheduled-jobs-automatic-trigger-log.jpg)

---

## Unified FastAPI

Phase 2 exposes the project through one FastAPI application in `src/api.py`.

Start it with:

```bash
python -m uvicorn src.api:app --host 0.0.0.0 --port 8000
```

Swagger UI:

```text
http://localhost:8000/docs
```

### API Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/health` | API health check |
| `POST` | `/ingest` | Execute the existing Phase 1 ingestion pipeline |
| `POST` | `/indexes` | Create/verify Phase 2 indexes |
| `GET` | `/queries` | List available practical queries |
| `GET` | `/queries/{name}` | Execute a named query |
| `GET` | `/aggregations` | List aggregation reports |
| `GET` | `/aggregations/{name}` | Execute a named aggregation |
| `POST` | `/refresh-mv` | Refresh materialized views |
| `GET` | `/jobs` | List job definitions and recent executions |
| `POST` | `/jobs/{name}/run` | Manually execute a scheduled job |

### Swagger Evidence

![FastAPI Swagger Docs](reports/screenshots/37-fastapi-swagger-docs.jpg)

### Job Execution through FastAPI

![FastAPI Job Execution](reports/screenshots/38-fastapi-job-execution.jpg)

### `/ingest` Reuses the Existing Pipeline

`POST /ingest` calls `run_pipeline(input_file)` from `src/main.py`. The CLI calls the same function, so there is no duplicate ingestion implementation.

```text
CLI --------------------+
                        |
                        v
                 run_pipeline()
                        ^
                        |
FastAPI /ingest --------+
```

Example:

```bash
curl -s -X POST \
  -H "Content-Type: application/json" \
  -d '{"input_file":"data/orders_test_10_update.csv"}' \
  http://localhost:8000/ingest | python -m json.tool
```

The recorded idempotent API run processed 10 Raw rows, classified 8 as corrected and 2 as quarantine, produced 0 inserts, 0 updates and 8 unchanged accepted records, and passed the consistency check.

![FastAPI Ingest Existing Pipeline](reports/screenshots/39-fastapi-ingest-existing-pipeline.jpg)

---

## Phase 2 Environment and Dependencies

The final `requirements.txt` includes:

```text
pymongo==4.16.0
pyspark==4.2.0
fastapi==0.142.2
uvicorn==0.54.0
```

An `.env.example` file documents `MONGODB_URI` without storing secrets.

For MongoDB on the same machine:

```bash
export MONGODB_URI="mongodb://localhost:27017/"
```

For WSL connecting to MongoDB running as a Windows service:

```bash
export MONGODB_URI="mongodb://$(ip route | awk '/default/ {print $3}'):27017/"
```

The application uses the `midterm_data_pipeline` database.

---

## Phase 2 Quick Evaluation Sequence

```bash
# 1. Activate the environment and install dependencies
source ~/midterm-venv/bin/activate
pip install -r requirements.txt

# 2. Configure MongoDB when using WSL + Windows MongoDB
export MONGODB_URI="mongodb://$(ip route | awk '/default/ {print $3}'):27017/"

# 3. Initialize MongoDB
python -m src.mongo_setup

# 4. Run the existing CLI ingestion path
python -m src.main --input "data/orders_test_10.csv"

# 5. Start FastAPI
python -m uvicorn src.api:app --host 0.0.0.0 --port 8000
```

Then open `http://localhost:8000/docs` and test indexes, queries, aggregations, materialized-view refresh, jobs, and ingestion from Swagger or the documented endpoints.

For automatic scheduled execution, open another terminal with the same `MONGODB_URI` and run:

```bash
python -m src.scheduler
```

---

## Phase 2 Validation

Compile the modified and new modules:

```bash
python -m py_compile \
  src/main.py \
  src/api.py \
  src/queries.py \
  src/indexes.py \
  src/aggregations.py \
  src/materialized_views.py \
  src/jobs.py \
  src/scheduler.py \
  src/elt_pipeline.py \
  src/spark_upsert.py
```

Run the regression suite:

```bash
python -m unittest discover -s tests -v
```

Latest verified result:

```text
Ran 21 tests

OK
```

This confirms that the Phase 2 extensions preserve the tested Phase 1 router, consistency, quality-rule, and idempotency behavior.

---

## Phase 2 Evidence Map

| Requirement | Implementation | Evidence |
|---|---|---|
| 5 practical queries | `src/queries.py` | Query/API executions |
| 3 indexes | `src/indexes.py` | Screenshot 22 |
| Compound index | `idx_payment_status_total` | Screenshot 22 + Explain |
| 3 Explain before/after comparisons | `reports/explain_results.json` | Screenshots 19–25 |
| 5 aggregation reports | `src/aggregations.py` | Screenshots 26–28 |
| 2 materialized views | `src/materialized_views.py` | Screenshots 29–34 |
| Incremental MV refresh | `run_id` + affected groups | Screenshots 30–34 |
| 2 scheduled jobs | `src/jobs.py` | Screenshot 35 |
| Automatic scheduling | `src/scheduler.py` | Screenshot 36 |
| Persistent job logging | `job_runs` | Screenshots 35–36 and 38 |
| Unified FastAPI | `src/api.py` | Screenshot 37 |
| Existing pipeline via `/ingest` | `run_pipeline()` | Screenshot 39 |
| Environment example | `.env.example` | Repository |
| Dependencies | `requirements.txt` | Repository |

---

## Summary

The project demonstrates a complete two-phase Big Data architecture: automatic Python Batch/PySpark routing, Raw-first ELT, deterministic data-quality correction, quarantine, audit trail, deduplication, idempotent upsert, execution metrics, MongoDB schema validation, practical indexed queries, measured execution-plan optimization, five analytical aggregation reports, two incrementally maintained materialized views, scheduled operational jobs with persistent execution logs, and a unified FastAPI interface that reuses the original ingestion pipeline. The regression suite remains at 21 passing tests, and the recorded evidence includes successful 500K, 1 GB and 5 GB Spark processing together with the complete Phase 2 execution evidence.
