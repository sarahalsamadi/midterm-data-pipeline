# Midterm Data Pipeline

Hybrid order-data processing pipeline using Python Batch, Apache Spark, MongoDB, ELT, Data Quality, Idempotent Upsert, and Quarantine handling.

## Project Goal

The pipeline processes mixed-quality order data while ensuring that no bad record is silently lost.

Every raw record is first loaded into MongoDB and is then classified as:

- Valid
- Corrected
- Quarantine

The pipeline automatically chooses the processing engine based on file size.

## Architecture

```text
CSV File
   |
   v
File Router
   |
   +----------------------+
   |                      |
   v                      v
Python Batch           PySpark
Small Files            Large Files
   |                      |
   +----------+-----------+
              |
              v
         orders_raw
              |
              v
        ELT / Cleaning
              |
       +------+------+
       |             |
       v             v
orders_validated  orders_quarantine
       |
       v
Idempotent Upsert
       |
       v
reports/results.json

Engine Selection

The default threshold is 200 MB.

Files less than or equal to the threshold use Python Batch.

Files larger than the threshold use PySpark.

The router prints:

File size
Threshold
Selected engine
Python Batch Path

The Python loader streams CSV rows without loading the full file into memory.

Rows are inserted into MongoDB in configurable batches.

Each batch reports:

Batch number
Row count
Time
Throughput
PySpark Path

Large files are processed using Apache Spark DataFrame APIs.

The Spark implementation uses:

Fixed schema
String raw fields
MongoDB Spark Connector
Parallel processing
Deterministic deduplication
Distributed MongoDB upsert

Pandas is not used for large-file processing.

ELT Design

The project follows ELT.

Raw data is loaded first into:

orders_raw

Raw documents contain:

run_id
source_file
source_row_number
ingested_at
engine_used
raw_record

Transformation and validation happen after the raw load.

Data Quality Rules

The pipeline implements safe correction rules including:

Arabic digit normalization
Thousands separator removal
Known Arabic price words
Currency normalization
Phone normalization
Email normalization
Date normalization
Status aliases
Trim normalization
Numeric normalization
Safe total recomputation

Corrections are only applied when the result can be determined safely.

Ambiguous records are placed in quarantine.

Total Recalculation

When item values and delivery cost are valid, the pipeline safely recomputes:

total_amount =
sum(item totals)
+ delivery_cost

A mismatch is recorded using:

TOTAL_RECOMPUTE
Audit Trail

Corrected records include correction information showing:

Field
Original value
Corrected value
Rule code
Quarantine

Invalid or ambiguous records are stored in:

orders_quarantine

Examples of error codes:

ID_ORDER_MISSING
ID_CUSTOMER_MISSING
DATE_INVALID_IMPOSSIBLE
PHONE_INVALID
EMAIL_INVALID
PRICE_UNKNOWN
DELIVERY_COST_UNKNOWN
JSON_ITEMS_CORRUPTED
VALUE_NEGATIVE_AMBIGUOUS

No bad record is silently dropped.

MongoDB Collections
orders_raw

Contains every input record for every pipeline run.

orders_validated

Contains final valid and corrected business records.

A unique index is enforced on:

order_id

Schema validation is enabled.

orders_quarantine

Contains rejected records together with error codes and the original raw record.

Idempotency

order_id is the stable business key.

Validated records use upsert instead of insert-only processing.

Running the same input again should not create duplicate business records.

The pipeline reports:

inserted
updated
unchanged

Raw history may grow because each run receives a new run_id.

Consistency Rule

For every run:

raw =
valid
+ corrected
+ quarantine

The pipeline raises an error if this condition is violated.

Deduplicated validated business records are reported separately using:

validated_unique_count
Metrics

Metrics are stored in:

reports/results.json

They include:

Run ID
File name
File size
Engine used
Rows read
Raw rows loaded
Valid count
Corrected count
Quarantine count
Error counts
Processing time
Throughput
Batch size or Spark partitions
Inserted records
Updated records
Unchanged records
Duplicate business keys
Validated unique count
Run

Activate the environment and configure MongoDB:

source ~/midterm-venv/bin/activate
export MONGODB_URI="mongodb://172.29.16.1:27017/"

Initialize MongoDB:

python -m src.mongo_setup

Run the pipeline:

python -m src.main --input "data/orders_test_10.csv"

For a large file:

python -m src.main --input "data/orders_spark_test.csv"
Generate a Sample

A smaller CSV can be generated from the original large dataset.

Example:

python -m src.create_small_sample \
  --input "/path/to/orders_huge_mixed_quality.csv" \
  --output "data/orders_test.csv" \
  --rows 10000

The generator streams the source CSV and does not use Excel.

Tests

Run:

python -m unittest discover -s tests -v
Example Python Batch Result
Raw: 10
Valid: 0
Corrected: 8
Quarantine: 2
Consistency check: 10 = 10

Running the same final business state again demonstrates idempotency through unchanged records.

Example Spark Test

A 500,000-row sample was processed using PySpark because its size exceeded the 200 MB threshold.

The pipeline reported:

Raw: 500000
Quarantine: 33359
Validated unique: 463407

Spark processing also reports detailed error counts and MongoDB upsert results.

Large Dataset Test

A generated sample containing 6,000,000 rows and approximately 2.5 GB was successfully processed using the Spark path.

This verifies that the large-file route operates using Spark rather than Python full-file processing.

Requirements
Python
Apache Spark / PySpark
Java
MongoDB
MongoDB Spark Connector
PyMongo

Install Python dependencies using:

pip install -r requirements.txt
Repository Data Policy

Large CSV files are intentionally excluded from Git.

Only code, configuration, tests, documentation, and result reports are stored in the repository.