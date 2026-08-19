# Midterm Data Pipeline Architecture

## Overview

This project implements a hybrid order-data pipeline using:

- Python Batch
- Apache Spark
- MongoDB
- ELT
- Data Quality
- Quarantine
- Idempotent Upsert
- Metrics

## Architecture

```text
CSV Input
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
      Transform / Validate
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

File Routing

The default threshold is 200 MB.

Files less than or equal to 200 MB use Python Batch.
Files greater than 200 MB use PySpark.

The router prints:

file size
threshold
selected engine
reason
Python Batch Path

Small files are processed using Python streaming.

The CSV file is not loaded completely into memory.

Records are inserted into MongoDB using configurable batches.

The batch path reports:

batch number
row count
elapsed time
total batches
throughput
PySpark Path

Large files are processed using PySpark DataFrames.

The implementation uses:

fixed schema
String raw fields
Spark DataFrame API
MongoDB Spark Connector
parallel processing
deterministic deduplication
distributed MongoDB upsert

The final stable MongoDB upsert configuration uses:

8 upsert partitions
500 records per batch
ELT

The project follows ELT.

Raw records are inserted into orders_raw before cleaning and validation.

Raw documents include provenance fields such as:

run_id
source file
source row number
ingested_at
engine_used
raw_record
Data Quality

Implemented safe correction rules include:

Arabic digit normalization
thousands separator removal
Arabic price-word conversion
currency normalization
phone normalization
email normalization
date normalization
status aliases
trimming
numeric normalization
total recomputation

Ambiguous records are not silently dropped.

Audit Trail

Corrected records maintain:

field
original value
corrected value
rule code
Classification

Every input record is classified as:

valid
corrected
quarantine

The pipeline checks:

raw = valid + corrected + quarantine
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
JSON_ITEMS_CORRUPTED
ITEMS_EMPTY
VALUE_NEGATIVE_AMBIGUOUS
MongoDB
orders_raw

Stores raw ingestion history.

orders_validated

Stores final valid and corrected business records.

A unique index is enforced on:

order_id

MongoDB schema validation is enabled with:

validationLevel: strict
validationAction: error
orders_quarantine

Stores rejected records and error information.

Idempotency

order_id is the stable business key.

Final business records use upsert instead of insert-only processing.

The pipeline reports:

inserted
updated
unchanged

Repeated processing of the same business state does not create duplicate validated records.

MongoDB verification showed:

Duplicate order_ids: []
Metrics

Metrics are written to:

reports/results.json

Metrics include:

run ID
file name
file size
engine
rows read
raw rows
valid
corrected
quarantine
elapsed time
throughput
error counts
inserted
updated
unchanged
partitions
duplicate business keys
Demonstrated Large Run

A 1 GB sample containing 2,439,999 rows was successfully processed using PySpark.

Results:

Raw: 2439999
Valid: 8970
Corrected: 2294249
Quarantine: 136780
Duplicate business keys: 16079
Validated unique: 2287140
Inserted: 929519
Updated: 150
Unchanged: 1357471
Input partitions: 16
Upsert partitions: 8
Elapsed seconds: 178.42
Throughput: 13675.31 rows/second

Consistency:

2439999 = 8970 + 2294249 + 136780
Testing

Run:

python -m unittest discover -s tests -v

All implemented unit tests must pass before submission.

Final Design Goals

The pipeline provides:

automatic routing
traceability
no silent data loss
measurable performance
data-quality auditing
MongoDB schema enforcement
unique business keys
idempotent final state
