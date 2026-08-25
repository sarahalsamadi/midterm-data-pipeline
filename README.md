# Midterm Data Pipeline

Hybrid order-data processing pipeline using Python Batch, Apache Spark, MongoDB, ELT, Data Quality, Idempotent Upsert, Quarantine handling, and performance metrics.

## Project Goal

The pipeline processes mixed-quality order data while ensuring that no input record is silently lost.

Every raw record is loaded first and is then classified as one of:

- Valid
- Corrected
- Quarantine

The pipeline automatically selects the processing engine according to input file size.

---

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
Deduplication
       |
       v
Idempotent Upsert
       |
       v
reports/results.json
Engine Selection

The default threshold is:

200 MB

Selection logic:

File size <= 200 MB
    -> Python Batch

File size > 200 MB
    -> PySpark

The decision is based on file size, not file name.

This means any CSV file with the expected schema can be processed by the same pipeline.

The router reports:

File size
Configured threshold
Selected engine
Reason for engine selection

Example:

File size: 5120.00 MB
Threshold: 200 MB
Selected engine: pyspark
Reason: File size 5120.00 MB is greater than the 200 MB threshold.
Python Batch Path

Files less than or equal to 200 MB are processed using the Python Batch path.

The loader streams rows instead of loading the complete CSV file into memory.

Default batch size:

5000 rows

For example, a 12,000-row file is processed as:

Batch 1: 5000
Batch 2: 5000
Batch 3: 2000

Each run reports:

Batch number
Rows processed
Batch count
Raw-load time
Raw-load throughput
Total pipeline time
Overall throughput
PySpark Path

Files larger than 200 MB are processed using Apache Spark.

The Spark implementation uses:

SparkSession
DataFrame API
Fixed input schema
String-based raw fields
Parallel partitions
MongoDB Spark Connector
Deterministic deduplication
Distributed MongoDB upsert
Explicit persistence strategy
Configurable shuffle partitions

Pandas is not used for large-file processing.

Spark Storage Strategy

Large DataFrames can exceed the available Java heap even when the source CSV itself fits on disk.

Earlier memory-based caching caused:

java.lang.OutOfMemoryError: Java heap space

Therefore, reusable large Spark DataFrames use:

StorageLevel.DISK_ONLY

instead of relying on memory caching.

Conceptually:

Large DataFrame
      |
      v
DISK_ONLY persistence
      |
      v
Reusable Spark partitions on disk

This reduces Java heap pressure while still preventing unnecessary recomputation of expensive DataFrames.

The trade-off is that disk access is slower than RAM, but it provides greater stability for large local datasets.

Spark Temporary Storage

Spark also requires temporary disk space for operations such as:

Shuffle
Sorting
Spill files
Persisted blocks
Intermediate computation

The default temporary filesystem may have limited space.

For this environment, Spark local storage is configured as:

/mnt/d/spark-temp

The SparkSession configuration includes:

.config(
    "spark.local.dir",
    "/mnt/d/spark-temp",
)

Before running large-file tests, create the directory:

mkdir -p /mnt/d/spark-temp

Check its available space:

df -h /mnt/d/spark-temp

The directory should have sufficient free disk space before processing multi-GB files.

Spark Partitions

Input partitions are determined by Spark according to the input data and execution plan.

For example:

Input partitions: 41

means the input is divided into 41 Spark tasks/partitions for processing.

A progress line such as:

[Stage 22:=====================> (16 + 16) / 41]

means approximately:

16 tasks completed
16 tasks currently running
41 total tasks
Shuffle Partitions

The pipeline configures:

spark.sql.shuffle.partitions = 64

Shuffle is required when Spark must redistribute records between partitions, such as during:

Deduplication
Grouping
Window operations
Aggregation

A stage such as:

[Stage 24: ... / 64]

indicates a shuffle-related stage using 64 partitions.

The project avoids unnecessary repartitioning because shuffle operations involve additional CPU, serialization, memory, and disk I/O.

MongoDB Upsert Parallelism

The final validated dataset is written to MongoDB using distributed upsert.

Current settings:

Upsert partitions: 8
Upsert batch size: 500

The purpose of using 8 write partitions is to provide parallel MongoDB writes without creating excessive concurrent database pressure.

The batch size of 500 reduces network round trips by grouping operations into bulk writes.

Conceptually:

Validated DataFrame
       |
       v
8 write partitions
       |
       v
Bulk batches of 500
       |
       v
MongoDB
ELT Design

The project follows ELT:

Extract
   |
   v
Load Raw
   |
   v
Transform / Validate

Raw input is loaded before transformation.

Raw documents contain fields such as:

run_id
source_file
source_row_number
ingested_at
engine_used
raw_record

This preserves traceability between the original input and transformed output.

MongoDB Collections

The database name is:

midterm_data_pipeline

The project uses three main collections.

orders_raw

Contains input records together with ingestion metadata.

Raw history may grow across executions because each run receives a unique:

run_id
orders_validated

Contains final valid and corrected business records.

A unique index is enforced on:

order_id

MongoDB schema validation is enabled.

The collection represents the latest accepted business state rather than a duplicate copy for every run.

orders_quarantine

Contains invalid or ambiguous records that cannot be corrected safely.

The original raw record and error codes are preserved for investigation.

Data Quality Rules

The pipeline implements deterministic and safe normalization rules including:

Arabic digit normalization
Thousands separator removal
Known Arabic price-word conversion
Currency normalization
Phone normalization
Email normalization
Date normalization
Status alias normalization
Trim normalization
Numeric normalization
Item validation
Safe total recomputation

Corrections are only made when a deterministic result can be produced.

Ambiguous values are not guessed.

They are sent to quarantine.

Example Error Codes

The pipeline reports error codes including:

ID_ORDER_MISSING
ID_CUSTOMER_MISSING
DATE_INVALID_IMPOSSIBLE
PHONE_INVALID
EMAIL_INVALID
PRICE_UNKNOWN
DELIVERY_COST_UNKNOWN
JSON_ITEMS_CORRUPTED
ITEMS_EMPTY
VALUE_NEGATIVE_AMBIGUOUS

A record may contain more than one error code.

Therefore, the sum of individual error-code counts does not necessarily equal the number of quarantine records.

Total Recalculation

When item values and delivery cost are valid, the pipeline can safely recompute:

total_amount =
sum(item totals)
+ delivery_cost

When a mismatch is corrected, the correction is recorded using the appropriate rule information.

Correction Audit Trail

Corrected records preserve correction information including:

Field
Original value
Corrected value
Rule code

This makes transformations auditable instead of silently replacing source values.

Record Classification

Every processed raw record must belong to exactly one main classification:

Valid
Corrected
Quarantine

The pipeline enforces:

raw =
valid
+ corrected
+ quarantine

If this consistency condition fails, the pipeline raises an error.

Example:

Raw:        12171679
Valid:         45335
Corrected:  11443532
Quarantine:   682812

Verification:

45335
+ 11443532
+ 682812
= 12171679

This ensures that input records are not silently lost.

Deduplication

Accepted records may still contain duplicate business keys.

The stable business key is:

order_id

The pipeline performs deterministic deduplication before the final MongoDB upsert.

Metrics include:

duplicate_business_keys
validated_unique_count

Example:

Valid + Corrected:       11,488,867
Duplicate business keys:     80,733
Validated unique:         11,408,134
Idempotency

The validated collection uses upsert instead of insert-only processing.

For a business record:

New order_id
    -> Inserted

Existing order_id with changed data
    -> Updated

Existing order_id with same business state
    -> Unchanged

Running the same dataset again should therefore not create duplicate validated business records.

The pipeline reports:

Inserted
Updated
Unchanged

This provides evidence of idempotent processing.

Metrics

Execution metrics are stored in:

reports/results.json

Each successful run receives a unique:

run_id

Recorded metrics include:

Run ID
File name
File size
Engine used
Rows read
Raw rows loaded
Valid count
Corrected count
Quarantine count
Classified count
Error counts
Processing time
Throughput
Batch size
Batch count
Spark input partitions
MongoDB upsert partitions
Inserted records
Updated records
Unchanged records
Duplicate business keys
Validated unique count
Recorded timestamp
Environment Setup

Enter the project directory:

cd "/mnt/d/4 Year/Big Data/midterm-data-pipeline"

Activate the Python virtual environment:

source ~/midterm-venv/bin/activate

Configure MongoDB when MongoDB is running on the Windows host:

export MONGODB_URI="mongodb://172.29.16.1:27017/"

Verify:

echo $MONGODB_URI

Test MongoDB:

python - <<'PY'
from pymongo import MongoClient

client = MongoClient(
    "mongodb://172.29.16.1:27017/",
    serverSelectionTimeoutMS=5000
)

print(client.admin.command("ping"))
client.close()
PY

Expected:

{'ok': 1.0}
Initialize MongoDB

Initialize collections, validation rules, and indexes using:

python -m src.mongo_setup

The validated collection maintains the unique index:

uq_order_id

on:

order_id
Prepare Spark Local Storage

Before large-file execution:

mkdir -p /mnt/d/spark-temp

Verify capacity:

df -h /mnt/d/spark-temp
Run the Pipeline
Small file
python -m src.main \
  --input "data/orders_test_10.csv"

Expected engine:

python_batch
Medium batch test
python -m src.main \
  --input "data/orders_batch_test.csv"

Expected engine:

python_batch

because the file is below 200 MB.

Spark test
python -m src.main \
  --input "data/orders_spark_test.csv"

Expected engine:

pyspark

when the file exceeds 200 MB.

Multi-GB Spark test
python -m src.main \
  --input "data/orders_large_5gb.csv"

Expected engine:

pyspark

The pipeline can process any test file with the expected CSV structure; the file name itself is not part of the engine-selection logic.

Generate Test Samples

A smaller or larger sample can be generated from the original dataset.

By row count
python -m src.create_small_sample \
  --input "/path/to/orders_huge_mixed_quality.csv" \
  --output "data/orders_test.csv" \
  --rows 10000
By target size

Example 1 GB:

python -m src.create_small_sample \
  --input "/path/to/orders_huge_mixed_quality.csv" \
  --output "data/orders_large_1gb.csv" \
  --size-gb 1

Example 5 GB:

python -m src.create_small_sample \
  --input "/path/to/orders_huge_mixed_quality.csv" \
  --output "data/orders_large_5gb.csv" \
  --size-gb 5

The sample generator streams the source CSV.

Tested Results
Python Batch Test

A 10-row sample produced:

Raw: 10
Valid: 0
Corrected: 8
Quarantine: 2
Consistency check: 10 = 10

Running the same final business state again produced unchanged records rather than duplicate validated records.

12,000-Row Python Batch Test

A 5.01 MB file containing 12,000 rows used:

Engine: python_batch
Batch size: 5000
Batch count: 3

This verifies configurable batch processing.

500,000-Row Spark Test

A sample larger than the 200 MB threshold used PySpark.

Example recorded metrics include:

Raw: 500000
Input partitions: 16
Engine: pyspark

Repeated execution demonstrated idempotent behavior through inserted, updated, and unchanged counts.

1 GB Spark Test

A 1 GB sample was successfully processed using PySpark.

Recorded metrics:

File size: 1024 MB
Rows: 2,439,999
Engine: pyspark

Valid: 8,970
Corrected: 2,294,249
Quarantine: 136,780

Duplicate business keys: 16,079
Validated unique: 2,287,140

Elapsed: 178.42 seconds
Throughput: 13,675.31 rows/second

Input partitions: 16
Upsert partitions: 8

This verifies the large-file Spark route on a multi-million-row dataset.

5 GB Stress Test

A 5 GB sample contains approximately:

12.17 million rows

Testing this dataset exposed two local-resource limitations that were addressed:

Java heap exhaustion
    -> DISK_ONLY persistence

Default temporary storage exhaustion
    -> spark.local.dir redirected to /mnt/d/spark-temp

Only successful completed runs should be recorded as final benchmark results in reports/results.json.

Tests

Run automated tests with:

python -m unittest discover -s tests -v

The test suite covers areas including:

Data-quality rules
Arabic digit normalization
Currency normalization
Date handling
Email handling
Phone handling
Item validation
Negative values
Price-word normalization
Total recomputation
Status aliases
Thousands separators
Consistency
Idempotency logic
Requirements

The project requires:

Python
PySpark
Java
MongoDB
MongoDB Spark Connector
PyMongo

Install Python dependencies using:

pip install -r requirements.txt
Repository Data Policy

Large CSV datasets are intentionally excluded from Git.

The repository stores:

Source code
Configuration
Automated tests
Documentation
Result reports