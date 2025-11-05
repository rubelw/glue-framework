import sys
from pyspark.sql import SparkSession, functions as F

# ---- shared library imports ----
# These utilities live under lib/ and provide consistent, testable logic across all Glue jobs.
from lib.args import parse_args           # Parses CLI-style arguments: --ENV, --CONFIG_S3_URI, --BOOKMARKED
from lib.config import load_config        # Loads environment-specific configuration (dev/prod) from JSON or S3
from lib.fs import must_exist_glob, ensure_dir  # Ensures file system paths exist or are created as needed
from lib.io import read_csv, write_parquet      # Handles Spark read/write with common options (headers, delimiters)
from lib.dq import dedupe_by_keys, normalize_lower, require_columns  # Data-quality helpers for validation/cleanup
from lib.logging import emit_run_stats     # Emits JSON-formatted run stats (row counts, environment, target path)

# ============================================================
# 🧭 STEP 1: Parse runtime arguments and start Spark
# ============================================================

# Parse Glue-style arguments passed at runtime.
# Expected args:
#   --ENV=dev or --ENV=prod
#   --CONFIG_S3_URI=file:///ws/jobs/customers_etl/config/dev.json
#   --BOOKMARKED=false (optional)
args = parse_args(sys.argv)

# Create a Spark session. This runs inside Glue or the local Docker Glue 5.0 container.
spark = SparkSession.builder.getOrCreate()

# ============================================================
# ⚙️ STEP 2: Load configuration
# ============================================================

# The configuration file contains both 'dev' and 'prod' settings.
# Example structure:
# {
#   "dev": {
#     "source_path": "file:///ws/data/customers/dev/*.csv",
#     "target_path": "file:///ws/out/customers/dev/",
#     "repartition": 4,
#     "partition_col": "ingest_dt"
#   }
# }
cfg_all = load_config(args["CONFIG_S3_URI"], spark=spark)
env = args["ENV"]
cfg = cfg_all[env]

# Extract key job parameters from config.
src_path  = cfg["source_path"]                 # Input source — can be local (file://) or remote (s3a://)
tgt_path  = cfg["target_path"]                 # Output destination
repart    = int(cfg.get("repartition", 8))     # Default to 8 partitions for parallelism if not specified
partition = cfg.get("partition_col", "ingest_dt")  # Partition column for output data

# ============================================================
# 🛡️ STEP 3: Validate and prepare local file system
# ============================================================

# When running locally, verify input data exists and ensure the output directory is writable.
# These checks are skipped automatically for s3a:// URIs (Glue-managed S3 locations).
must_exist_glob(src_path, label="source_path", if_scheme="file")
ensure_dir(tgt_path, scheme="file")

# ============================================================
# 📥 STEP 4: Read input data
# ============================================================

# Read CSV input data into a Spark DataFrame.
# read_csv() applies defaults like header=True and auto schema inference.
df = read_csv(spark, src_path)

# ============================================================
# 🧪 STEP 5: Validate schema and required columns
# ============================================================

# Define columns required for this specific ETL job.
# For customers_etl, we require:
#   - customer_id (primary key)
#   - email (transform target)
#   - partition column (e.g., ingest_dt)
required_cols = ["customer_id", "email"]
if partition:
    required_cols.append(partition)

# Enforce presence of required columns; will raise RuntimeError if missing.
require_columns(df, required_cols)

# ============================================================
# 🧹 STEP 6: Apply transformations and cleanup
# ============================================================

# Deduplicate on primary key (customer_id).
df = dedupe_by_keys(df, ["customer_id"])

# Standardize email addresses by lowercasing the domain and username parts.
df = normalize_lower(df, "email")

# Add an ingestion timestamp for lineage and debugging.
df = df.withColumn("load_ts", F.current_timestamp())

# ============================================================
# 💾 STEP 7: Write results
# ============================================================

# Write the transformed DataFrame to Parquet format.
# Partitioning improves downstream query performance (e.g., Athena, Trino).
write_parquet(df, tgt_path, repartition=repart, partitionBy=partition)

# ============================================================
# 📊 STEP 8: Emit run statistics
# ============================================================

# Emit structured run stats in JSON format.
# This helps with pipeline observability and can be indexed in ELK, CloudWatch, etc.
emit_run_stats(
    env=env,
    rows_out=df.count(),
    target=tgt_path,
    job="customers_etl"
)
