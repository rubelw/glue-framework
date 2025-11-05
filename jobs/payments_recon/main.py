import sys
from pyspark.sql import SparkSession, functions as F

# ---- Import shared framework libraries ----
# These helper modules (under lib/) keep job scripts lightweight and consistent across all Glue jobs.
from lib.args import parse_args                  # Parses command-line args: --ENV, --CONFIG_S3_URI, --BOOKMARKED
from lib.config import load_config               # Loads the job's JSON config from file:// or s3:// URI
from lib.fs import must_exist_glob, ensure_dir   # Validates source and output paths, creates local dirs as needed
from lib.io import read_csv, write_parquet       # Unified Spark I/O helpers for CSV and Parquet formats
from lib.dq import require_columns, normalize_lower, dedupe_by_keys  # Data-quality and normalization utilities
from lib.logging import emit_run_stats           # Emits structured JSON metrics (rows in/out, env, target, etc.)

# ============================================================
# 🧭 STEP 1: Parse runtime arguments and initialize Spark
# ============================================================

# Parse Glue-style CLI arguments passed by spark-submit or the Makefile runner.
# Example:
#   --ENV=dev
#   --CONFIG_S3_URI=file:///ws/jobs/payments_recon/config/dev.json
#   --BOOKMARKED=false
args = parse_args(sys.argv)

# Create a Spark session — the main entry point for all Spark SQL and DataFrame APIs.
# This runs inside the AWS Glue runtime (or locally inside the Glue 5.0 Docker image).
spark = SparkSession.builder.getOrCreate()

# ============================================================
# ⚙️ STEP 2: Load configuration for current environment
# ============================================================

# Load the JSON configuration that defines source/target paths, partitioning, and join keys.
# Supports reading from both local (file://) and S3 (s3:// or s3a://) URIs.
cfg_all = load_config(args["CONFIG_S3_URI"], spark=spark)

# Extract the environment-specific configuration (e.g., "dev" or "prod").
env = args["ENV"]
cfg = cfg_all[env]

# Example config structure (in dev.json):
# {
#   "dev": {
#     "source_paths": {
#       "left":  "file:///ws/data/payments/dev/*.csv",
#       "right": "file:///ws/data/orders/dev/*.csv"
#     },
#     "target_path": "file:///ws/out/payments_recon/dev/",
#     "repartition": 2,
#     "partition_col": "txn_dt",
#     "join_key": "order_id"
#   }
# }

# Pull key settings from the loaded configuration.
source_paths = cfg.get("source_paths", {})          # Dictionary defining input datasets ("left" and "right")
target_path  = cfg["target_path"]                   # Output location for the enriched dataset
repartition  = int(cfg.get("repartition", 8))       # Spark parallelism — affects shuffle size and output files
partition_by = cfg.get("partition_col", "txn_dt")   # Column to partition output data by (optional)
join_key     = cfg.get("join_key", "order_id")      # Primary key for joining the two datasets

# ============================================================
# 🛡️ STEP 3: Validate local input/output paths
# ============================================================

# These checks are important for local development.
# They ensure that input files exist before Spark starts and that the output directory is writable.
# For S3-based URIs, these checks are skipped (Glue manages S3 paths itself).
for label, path in source_paths.items():
    must_exist_glob(path, label=f"source:{label}", if_scheme="file")

# Ensure the output directory exists (only for local filesystem targets).
ensure_dir(target_path, scheme="file")

# Extract paths for the left (primary) and right (lookup/enrichment) datasets.
left_path  = source_paths.get("left")
right_path = source_paths.get("right")

# ============================================================
# 📥 STEP 4: Read input data
# ============================================================

# Read the left and right datasets as DataFrames using the shared helper.
# These helpers standardize CSV options (header=True, inferSchema=True) for all Glue jobs.
left  = read_csv(spark, left_path)
right = read_csv(spark, right_path)

# ============================================================
# 🧪 STEP 5: Validate schema requirements
# ============================================================

# Ensure the left dataset has both the join key and the partition column.
# The partition column (txn_dt) is used for output partitioning.
req_left  = [join_key] + ([partition_by] if partition_by else [])

# Ensure the right dataset contains at least the join key.
req_right = [join_key]

# Validate both DataFrames.
# Raises a descriptive RuntimeError if any required column is missing.
require_columns(left, req_left)
require_columns(right, req_right)

# ============================================================
# 🧹 STEP 6: Clean and normalize data
# ============================================================

# Normalize email addresses if the column exists in either dataset.
# The normalize_lower() function lowercases email strings for consistency.
if "email" in left.columns:
    left = normalize_lower(left, "email")
if "email" in right.columns:
    right = normalize_lower(right, "email")

# Deduplicate the right-hand dataset on the join key.
# This ensures each order_id (or customer_id) maps to a single lookup record.
right = right.dropDuplicates([join_key])

# ============================================================
# 🔗 STEP 7: Join and enrich data
# ============================================================

# Perform a LEFT JOIN between payments (left) and orders (right).
# Broadcasting the smaller dataset ("right") optimizes performance by avoiding shuffles.
enriched = (
    left.join(F.broadcast(right), on=join_key, how="left")
         .withColumn("load_ts", F.current_timestamp())  # Add load timestamp for lineage and auditing
)

# ============================================================
# 💾 STEP 8: Write results to output
# ============================================================

# Write output as Parquet (columnar format optimized for analytics).
# The repartition parameter controls how many output files are produced.
# Partitioning by txn_dt improves query performance for time-based analysis.
write_parquet(enriched, target_path, repartition=repartition, partitionBy=partition_by)

# ============================================================
# 📊 STEP 9: Emit run statistics
# ============================================================

# Emit structured run statistics including row counts and environment info.
# This can be logged to stdout, ELK, or CloudWatch for observability.
emit_run_stats(
    env=env,
    rows_in_left=left.count(),
    rows_in_right=right.count(),
    rows_out=enriched.count(),
    target=target_path,
    job="payments_recon"
)
