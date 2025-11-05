import sys
from pyspark.sql import SparkSession, functions as F

# ---- Import shared libraries from /lib ----
# These provide reusable, tested helpers for argument parsing, config handling, I/O, validation, and logging.
from lib.args import parse_args                 # Parses command-line args: --ENV, --CONFIG_S3_URI, etc.
from lib.config import load_config              # Loads the environment configuration JSON (local or S3)
from lib.fs import must_exist_glob, ensure_dir  # Validates local file system paths or creates output dirs
from lib.io import read_csv, write_parquet      # Simplifies Spark CSV/Parquet I/O
from lib.dq import require_columns, normalize_lower, dedupe_by_keys  # Enforces data quality standards
from lib.logging import emit_run_stats          # Logs structured job metrics (counts, env, output paths)

# ============================================================
# 🧭 STEP 1: Parse runtime arguments and initialize Spark
# ============================================================

# Parse AWS Glue-style arguments passed via `spark-submit` or Makefile.
# Example:
#   --ENV=dev
#   --CONFIG_S3_URI=file:///ws/jobs/orders_enrichment/config/dev.json
#   --BOOKMARKED=false
args = parse_args(sys.argv)

# Create a Spark session (entry point for all DataFrame operations).
# In local mode, this connects to the Glue 5.0 container’s embedded Spark runtime.
spark = SparkSession.builder.getOrCreate()

# ============================================================
# ⚙️ STEP 2: Load configuration for the selected environment
# ============================================================

# Load job configuration (both dev and prod settings may exist).
# The function supports `file://` (local JSONs) and `s3://` URIs.
cfg_all = load_config(args["CONFIG_S3_URI"], spark=spark)

# Extract environment-specific settings (e.g., “dev”, “prod”)
env = args["ENV"]
cfg = cfg_all[env]

# Configuration dictionary typically looks like:
# {
#   "dev": {
#     "source_paths": {
#       "left": "file:///ws/data/orders/dev/*.csv",
#       "right": "file:///ws/data/customers/dev/*.csv"
#     },
#     "target_path": "file:///ws/out/orders_enrichment/dev/",
#     "repartition": 4,
#     "partition_col": "order_dt",
#     "join_key": "customer_id"
#   }
# }

# Pull individual config parameters with sensible defaults.
source_paths = cfg.get("source_paths", {})         # Dict of input sources (e.g., orders + customers)
target_path  = cfg["target_path"]                  # Where to write results
repartition  = int(cfg.get("repartition", 8))      # Controls output parallelism
partition_by = cfg.get("partition_col", "order_dt")  # Optional partition column for writes
join_key     = cfg.get("join_key", "customer_id")  # Common key used for joins

# ============================================================
# 🛡️ STEP 3: Validate local inputs and output paths
# ============================================================

# For local runs, confirm that source files exist and output directories are writable.
# This prevents Spark from failing later due to missing paths.
for label, path in source_paths.items():
    must_exist_glob(path, label=f"source:{label}", if_scheme="file")

# Create output directories if they don’t exist (local mode only).
ensure_dir(target_path, scheme="file")

# ============================================================
# 📥 STEP 4: Read source data
# ============================================================

# Read both the left (primary) and right (lookup/enrichment) datasets.
# The helper function `read_csv` automatically applies header=True and schema inference.
left_path  = source_paths.get("left")
right_path = source_paths.get("right")

left  = read_csv(spark, left_path)
right = read_csv(spark, right_path)

# ============================================================
# 🧪 STEP 5: Validate schema and required columns
# ============================================================

# The left dataset must contain the join key and optionally the partition column.
req_left  = [join_key] + ([partition_by] if partition_by else [])

# The right dataset must at least contain the join key.
req_right = [join_key]

# Validate that both dataframes contain their required columns.
# If not, `require_columns` raises a descriptive RuntimeError.
require_columns(left, req_left)
require_columns(right, req_right)

# ============================================================
# 🧹 STEP 6: Data standardization and cleanup
# ============================================================

# Apply normalization to email columns if present.
# This ensures consistent lowercasing for joins and reporting.
if "email" in left.columns:
    left = normalize_lower(left, "email")
if "email" in right.columns:
    right = normalize_lower(right, "email")

# Deduplicate the right-side dataset on the join key to avoid duplicate enrichment rows.
right = right.dropDuplicates([join_key])

# ============================================================
# 🔗 STEP 7: Join datasets and enrich
# ============================================================

# Perform a LEFT JOIN from the main dataset (left) to the enrichment dataset (right).
# The broadcast hint ensures the smaller dataframe (right) is distributed efficiently.
enriched = (
    left.join(F.broadcast(right), on=join_key, how="left")
         .withColumn("load_ts", F.current_timestamp())  # Add load timestamp for lineage tracking
)

# ============================================================
# 💾 STEP 8: Write enriched results to output
# ============================================================

# Write to Parquet format for optimal performance and schema evolution.
# Output partitioning improves read performance for downstream consumers (Athena, Trino, etc.).
write_parquet(enriched, target_path, repartition=repartition, partitionBy=partition_by)

# ============================================================
# 📊 STEP 9: Emit run statistics
# ============================================================

# Collect lightweight metrics for observability and logging.
# These can be ingested into dashboards or logs for Glue monitoring.
emit_run_stats(
    env=env,
    rows_in_left=left.count(),
    rows_in_right=right.count(),
    rows_out=enriched.count(),
    target=target_path,
    job="orders_enrichment"
)
