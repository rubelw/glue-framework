import sys
from pyspark.sql import SparkSession, functions as F

from lib.args import parse_args
from lib.config import load_config
from lib.fs import must_exist_glob, ensure_dir
from lib.io import read_csv, write_parquet
from lib.dq import require_columns, normalize_lower, dedupe_by_keys
from lib.logging import emit_run_stats

# ---- args & spark ----
args = parse_args(sys.argv)
spark = SparkSession.builder.getOrCreate()

# ---- config ----
cfg_all = load_config(args["CONFIG_S3_URI"], spark=spark)
env = args["ENV"]
cfg = cfg_all[env]

# Config-driven settings
source_paths = cfg.get("source_paths", {})  # {"left": "...", "right": "..."}
target_path  = cfg["target_path"]
repartition  = int(cfg.get("repartition", 8))
partition_by = cfg.get("partition_col", "txn_dt")
join_key     = cfg.get("join_key", "order_id")

# ---- local guards (no-op for s3a://) ----
for label, path in source_paths.items():
    must_exist_glob(path, label=f"source:{label}", if_scheme="file")
ensure_dir(target_path, scheme="file")

left_path  = source_paths.get("left")
right_path = source_paths.get("right")

# ---- read ----
left  = read_csv(spark, left_path)
right = read_csv(spark, right_path)

# Require key + partition in left; key in right
req_left  = [join_key] + ([partition_by] if partition_by else [])
req_right = [join_key]
require_columns(left, req_left)
require_columns(right, req_right)

# ---- normalize/clean (apply explicitly to each DF) ----
if "email" in left.columns:
    left = normalize_lower(left, "email")
if "email" in right.columns:
    right = normalize_lower(right, "email")

# Right side should be unique on join key
right = right.dropDuplicates([join_key])

# ---- join & write ----
enriched = (
    left.join(F.broadcast(right), on=join_key, how="left")
        .withColumn("load_ts", F.current_timestamp())
)

write_parquet(enriched, target_path, repartition=repartition, partitionBy=partition_by)

# ---- run stats ----
emit_run_stats(
    env=env,
    rows_in_left=left.count(),
    rows_in_right=right.count(),
    rows_out=enriched.count(),
    target=target_path,
    job="payments_recon"
)
