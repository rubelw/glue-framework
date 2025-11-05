import sys
from pyspark.sql import SparkSession, functions as F

from lib.args import parse_args
from lib.config import load_config
from lib.fs import must_exist_glob, ensure_dir
from lib.io import read_csv, write_parquet
from lib.dq import dedupe_by_keys, normalize_lower, require_columns
from lib.logging import emit_run_stats

# ---- args & spark ----
args = parse_args(sys.argv)
spark = SparkSession.builder.getOrCreate()

# ---- config ----
cfg_all = load_config(args["CONFIG_S3_URI"], spark=spark)
env = args["ENV"]
cfg = cfg_all[env]

src_path  = cfg["source_path"]                 # file:/// or s3a://
tgt_path  = cfg["target_path"]
repart    = int(cfg.get("repartition", 8))
partition = cfg.get("partition_col", "ingest_dt")

# ---- local guardrails (no-op for s3a://) ----
must_exist_glob(src_path, label="source_path", if_scheme="file")
ensure_dir(tgt_path, scheme="file")

# ---- read ----
df = read_csv(spark, src_path)

# Require columns that actually apply to this job
required_cols = ["customer_id", "email"]
if partition:
    required_cols.append(partition)
require_columns(df, required_cols)

# ---- transforms ----
df = dedupe_by_keys(df, ["customer_id"])
df = normalize_lower(df, "email")
df = df.withColumn("load_ts", F.current_timestamp())

# ---- write ----
write_parquet(df, tgt_path, repartition=repart, partitionBy=partition)

# ---- run stats ----
emit_run_stats(env=env, rows_out=df.count(), target=tgt_path, job="customers_etl")
