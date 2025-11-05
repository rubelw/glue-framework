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
source_paths = cfg.get("source_paths", {})  # {"primary": "..."}
target_path  = cfg["target_path"]
repartition  = int(cfg.get("repartition", 8))
partition_by = cfg.get("partition_col", "ingest_dt")
join_key     = cfg.get("join_key", "customer_id")

# Sink controls the write path: parquet (default) or iceberg
sink = cfg.get("sink", {"format": "parquet"})

# Local guards (no-op for s3a://)
for label, path in source_paths.items():
    must_exist_glob(path, label=f"source:{label}", if_scheme="file")
ensure_dir(target_path, scheme="file")

# Read primary source
primary_path = source_paths.get("primary")
df = read_csv(spark, primary_path)

# Required columns
req_cols = []
if partition_by: req_cols.append(partition_by)
if join_key:     req_cols.append(join_key)
require_columns(df, req_cols)

# Typical cleaning
if "email" in df.columns:
    df = normalize_lower(df, "email")
if join_key and join_key in df.columns:
    df = dedupe_by_keys(df, [join_key])

df = df.withColumn("load_ts", F.current_timestamp())

# Write: iceberg vs parquet
if sink.get("format") == "iceberg":
    # Expect a catalog configured via Spark conf (e.g., glue/hive) and these keys present:
    # sink = {"format":"iceberg","catalog":"glue_catalog","namespace":"default","table":"customers_iceberg"}
    catalog   = sink.get("catalog", "glue_catalog")
    namespace = sink.get("namespace", "default")
    table     = sink.get("table", "customers_iceberg")

    # NOTE: Catalog configuration must be provided in Spark session (catalog type, warehouse, IAM, etc.)
    # Example (Glue): spark.sql.catalog.glue_catalog = org.apache.iceberg.spark.SparkCatalog
    #                 spark.sql.catalog.glue_catalog.catalog-impl = org.apache.iceberg.aws.glue.GlueCatalog
    #                 spark.sql.catalog.glue_catalog.warehouse = s3://my-warehouse/
    df.writeTo(f"{catalog}.{namespace}.{table}").using("iceberg").createOrReplace()
else:
    # Parquet (default local-friendly)
    from lib.io import write_parquet
    write_parquet(df, target_path, repartition=repartition, partitionBy=partition_by)

emit_run_stats(
    env=env,
    rows_out=df.count(),
    target=target_path,
    job="customers_iceberg"
)
