import sys, json, time, pathlib
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F

ARG_NAMES = ["ENV", "CONFIG_S3_URI", "BOOKMARKED"]
args = getResolvedOptions(sys.argv, ARG_NAMES)

spark = SparkSession.builder.getOrCreate()

def load_config(uri: str) -> dict:
    if uri.startswith("file://"):
        path = uri.replace("file://", "")
        with open(path, "r") as f:
            return json.load(f)
    if uri.startswith("s3a://"):
        # Optional: read config from S3 if you switch to s3a locally
        df = spark.read.text(uri)
        raw = "\n".join(r.value for r in df.collect())
        return json.loads(raw)
    raise ValueError(f"Unsupported CONFIG_S3_URI: {uri}")

cfg_all = load_config(args["CONFIG_S3_URI"])
env = args["ENV"]
cfg = cfg_all[env]

orders_path    = cfg["orders_path"]       # e.g., file:///ws/data/orders/dev/*.csv
customers_path = cfg["customers_path"]    # e.g., file:///ws/data/customers/dev/*.csv
target_path    = cfg["target_path"]       # e.g., file:///ws/out/orders_enrichment/dev/
repartition_n  = int(cfg.get("repartition", 8))
join_key       = cfg.get("join_key", "customer_id")
partition_col  = cfg.get("partition_col", "order_dt")

# --- Read ---
orders_df = (spark.read.option("header", "true").csv(orders_path))
cust_df   = (spark.read.option("header", "true").csv(customers_path))

# Ensure expected columns exist
for col in [join_key, partition_col, "email"]:
    if col not in orders_df.columns and col in ["email"]:
        # email may come from customers; that’s fine
        pass
    elif col not in orders_df.columns and col != "email":
        raise RuntimeError(f"Missing required column '{col}' in orders input.")
if join_key not in cust_df.columns:
    raise RuntimeError(f"Missing required join key '{join_key}' in customers input.")

# --- Clean + Join ---
cust_clean = (
    cust_df
    .withColumn("email", F.lower(F.col("email")))
    .dropDuplicates([join_key])
)

enriched = (
    orders_df
    .join(F.broadcast(cust_clean.select(join_key, "email")), on=join_key, how="left")
    .withColumn("load_ts", F.current_timestamp())
)

# --- Write ---
(enriched
 .repartition(repartition_n)
 .write
 .mode("overwrite")
 .partitionBy(partition_col)
 .parquet(target_path))

# --- Emit simple run stats ---
print(json.dumps({
    "env": env,
    "rows_in_orders": orders_df.count(),
    "rows_in_customers": cust_df.count(),
    "rows_out": enriched.count(),
    "target": target_path,
    "ts": int(time.time())
}))
