import sys, json, time
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
  # (Optional) Support s3a:// for local S3 testing with creds:
  if uri.startswith("s3a://"):
    df = spark.read.text(uri)
    raw = "\n".join(r.value for r in df.collect())
    return json.loads(raw)
  raise ValueError(f"Unsupported CONFIG_S3_URI: {uri}")

cfg = load_config(args["CONFIG_S3_URI"])
env = args["ENV"]
job_cfg = cfg[env]

src_path = job_cfg["source_path"]           # e.g., file:///ws/data/customers/*.csv
tgt_path = job_cfg["target_path"]           # e.g., file:///ws/out/customers/
repart   = int(job_cfg.get("repartition", 8))

df = (spark.read.option("header", "true").csv(src_path))
df_clean = (df.dropDuplicates(["customer_id"])
              .withColumn("email", F.lower(F.col("email")))
              .withColumn("load_ts", F.current_timestamp()))

(df_clean.repartition(repart)
        .write.mode("overwrite")
        .partitionBy("ingest_dt")
        .parquet(tgt_path))

print(json.dumps({
  "env": env,
  "rows_out": df_clean.count(),
  "target": tgt_path,
  "ts": int(time.time())
}))
