# tests/unit/test_customers_etl.py
import json
import runpy
import sys
from pathlib import Path

def test_customers_etl_e2e_writes_parquet_and_dedupes(spark, tmp_path, monkeypatch, capsys):
    # 1) Create tiny input CSVs
    data_dir = tmp_path / "data" / "customers" / "dev"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "customers_part1.csv").write_text(
        "customer_id,email,ingest_dt\n"
        "1,Alice@example.com,2025-11-01\n"
        "2,bob@Example.com,2025-11-01\n"
        "2,bob@Example.com,2025-11-01\n"
        "3,Carla@EXAMPLE.COM,2025-11-02\n"
    )

    # 2) Config (dev + prod sections; job reads by ENV)
    out_dir = tmp_path / "out" / "customers" / "dev"
    cfg = {
        "dev": {
            "source_path": f"file://{data_dir}/*.csv",
            "target_path": f"file://{out_dir}/",
            "repartition": 2
        },
        "prod": {
            "source_path": "file:///no-op",
            "target_path": "file:///no-op",
            "repartition": 8
        }
    }
    cfg_path = tmp_path / "config.json"
    cfg_path.write_text(json.dumps(cfg))

    # 3) Set argv as if Glue passed arguments
    sys_argv_backup = sys.argv[:]
    sys.argv = [
        "pytest",
        "--ENV", "dev",
        "--CONFIG_S3_URI", f"file://{cfg_path}",
        "--BOOKMARKED", "false"
    ]
    try:
        # 4) Execute the job entrypoint (uses awsglue + pyspark inside container)
        runpy.run_path(str(Path("jobs/customers_etl/main.py")), run_name="__main__")
    finally:
        sys.argv = sys_argv_backup

    # 5) Validate output exists and is cleaned/deduped
    #    Read back from the target path (partitioned by ingest_dt)
    out_df = spark.read.parquet(str(out_dir))
    # Expect duplicates on customer_id=2 to be dropped → 3 unique rows
    assert out_df.count() == 3

    # Emails lowercased
    emails = {r.email for r in out_df.select("email").collect()}
    assert emails == {"alice@example.com", "bob@example.com", "carla@example.com"}

    # Required columns exist
    cols = set(out_df.columns)
    for c in ["customer_id", "email", "ingest_dt", "load_ts"]:
        assert c in cols
