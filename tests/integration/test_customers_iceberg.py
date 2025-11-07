import json, sys, runpy
from pathlib import Path

def test_e2e_customers_iceberg_single(spark, tmp_path):
    data_dir = tmp_path / "data" / "customers" / "dev"
    out_dir  = tmp_path / "out" / "customers_iceberg" / "dev"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "sample.csv").write_text(
        "customer_id,value,ingest_dt\n"
        "1,alpha,2025-11-01\n"
        "2,beta,2025-11-02\n"
    )

    cfg = {
        "dev": {
            "source_paths": { "primary": f"file://{data_dir}/*.csv" },
            "target_path":  f"file://{out_dir}/",
            "repartition":  2,
            "partition_col": "ingest_dt",
            "join_key": "customer_id",
            "sink": { "format": "parquet" }
        }
    }
    cfg_path = tmp_path / "cfg.json"
    cfg_path.write_text(json.dumps(cfg))

    argv_bak = sys.argv[:]
    sys.argv = ["pytest","--ENV","dev","--CONFIG_S3_URI",f"file://{cfg_path}","--BOOKMARKED","false"]
    try:
        runpy.run_path(str(Path("jobs/customers_iceberg/main.py")), run_name="__main__")
    finally:
        sys.argv = argv_bak

    df = spark.read.parquet(str(out_dir))
    assert df.count() == 2
