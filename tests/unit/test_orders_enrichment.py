import json, sys, runpy
from pathlib import Path

def test_e2e_orders_enrichment_two_sources(spark, tmp_path):
    left_dir  = tmp_path / "data" / "orders" / "dev"
    right_dir = tmp_path / "data" / "customers" / "dev"
    out_dir   = tmp_path / "out" / "orders_enrichment" / "dev"
    left_dir.mkdir(parents=True, exist_ok=True)
    right_dir.mkdir(parents=True, exist_ok=True)

    (left_dir / "left.csv").write_text(
        "customer_id,order_dt,value\n"
        "1,2025-11-01,alpha\n"
        "2,2025-11-02,beta\n"
    )
    (right_dir / "right.csv").write_text(
        "customer_id,email\n"
        "1,ALICE@EXAMPLE.COM\n"
        "2,bob@example.com\n"
    )

    cfg = {
        "dev": {
            "source_paths": {
                "left":  f"file://{left_dir}/*.csv",
                "right": f"file://{right_dir}/*.csv"
            },
            "target_path":  f"file://{out_dir}/",
            "repartition":  2,
            "partition_col": "order_dt",
            "join_key": "customer_id"
        }
    }
    cfg_path = tmp_path / "cfg.json"
    cfg_path.write_text(json.dumps(cfg))

    argv_bak = sys.argv[:]
    sys.argv = ["pytest","--ENV","dev","--CONFIG_S3_URI",f"file://{cfg_path}","--BOOKMARKED","false"]
    try:
        runpy.run_path(str(Path("jobs/orders_enrichment/main.py")), run_name="__main__")
    finally:
        sys.argv = argv_bak

    df = spark.read.parquet(str(out_dir))
    # two orders, both should be present; email normalized
    assert df.count() == 2
    emails = {r.email for r in df.select("email").collect() if hasattr(r, "email")}
    assert emails == {"alice@example.com", "bob@example.com"}