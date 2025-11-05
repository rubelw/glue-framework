import json, sys, runpy
from pathlib import Path

def test_orders_enrichment_e2e(spark, tmp_path, monkeypatch):
    # inputs
    cust_dir = tmp_path / "data" / "customers" / "dev"
    ord_dir  = tmp_path / "data" / "orders" / "dev"
    out_dir  = tmp_path / "out" / "orders_enrichment" / "dev"
    cust_dir.mkdir(parents=True, exist_ok=True)
    ord_dir.mkdir(parents=True, exist_ok=True)

    (cust_dir / "customers.csv").write_text(
        "customer_id,email,ingest_dt\n"
        "1,ALICE@EXAMPLE.COM,2025-11-01\n"
        "2,bob@example.com,2025-11-01\n"
    )
    (ord_dir / "orders.csv").write_text(
        "order_id,customer_id,order_dt,amount\n"
        "o-1,1,2025-11-01,10.0\n"
        "o-2,2,2025-11-02,5.0\n"
    )

    cfg = {
        "dev": {
            "orders_path": f"file://{ord_dir}/*.csv",
            "customers_path": f"file://{cust_dir}/*.csv",
            "target_path": f"file://{out_dir}/",
            "repartition": 2,
            "join_key": "customer_id",
            "partition_col": "order_dt"
        }
    }
    cfg_path = tmp_path / "orders_cfg.json"
    cfg_path.write_text(json.dumps(cfg))

    argv_bak = sys.argv[:]
    sys.argv = [
        "pytest",
        "--ENV", "dev",
        "--CONFIG_S3_URI", f"file://{cfg_path}",
        "--BOOKMARKED", "false"
    ]
    try:
        runpy.run_path(str(Path("jobs/orders_enrichment/main.py")), run_name="__main__")
    finally:
        sys.argv = argv_bak

    df = spark.read.parquet(str(out_dir))
    assert df.count() == 2
    emails = {r.email for r in df.select("email").collect()}
    assert emails == {"alice@example.com", "bob@example.com"}
