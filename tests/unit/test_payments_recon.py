import json, sys, runpy
from pathlib import Path

def test_e2e_payments_recon_two_sources(spark, tmp_path):
    left_dir  = tmp_path / "data" / "payments" / "dev"
    right_dir = tmp_path / "data" / "orders" / "dev"
    out_dir   = tmp_path / "out" / "payments_recon" / "dev"
    left_dir.mkdir(parents=True, exist_ok=True)
    right_dir.mkdir(parents=True, exist_ok=True)

    # NOTE: partition column matches config ("txn_dt")
    (left_dir / "left.csv").write_text(
        "order_id,txn_dt,value\n"
        "1,2025-11-01,alpha\n"
        "2,2025-11-02,beta\n"
    )
    (right_dir / "right.csv").write_text(
        "order_id,email\n"
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
            "partition_col": "txn_dt",
            "join_key": "order_id"
        }
    }
    cfg_path = tmp_path / "cfg.json"
    cfg_path.write_text(json.dumps(cfg))

    argv_bak = sys.argv[:]
    sys.argv = ["pytest","--ENV","dev","--CONFIG_S3_URI",f"file://{cfg_path}","--BOOKMARKED","false"]
    try:
        runpy.run_path(str(Path("jobs/payments_recon/main.py")), run_name="__main__")
    finally:
        sys.argv = argv_bak

    df = spark.read.parquet(str(out_dir))

    # Basic shape check
    assert df.count() == 2, f"Expected 2 rows, got {df.count()}"

    # Ensure email exists
    cols = set(df.columns)
    assert "email" in cols, f"'email' column missing. Columns: {sorted(cols)}"

    # Collect emails and assert all are lower-cased
    emails = [r["email"] for r in df.select("email").collect()]
    # Helpful diagnostics if this fails
    not_lower = [e for e in emails if e != (e or "").lower()]
    assert not not_lower, f"Emails not normalized to lowercase: {not_lower} | all_emails={emails}"

    # Exact set match (order-independent)
    assert set(emails) == {"alice@example.com", "bob@example.com"}, f"Emails mismatch: {sorted(emails)}"