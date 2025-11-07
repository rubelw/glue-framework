import json, sys, runpy
from pathlib import Path

def test_e2e_orders_enrichment_two_sources(spark, tmp_path, pg_env):
    # --- Arrange: create CSV inputs ---
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

    # --- Config: write to Postgres (similar to customers_etl test) ---
    pg_url = f"jdbc:postgresql://{pg_env['host']}:{pg_env['port']}/{pg_env['db']}"
    pg_table = "public.orders_enriched"

    cfg = {
        "dev": {
            "source_paths": {
                "left":  f"file://{left_dir}/*.csv",
                "right": f"file://{right_dir}/*.csv"
            },
            "target_path":  f"file://{out_dir}/",  # still used if parquet sink is enabled
            "repartition":  2,
            "partition_col": "order_dt",
            "join_key": "customer_id",
            "postgres": {
                "url": pg_url,
                "user": pg_env["user"],
                "password": pg_env["password"],
                "driver": "org.postgresql.Driver",
                "table": pg_table,
                "mode": "overwrite"
            }
        }
    }
    cfg_path = tmp_path / "orders_enrichment_cfg.json"
    cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

    # --- Act: run the job like Glue/Makefile would ---
    argv_bak = sys.argv[:]
    sys.argv = [
        "pytest",
        "--ENV", "dev",
        "--CONFIG_S3_URI", f"file://{cfg_path}",
        "--BOOKMARKED", "false",
    ]
    try:
        runpy.run_path(str(Path("jobs/orders_enrichment/main.py")), run_name="__main__")
    finally:
        sys.argv = argv_bak

    # --- Assert: read back from Postgres via JDBC and validate ---
    df_pg = (
        spark.read
        .format("jdbc")
        .option("url", pg_url)
        .option("dbtable", pg_table)
        .option("user", pg_env["user"])
        .option("password", pg_env["password"])
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    # Expect 2 rows (one per order) and normalized emails; columns should be lower-case
    assert df_pg.count() == 2, "Expected two enriched order rows in Postgres."
    for expected_col in ["order_id", "customer_id", "order_dt","amount"]:
        assert expected_col in df_pg.columns, f"Expected column '{expected_col}' in Postgres sink."


