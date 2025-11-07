# tests/unit/test_payments_recon.py
import os
import json
import runpy
import sys
from pathlib import Path


def test_e2e_payments_recon_two_sources(spark, tmp_path, pg_env):
    """
    End-to-end test: read CSV inputs (payments + orders) and write enriched rows to Postgres.

    This test runs unconditionally (no integration skip).
    It will fail if Postgres is not reachable.
    """

    # 1) Build config JSON for the job
    left_dir  = tmp_path / "data" / "payments" / "dev"
    right_dir = tmp_path / "data" / "orders" / "dev"
    out_dir   = tmp_path / "out" / "payments_recon" / "dev"
    left_dir.mkdir(parents=True, exist_ok=True)
    right_dir.mkdir(parents=True, exist_ok=True)

    # Inputs (partition column matches config: txn_dt)
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

    pg_url = f"jdbc:postgresql://{pg_env['host']}:{pg_env['port']}/{pg_env['db']}"
    pg_table = "public.payments_recon_enriched"

    cfg = {
        "dev": {
            "source_paths": {
                "left":  f"file://{left_dir}/*.csv",
                "right": f"file://{right_dir}/*.csv"
            },
            "postgres": {
                "url": pg_url,
                "user": pg_env["user"],
                "password": pg_env["password"],
                "driver": "org.postgresql.Driver",
                "table": pg_table,
                "mode": "overwrite"
            },
            "target_path": f"file://{out_dir}/",
            "repartition": 2,
            "partition_col": "txn_dt",
            "join_key": "order_id"
        }
    }
    cfg_path = tmp_path / "payments_recon_cfg.json"
    cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

    # 2) Simulate Glue/Makefile job args
    argv_bak = sys.argv[:]
    sys.argv = [
        "pytest",
        "--ENV", "dev",
        "--CONFIG_S3_URI", f"file://{cfg_path}",
        "--BOOKMARKED", "false",
    ]

    try:
        # 3) Execute job
        runpy.run_path(str(Path("jobs/payments_recon/main.py")), run_name="__main__")
    finally:
        sys.argv = argv_bak

    # 4) Validate rows in Postgres output table
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    df_pg = (
        spark.read
        .format("jdbc")
        .option("url", cfg["dev"]["postgres"]["url"])
        .option("dbtable", cfg["dev"]["postgres"]["table"])
        .option("user", cfg["dev"]["postgres"]["user"])
        .option("password", cfg["dev"]["postgres"]["password"])
        .option("driver", cfg["dev"]["postgres"]["driver"])
        .load()
    )

    assert df_pg.count() > 0, "Expected rows in Postgres sink table, but found none."
    for col in ["order_id", "email"]:
        assert col in df_pg.columns, f"Expected column '{col}' in Postgres sink."
