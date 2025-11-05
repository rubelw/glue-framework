from pyspark.sql import Row
from lib.dq import dedupe_by_keys, normalize_lower, require_columns

def test_dedupe_and_normalize(spark):
    df = spark.createDataFrame([
        Row(customer_id="1", email="ALICE@EXAMPLE.COM"),
        Row(customer_id="1", email="ALICE@EXAMPLE.COM"),
        Row(customer_id="2", email="Bob@Example.com"),
    ])
    require_columns(df, ["customer_id", "email"])
    out = dedupe_by_keys(df, ["customer_id"])
    assert out.count() == 2
    out2 = normalize_lower(out, "email")
    emails = {r.email for r in out2.select("email").collect()}
    assert emails == {"alice@example.com", "bob@example.com"}
