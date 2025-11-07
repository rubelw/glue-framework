# tests/unit/test_transform.py
from pyspark.sql import Row
from lib.transform import normalize_emails

def test_normalize_emails_lowers_case(spark):
    df = spark.createDataFrame(
        [Row(customer_id="1", email="Alice@Example.COM"),
         Row(customer_id="2", email="BoB@Example.com")]
    )
    out = normalize_emails(df, col="email")
    rows = {r.email for r in out.select("email").collect()}
    assert rows == {"alice@example.com", "bob@example.com"}
