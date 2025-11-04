from pyspark.sql import functions as F

def normalize_emails(df, col="email"):
    return df.withColumn(col, F.lower(F.col(col)))
