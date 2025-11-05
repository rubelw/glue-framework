from pyspark.sql import DataFrame, SparkSession

def read_csv(spark: SparkSession, path: str, header: bool = True):
    return spark.read.option("header", "true" if header else "false").csv(path)

def write_parquet(df: DataFrame, path: str, *, mode="overwrite",
                  repartition: int = None, partitionBy: str = None):
    writer = df
    if repartition:
        writer = writer.repartition(int(repartition))
    w = writer.write.mode(mode)
    if partitionBy:
        w = w.partitionBy(partitionBy)
    w.parquet(path)
