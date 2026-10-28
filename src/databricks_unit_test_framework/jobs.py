from pyspark.sql import SparkSession
from .transformations import standardize_customer, calculate_order_amount


def run_customer_silver(spark: SparkSession, source_table: str, target_table: str) -> None:
    df = spark.table(source_table)
    result = standardize_customer(df)
    result.write.mode("overwrite").format("delta").saveAsTable(target_table)


def run_order_gold(spark: SparkSession, source_table: str, target_table: str) -> None:
    df = spark.table(source_table)
    result = calculate_order_amount(df)
    result.write.mode("overwrite").format("delta").saveAsTable(target_table)
