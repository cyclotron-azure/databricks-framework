from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def standardize_customer(df: DataFrame) -> DataFrame:
    """
    Example Silver transformation:
      - trim names
      - normalize email
      - derive customer_status
      - remove exact duplicate customer_id records
    """
    return (
        df
        .withColumn("first_name", F.trim(F.col("first_name")))
        .withColumn("last_name", F.trim(F.col("last_name")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn(
            "customer_status",
            F.when(F.col("is_active") == True, F.lit("ACTIVE"))
             .otherwise(F.lit("INACTIVE"))
        )
        .dropDuplicates(["customer_id"])
    )


def calculate_order_amount(df: DataFrame) -> DataFrame:
    """Example Silver/Gold business calculation."""
    return df.withColumn(
        "order_amount",
        F.round(F.col("quantity") * F.col("unit_price"), 2)
    )
