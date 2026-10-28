# Databricks notebook source
from pyspark.sql import SparkSession
from databricks_unit_test_framework.jobs import run_customer_silver

spark = SparkSession.builder.getOrCreate()

source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")

run_customer_silver(
    spark=spark,
    source_table=source_table,
    target_table=target_table,
)
