# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks ⇄ Azure SQL Database
# MAGIC
# MAGIC This notebook shows how to:
# MAGIC 1. Configure a secure connection (credentials from a Databricks secret scope)
# MAGIC 2. **Read** from Azure SQL (full table, pushdown query, parallel read)
# MAGIC 3. **Write** to Azure SQL (append, overwrite, upsert via staging + MERGE)
# MAGIC 4. Verify the round trip
# MAGIC
# MAGIC **Prerequisites**
# MAGIC - Azure SQL firewall / VNet rules allow your Databricks workspace (or "Allow Azure services" is on)
# MAGIC - A secret scope with the SQL username and password, e.g.:
# MAGIC   ```
# MAGIC   databricks secrets create-scope azure-sql
# MAGIC   databricks secrets put-secret azure-sql sql-user
# MAGIC   databricks secrets put-secret azure-sql sql-password
# MAGIC   ```
# MAGIC - The Microsoft SQL Server JDBC driver ships with Databricks Runtime, so no library install is needed for the JDBC approach

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

dbutils.widgets.text("server",        "your-server.database.windows.net", "Azure SQL server")
dbutils.widgets.text("database",      "your_database",                    "Database")
dbutils.widgets.text("secret_scope",  "azure-sql",                        "Secret scope")
dbutils.widgets.text("user_key",      "sql-user",                         "Secret key: username")
dbutils.widgets.text("password_key",  "sql-password",                     "Secret key: password")
dbutils.widgets.text("source_table",  "dbo.SalesOrders",                  "Table to read")
dbutils.widgets.text("target_table",  "dbo.SalesOrdersSummary",           "Table to write")

server       = dbutils.widgets.get("server")
database     = dbutils.widgets.get("database")
scope        = dbutils.widgets.get("secret_scope")
source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")

sql_user     = dbutils.secrets.get(scope, dbutils.widgets.get("user_key"))
sql_password = dbutils.secrets.get(scope, dbutils.widgets.get("password_key"))

jdbc_url = (
    f"jdbc:sqlserver://{server}:1433;"
    f"database={database};"
    "encrypt=true;trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30;"
)

connection_properties = {
    "user": sql_user,
    "password": sql_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

print(f"Connecting to {server}/{database}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### (Optional) Microsoft Entra ID service principal auth
# MAGIC Use this instead of SQL username/password. Requires `%pip install msal` and the service principal
# MAGIC to be created as a user in the database (`CREATE USER [<sp-name>] FROM EXTERNAL PROVIDER`).
# MAGIC Set `USE_ENTRA_ID = True` to enable.

# COMMAND ----------

USE_ENTRA_ID = False

if USE_ENTRA_ID:
    import msal

    tenant_id     = dbutils.secrets.get(scope, "sp-tenant-id")
    client_id     = dbutils.secrets.get(scope, "sp-client-id")
    client_secret = dbutils.secrets.get(scope, "sp-client-secret")

    app = msal.ConfidentialClientApplication(
        client_id,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        client_credential=client_secret,
    )
    token = app.acquire_token_for_client(scopes=["https://database.windows.net/.default"])
    if "access_token" not in token:
        raise RuntimeError(f"Token request failed: {token.get('error_description')}")

    connection_properties = {
        "accessToken": token["access_token"],
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read from Azure SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Read a full table

# COMMAND ----------

df_full = spark.read.jdbc(url=jdbc_url, table=source_table, properties=connection_properties)

df_full.printSchema()
display(df_full.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Read with a pushdown query
# MAGIC Wrap the query in parentheses with an alias, so SQL Server does the filtering/aggregation and only the result is transferred.

# COMMAND ----------

pushdown_query = f"""
(
    SELECT *
    FROM {source_table}
    WHERE 1 = 1            -- add your filter, e.g. OrderDate >= '2025-01-01'
) AS src
"""

df_query = spark.read.jdbc(url=jdbc_url, table=pushdown_query, properties=connection_properties)
display(df_query.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c. Parallel read for large tables
# MAGIC Splits the read across multiple connections using a numeric (or date) column.
# MAGIC Set the column and bounds to match your data.

# COMMAND ----------

# Uncomment and adjust for your table
# df_parallel = (
#     spark.read.format("jdbc")
#     .option("url", jdbc_url)
#     .option("dbtable", source_table)
#     .option("user", sql_user)
#     .option("password", sql_password)
#     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
#     .option("partitionColumn", "OrderId")   # numeric/date/timestamp column
#     .option("lowerBound", 1)
#     .option("upperBound", 1_000_000)
#     .option("numPartitions", 8)             # = number of parallel connections
#     .option("fetchsize", 10000)
#     .load()
# )
# print(df_parallel.rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write to Azure SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare a DataFrame to write
# MAGIC Here we build a small sample DataFrame. Replace this with your own transformation,
# MAGIC or read from a Delta / Unity Catalog table, e.g. `spark.table("main.sales.orders")`.

# COMMAND ----------

from pyspark.sql import functions as F

df_to_write = (
    spark.createDataFrame(
        [
            (1, "EMEA", 120.50),
            (2, "EMEA",  75.25),
            (3, "APAC", 310.00),
            (4, "AMER", 42.10),
        ],
        ["OrderId", "Region", "Amount"],
    )
    .withColumn("LoadedAtUtc", F.current_timestamp())
)

display(df_to_write)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a. Append
# MAGIC Creates the table if it doesn't exist, otherwise adds rows.

# COMMAND ----------

(
    df_to_write.write
    .mode("append")
    .option("batchsize", 10000)        # rows per JDBC batch insert
    .jdbc(url=jdbc_url, table=target_table, properties=connection_properties)
)
print(f"Appended {df_to_write.count()} rows to {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. Overwrite (keep table schema)
# MAGIC `truncate=true` empties the table instead of dropping and recreating it, so indexes, constraints and column types are preserved.

# COMMAND ----------

# Uncomment to run
# (
#     df_to_write.write
#     .mode("overwrite")
#     .option("truncate", "true")
#     .option("batchsize", 10000)
#     .jdbc(url=jdbc_url, table=target_table, properties=connection_properties)
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3c. Upsert (staging table + MERGE)
# MAGIC Spark JDBC only appends/overwrites, so for upserts, write to a staging table and run a `MERGE` on the SQL side.
# MAGIC
# MAGIC > On **standard (shared) access mode** clusters, `spark._sc` is blocked. Use a dedicated / single-user cluster for this cell,
# MAGIC > or run the MERGE with `pyodbc` or from Azure Data Factory / a SQL job.

# COMMAND ----------

staging_table = f"{target_table}_staging"

# 1) Load the staging table (overwrite each run)
(
    df_to_write.write
    .mode("overwrite")
    .option("truncate", "true")
    .jdbc(url=jdbc_url, table=staging_table, properties=connection_properties)
)

# 2) Run MERGE on Azure SQL
merge_sql = f"""
MERGE {target_table} AS tgt
USING {staging_table} AS src
    ON tgt.OrderId = src.OrderId
WHEN MATCHED THEN
    UPDATE SET tgt.Region      = src.Region,
               tgt.Amount      = src.Amount,
               tgt.LoadedAtUtc = src.LoadedAtUtc
WHEN NOT MATCHED BY TARGET THEN
    INSERT (OrderId, Region, Amount, LoadedAtUtc)
    VALUES (src.OrderId, src.Region, src.Amount, src.LoadedAtUtc);
"""

def run_sql(statement: str):
    """Execute a T-SQL statement on Azure SQL using the JVM JDBC driver."""
    driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
    if "accessToken" in connection_properties:
        props = spark._sc._gateway.jvm.java.util.Properties()
        props.setProperty("accessToken", connection_properties["accessToken"])
        conn = driver_manager.getConnection(jdbc_url, props)
    else:
        conn = driver_manager.getConnection(jdbc_url, sql_user, sql_password)
    try:
        stmt = conn.createStatement()
        stmt.execute(statement)
        stmt.close()
    finally:
        conn.close()

run_sql(merge_sql)
print(f"Merged {staging_table} into {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify: read back what we wrote

# COMMAND ----------

df_check = spark.read.jdbc(url=jdbc_url, table=target_table, properties=connection_properties)
display(df_check.orderBy("OrderId"))
print(f"Row count in {target_table}: {df_check.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tips
# MAGIC - **Write speed:** tune `batchsize` (5k–10k is typical) and `numPartitions` (don't exceed what your Azure SQL tier can handle in concurrent connections). For very large loads, the [Apache Spark connector for SQL Server](https://github.com/microsoft/sql-spark-connector) (`com.microsoft.sqlserver.jdbc.spark`, installed as a cluster library) does bulk inserts and is much faster.
# MAGIC - **Column types:** with `overwrite` and no `truncate`, Spark drops and recreates the table with default types (e.g. `TEXT`). Pre-create the table in SQL and use `truncate=true` to control types.
# MAGIC - **Networking:** if you get a connection timeout, check the Azure SQL firewall, private endpoints, and your workspace's outbound IPs / NAT gateway.
# MAGIC - **Secrets:** never hardcode credentials in the notebook. Use secret scopes (Databricks-backed or Azure Key Vault-backed).
