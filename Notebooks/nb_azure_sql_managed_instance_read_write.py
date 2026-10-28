# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks ⇄ Azure SQL Managed Instance
# MAGIC
# MAGIC This notebook shows how to:
# MAGIC 1. Test network connectivity to the Managed Instance (the most common failure point)
# MAGIC 2. Configure a secure JDBC connection (credentials from a Databricks secret scope)
# MAGIC 3. **Read** from SQL MI (full table, pushdown query, parallel read)
# MAGIC 4. **Write** to SQL MI (append, overwrite, upsert via staging + MERGE)
# MAGIC 5. Verify the round trip
# MAGIC
# MAGIC ### How SQL Managed Instance differs from Azure SQL Database
# MAGIC | | Azure SQL Database | SQL Managed Instance |
# MAGIC |---|---|---|
# MAGIC | Host | `<server>.database.windows.net` | `<mi-name>.<dns-zone>.database.windows.net` (private) |
# MAGIC | Network | Public endpoint + firewall | **Private VNet by default**. Databricks must be VNet-injected/peered into a route to the MI subnet, or you enable the MI public endpoint |
# MAGIC | Port | 1433 | **1433** (private, proxy) or **11000–11999** (private, redirect mode); **3342** for the public endpoint |
# MAGIC | Public host | n/a | `<mi-name>.public.<dns-zone>.database.windows.net` |
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Network path from the Databricks compute to the MI (VNet peering / VNet injection, or the MI public endpoint with an NSG rule allowing your Databricks egress IPs on port 3342)
# MAGIC - A secret scope with the SQL username and password:
# MAGIC   ```
# MAGIC   databricks secrets create-scope sqlmi
# MAGIC   databricks secrets put-secret sqlmi sql-user
# MAGIC   databricks secrets put-secret sqlmi sql-password
# MAGIC   ```
# MAGIC - The Microsoft SQL Server JDBC driver ships with Databricks Runtime, so no library install is needed

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

dbutils.widgets.text("mi_host",       "your-mi.abc123def456.database.windows.net", "MI host (FQDN)")
dbutils.widgets.text("port",          "1433",                                      "Port (1433 private / 3342 public)")
dbutils.widgets.text("database",      "your_database",                             "Database")
dbutils.widgets.text("secret_scope",  "sqlmi",                                     "Secret scope")
dbutils.widgets.text("user_key",      "sql-user",                                  "Secret key: username")
dbutils.widgets.text("password_key",  "sql-password",                              "Secret key: password")
dbutils.widgets.text("source_table",  "dbo.SalesOrders",                           "Table to read")
dbutils.widgets.text("target_table",  "dbo.SalesOrdersSummary",                    "Table to write")
dbutils.widgets.text("cert_hostname", "",                                          "hostNameInCertificate (optional)")

mi_host      = dbutils.widgets.get("mi_host").strip()
port         = dbutils.widgets.get("port").strip()
database     = dbutils.widgets.get("database").strip()
scope        = dbutils.widgets.get("secret_scope")
source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")
cert_host    = dbutils.widgets.get("cert_hostname").strip()

sql_user     = dbutils.secrets.get(scope, dbutils.widgets.get("user_key"))
sql_password = dbutils.secrets.get(scope, dbutils.widgets.get("password_key"))

jdbc_url = (
    f"jdbc:sqlserver://{mi_host}:{port};"
    f"database={database};"
    "encrypt=true;trustServerCertificate=false;"
    "loginTimeout=30;"
)
# Only needed if you see a certificate hostname mismatch error, e.g. "*.abc123def456.database.windows.net"
# (public endpoint certificates use "*.public.<dns-zone>.database.windows.net")
if cert_host:
    jdbc_url += f"hostNameInCertificate={cert_host};"

connection_properties = {
    "user": sql_user,
    "password": sql_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

print(f"Target: {mi_host}:{port}/{database}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Test network connectivity
# MAGIC If this fails, the problem is networking (VNet peering, NSG, route table, or public endpoint), not credentials or code.

# COMMAND ----------

import socket

def check_tcp(host: str, port: int, timeout: float = 8.0) -> None:
    try:
        ip = socket.gethostbyname(host)
    except socket.gaierror as e:
        raise RuntimeError(
            f"DNS lookup failed for {host}: {e}. For a private MI endpoint, the Databricks VNet "
            "must be able to resolve the MI private DNS zone (peering + DNS link)."
        )
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            print(f"OK: TCP connection to {host} ({ip}) on port {port} succeeded")
    except OSError as e:
        raise RuntimeError(
            f"TCP connect to {host} ({ip}):{port} failed: {e}. Check VNet peering/injection, the MI "
            "subnet NSG + route table, and (for public endpoint) that it's enabled with port 3342 allowed."
        )

check_tcp(mi_host, int(port))

# COMMAND ----------

# MAGIC %md
# MAGIC ### (Optional) Microsoft Entra ID service principal auth
# MAGIC Use this instead of SQL username/password. Requires `%pip install msal`, and the service principal must exist in the MI
# MAGIC (`CREATE LOGIN [<sp-name>] FROM EXTERNAL PROVIDER` on master, then `CREATE USER ... FROM EXTERNAL PROVIDER` in the database).
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
# MAGIC ## 3. Read from SQL Managed Instance

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a. Read a full table

# COMMAND ----------

df_full = spark.read.jdbc(url=jdbc_url, table=source_table, properties=connection_properties)

df_full.printSchema()
display(df_full.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. Read with a pushdown query
# MAGIC Wrap the query in parentheses with an alias so SQL MI does the filtering/aggregation and only the result is transferred.
# MAGIC Because this is a Managed Instance, you can also query across databases on the same instance with three-part names
# MAGIC (e.g. `OtherDb.dbo.Customers`).

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
# MAGIC ### 3c. Parallel read for large tables
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
# MAGIC ## 4. Write to SQL Managed Instance

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
# MAGIC ### 4a. Append
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
# MAGIC ### 4b. Overwrite (keep table schema)
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
# MAGIC ### 4c. Upsert (staging table + MERGE)
# MAGIC Spark JDBC only appends/overwrites, so for upserts, write to a staging table and run a `MERGE` on the SQL side.
# MAGIC
# MAGIC > On **standard (shared) access mode** clusters, `spark._sc` is blocked. Use a dedicated / single-user cluster for this cell,
# MAGIC > or run the MERGE with `pyodbc` or from Azure Data Factory / a SQL Agent job (SQL MI supports SQL Agent).

# COMMAND ----------

staging_table = f"{target_table}_staging"

# 1) Load the staging table (overwrite each run)
(
    df_to_write.write
    .mode("overwrite")
    .option("truncate", "true")
    .jdbc(url=jdbc_url, table=staging_table, properties=connection_properties)
)

# 2) Run MERGE on SQL MI
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
    """Execute a T-SQL statement on SQL MI using the JVM JDBC driver."""
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
# MAGIC ## 5. Verify: read back what we wrote

# COMMAND ----------

df_check = spark.read.jdbc(url=jdbc_url, table=target_table, properties=connection_properties)
display(df_check.orderBy("OrderId"))
print(f"Row count in {target_table}: {df_check.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting & tips
# MAGIC - **Connection timeout / DNS failure:** it's almost always networking. The private MI endpoint is only reachable from inside (or peered to) the MI's VNet. Options: deploy Databricks with VNet injection into a peered VNet, or enable the MI public endpoint and allow your Databricks egress IPs on port **3342** in the MI subnet NSG (use `<mi-name>.public.<dns-zone>.database.windows.net` as the host, port 3342).
# MAGIC - **Redirect vs proxy connection policy:** with the private endpoint, *Redirect* mode needs ports **11000–11999** open (in addition to 1433) between Databricks and the MI subnet. *Proxy* mode only needs 1433. If connections hang after login, check this.
# MAGIC - **Certificate hostname mismatch:** set the optional `cert_hostname` widget to the wildcard the MI certificate uses.
# MAGIC - **Write speed:** tune `batchsize` (5k–10k is typical) and `numPartitions` (don't exceed what your MI vCore size can handle in concurrent connections). For very large loads, the [Apache Spark connector for SQL Server](https://github.com/microsoft/sql-spark-connector) (`com.microsoft.sqlserver.jdbc.spark`, installed as a cluster library) does bulk inserts and is much faster.
# MAGIC - **Column types:** with `overwrite` and no `truncate`, Spark drops and recreates the table with default types (e.g. `TEXT`). Pre-create the table in SQL and use `truncate=true` to control types.
# MAGIC - **Secrets:** never hardcode credentials in the notebook. Use secret scopes (Databricks-backed or Azure Key Vault-backed).
