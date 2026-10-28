# Databricks notebook source
# MAGIC %md
# MAGIC # ADLS Gen2 CSV → Parquet with Upsert
# MAGIC
# MAGIC Reads CSV files from Azure Data Lake Storage Gen2 and upserts them into a target stored as **Parquet**.
# MAGIC
# MAGIC > **Important:** plain Parquet files are immutable, so they have no native `UPDATE` / `MERGE`.
# MAGIC > The standard solution on Databricks is **Delta Lake**: the data is stored as Parquet files, with a transaction log
# MAGIC > on top that makes `MERGE` (upsert) atomic. That's the main path in this notebook.
# MAGIC > If you truly need a *plain* Parquet folder with no Delta log, see sections 7 and 8.
# MAGIC
# MAGIC **What this notebook does**
# MAGIC 1. Configures ADLS access
# MAGIC 2. Reads CSV (with header, schema inference, and lineage columns)
# MAGIC 3. De-duplicates the incoming batch (keeps the latest row per key)
# MAGIC 4. Upserts into the target with `MERGE` (insert new keys, update changed keys, ignore stale rows)
# MAGIC 5. Verifies results
# MAGIC 6. *(Optional)* Auto Loader for incremental file ingestion
# MAGIC 7. *(Optional)* Plain-Parquet upsert fallback
# MAGIC 8. *(Optional)* Export a plain-Parquet snapshot

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

dbutils.widgets.text("storage_account", "yourstorageaccount",   "Storage account")
dbutils.widgets.text("container",       "datalake",             "Container")
dbutils.widgets.text("source_path",     "raw/customers/",       "Source CSV folder (in container)")
dbutils.widgets.text("target_path",     "curated/customers",    "Target folder (in container)")
dbutils.widgets.text("key_cols",        "customer_id",          "Business key column(s), comma-separated")
dbutils.widgets.text("order_col",       "updated_at",           "Recency column (latest wins); blank = none")
dbutils.widgets.text("partition_cols",  "",                     "Partition column(s), optional")
dbutils.widgets.dropdown("auth_mode", "unity_catalog", ["unity_catalog", "service_principal", "account_key"], "ADLS auth mode")
dbutils.widgets.text("secret_scope",    "adls",                 "Secret scope (for SP / key auth)")

account     = dbutils.widgets.get("storage_account").strip()
container   = dbutils.widgets.get("container").strip()
key_cols    = [c.strip() for c in dbutils.widgets.get("key_cols").split(",") if c.strip()]
order_col   = dbutils.widgets.get("order_col").strip() or None
part_cols   = [c.strip() for c in dbutils.widgets.get("partition_cols").split(",") if c.strip()]
auth_mode   = dbutils.widgets.get("auth_mode")
scope       = dbutils.widgets.get("secret_scope").strip()

base        = f"abfss://{container}@{account}.dfs.core.windows.net"
source_path = f"{base}/{dbutils.widgets.get('source_path').strip('/')}/"
target_path = f"{base}/{dbutils.widgets.get('target_path').strip('/')}"

print("Source :", source_path)
print("Target :", target_path)
print("Keys   :", key_cols, "| Recency column:", order_col, "| Partitions:", part_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ADLS authentication
# MAGIC - **`unity_catalog`** (recommended): access is granted through a Unity Catalog *external location* / storage credential. Nothing to configure here.
# MAGIC - **`service_principal`**: OAuth with a Microsoft Entra ID app. Needs secrets `sp-client-id`, `sp-client-secret`, `sp-tenant-id` in the scope, and the SP needs *Storage Blob Data Contributor* on the account.
# MAGIC - **`account_key`**: storage account key from secret `storage-account-key` (simplest, least secure).
# MAGIC
# MAGIC The last two set Spark session configs, which are **not permitted on serverless or standard/shared access mode** compute. Use Unity Catalog there.

# COMMAND ----------

host = f"{account}.dfs.core.windows.net"

if auth_mode == "service_principal":
    client_id     = dbutils.secrets.get(scope, "sp-client-id")
    client_secret = dbutils.secrets.get(scope, "sp-client-secret")
    tenant_id     = dbutils.secrets.get(scope, "sp-tenant-id")

    spark.conf.set(f"fs.azure.account.auth.type.{host}", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{host}",
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{host}", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{host}", client_secret)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{host}",
                   f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

elif auth_mode == "account_key":
    spark.conf.set(f"fs.azure.account.key.{host}", dbutils.secrets.get(scope, "storage-account-key"))

# unity_catalog: nothing to do

# Let MERGE add new columns from the source automatically (set to "false" to enforce a strict schema)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ### (Optional) Create demo CSV data
# MAGIC Writes small sample files so you can watch the upsert work end to end.
# MAGIC Run with `batch=1`, run sections 2–5, then run with `batch=2` and re-run sections 2–5.
# MAGIC Batch 2 contains an update (id 2), a new row (id 4), and a stale row (id 1, older `updated_at`) that must **not** overwrite the newer one.
# MAGIC Set `DEMO_BATCH = 0` to skip.

# COMMAND ----------

DEMO_BATCH = 0   # 0 = off, 1 = first load, 2 = incremental changes

demo_files = {
    1: ("batch1.csv",
        "customer_id,name,email,country,updated_at\n"
        "1,Alice,alice@example.com,US,2025-01-01T10:00:00\n"
        "2,Bob,bob@example.com,UK,2025-01-01T10:00:00\n"
        "3,Carol,carol@example.com,DE,2025-01-01T10:00:00\n"),
    2: ("batch2.csv",
        "customer_id,name,email,country,updated_at\n"
        "2,Bob Smith,bob.smith@example.com,UK,2025-02-01T09:00:00\n"
        "4,Dan,dan@example.com,FR,2025-02-01T09:00:00\n"
        "1,Alice OLD,old@example.com,US,2024-12-01T08:00:00\n"),
}

if DEMO_BATCH in demo_files:
    fname, content = demo_files[DEMO_BATCH]
    dbutils.fs.put(f"{source_path}{fname}", content, True)
    print(f"Wrote demo file {source_path}{fname}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read CSV from ADLS
# MAGIC Schema inference is convenient but does an extra pass over the data and can guess wrong.
# MAGIC For production, define the schema explicitly (example below) and pass it with `.schema(...)`.

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

# Explicit schema example, use instead of inferSchema for production:
# csv_schema = StructType([
#     StructField("customer_id", IntegerType(),   False),
#     StructField("name",        StringType(),    True),
#     StructField("email",       StringType(),    True),
#     StructField("country",     StringType(),    True),
#     StructField("updated_at",  TimestampType(), True),
# ])

def read_csv_batch(path: str):
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")          # replace with .schema(csv_schema) for production
        .option("mode", "PERMISSIVE")           # keep going on malformed rows
        .option("multiLine", "false")           # set true if fields contain embedded newlines
        .option("encoding", "UTF-8")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .load(path)
        .withColumn("_source_file", F.col("_metadata.file_path"))   # lineage
        .withColumn("_ingested_at", F.current_timestamp())
    )

df_raw = read_csv_batch(source_path)

df_raw.printSchema()
print("Rows read:", df_raw.count())
display(df_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Define the upsert logic
# MAGIC `upsert_to_delta` is reusable: it works on a plain DataFrame here and as a `foreachBatch` handler with Auto Loader (section 6).
# MAGIC
# MAGIC - Drops rows with null keys (they can never match)
# MAGIC - Keeps only the **latest** row per key within the incoming batch (a MERGE fails if multiple source rows match one target row)
# MAGIC - First run: creates the target. Later runs: `MERGE`
# MAGIC - Updates only when the incoming row is **newer** than the stored one (safe against late or replayed files)

# COMMAND ----------

from delta.tables import DeltaTable

def dedupe_latest(df, keys, order_column):
    """Keep one row per key: the one with the greatest order_column (or arbitrary if none)."""
    df = df.dropna(subset=keys)
    if order_column:
        w = Window.partitionBy(*keys).orderBy(F.col(order_column).desc_nulls_last(), F.col("_ingested_at").desc())
        return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    return df.dropDuplicates(keys)


def upsert_to_delta(batch_df, batch_id=None):
    src = dedupe_latest(batch_df, key_cols, order_col)

    # First load: create the target
    if not DeltaTable.isDeltaTable(spark, target_path):
        writer = src.write.format("delta").mode("overwrite")
        if part_cols:
            writer = writer.partitionBy(*part_cols)
        writer.save(target_path)
        print(f"[batch {batch_id}] Target did not exist, created it with {src.count()} rows")
        return

    tgt = DeltaTable.forPath(spark, target_path)

    merge_cond = " AND ".join(f"tgt.`{k}` = src.`{k}`" for k in key_cols)
    update_cond = (
        f"src.`{order_col}` > tgt.`{order_col}` OR tgt.`{order_col}` IS NULL"
        if order_col else None
    )

    (
        tgt.alias("tgt")
        .merge(src.alias("src"), merge_cond)
        .whenMatchedUpdateAll(condition=update_cond)   # update only if newer
        .whenNotMatchedInsertAll()                     # insert new keys
        .execute()
    )
    print(f"[batch {batch_id}] MERGE complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run the upsert

# COMMAND ----------

upsert_to_delta(df_raw, batch_id="manual")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify

# COMMAND ----------

df_target = spark.read.format("delta").load(target_path)

print("Rows in target:", df_target.count())
display(df_target.orderBy(*key_cols))

# Key uniqueness check (should return no rows)
dupes = df_target.groupBy(*key_cols).count().filter("count > 1")
print("Duplicate keys:", dupes.count())

# What did the last operation do? (rows inserted / updated, files written)
history = DeltaTable.forPath(spark, target_path).history(5)
display(history.select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. (Optional) Incremental ingestion with Auto Loader
# MAGIC Section 2 re-reads every file in the folder each run. Auto Loader tracks which files it has already processed and only reads new ones.
# MAGIC It reuses `upsert_to_delta` via `foreachBatch`. Set `USE_AUTOLOADER = True` to run.
# MAGIC Use this instead of sections 2 and 4, not in addition to them.

# COMMAND ----------

USE_AUTOLOADER = False

if USE_AUTOLOADER:
    checkpoint_path = f"{base}/_checkpoints/{dbutils.widgets.get('target_path').strip('/')}"
    schema_path     = f"{base}/_schemas/{dbutils.widgets.get('target_path').strip('/')}"

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", schema_path)
        .load(source_path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    (
        stream.writeStream
        .foreachBatch(upsert_to_delta)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)          # process everything new, then stop (good for scheduled jobs)
        .start()
        .awaitTermination()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. (Optional) Upsert into a *plain* Parquet folder (no Delta)
# MAGIC Only use this if consumers cannot read Delta. It works by rebuilding the dataset: keep existing rows whose key is **not** in the
# MAGIC incoming batch (plus stale-row protection), add the incoming rows, write to a temporary folder, then swap it into place.
# MAGIC
# MAGIC **Trade-offs:** rewrites the whole dataset, is **not atomic** (readers can see a missing/partial folder during the swap),
# MAGIC has no concurrency control, no time travel, and no rollback. Prefer Delta whenever possible.
# MAGIC Set `RUN_PLAIN_PARQUET_UPSERT = True` to run.

# COMMAND ----------

RUN_PLAIN_PARQUET_UPSERT = False

def upsert_plain_parquet(src_df, parquet_path: str):
    src = dedupe_latest(src_df, key_cols, order_col)
    tmp_path = parquet_path.rstrip("/") + "__tmp"

    try:
        existing = spark.read.parquet(parquet_path)
    except Exception:
        # First load
        src.write.mode("overwrite").parquet(parquet_path)
        return

    if order_col:
        # Existing rows are replaced only when the incoming row is newer (or existing is null)
        key_match = [existing[k] == src[k] for k in key_cols]
        is_newer = (src[order_col] > existing[order_col]) | existing[order_col].isNull()
        matched = src.join(existing, key_match, "inner")
        replaced_keys = matched.filter(is_newer).select(*[existing[k] for k in key_cols])
        stale_keys = matched.filter(~is_newer).select(*[src[k] for k in key_cols])
        src_effective = src.join(stale_keys, key_cols, "left_anti")         # drop stale incoming rows
        keep_existing = existing.join(replaced_keys, key_cols, "left_anti") # drop rows being replaced
    else:
        src_effective = src
        keep_existing = existing.join(src.select(*key_cols), key_cols, "left_anti")

    merged = keep_existing.unionByName(src_effective, allowMissingColumns=True)

    # Spark can't overwrite a path it is reading from, so write to tmp first, then swap
    merged.write.mode("overwrite").parquet(tmp_path)
    dbutils.fs.rm(parquet_path, True)
    dbutils.fs.mv(tmp_path, parquet_path, True)

if RUN_PLAIN_PARQUET_UPSERT:
    plain_parquet_path = target_path + "_plain_parquet"
    upsert_plain_parquet(df_raw, plain_parquet_path)
    display(spark.read.parquet(plain_parquet_path).orderBy(*key_cols))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. (Optional) Export a plain-Parquet snapshot of the Delta table
# MAGIC The best of both worlds: do upserts in Delta, then publish a plain Parquet copy for tools that can't read Delta.
# MAGIC (The Delta table's own files are also Parquet, but include old versions and shouldn't be read directly.)

# COMMAND ----------

RUN_EXPORT_SNAPSHOT = False

if RUN_EXPORT_SNAPSHOT:
    export_path = f"{base}/export/{dbutils.widgets.get('target_path').strip('/')}_parquet"
    (
        spark.read.format("delta").load(target_path)
        .drop("_source_file", "_ingested_at")     # drop lineage columns if consumers don't need them
        .write.mode("overwrite")
        .parquet(export_path)
    )
    print("Exported snapshot to", export_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes
# MAGIC - **Unity Catalog tables:** to register the target as a table instead of a path, use `df.write.format("delta").saveAsTable("catalog.schema.customers")` and `DeltaTable.forName(spark, "catalog.schema.customers")`.
# MAGIC - **Partitioning:** partition only large tables (roughly > 1 TB) on low-cardinality columns. For tables that are smaller, or where you filter on high-cardinality columns, consider liquid clustering or `OPTIMIZE ... ZORDER BY` instead. If you do partition, include the partition column in the merge condition so MERGE can prune files.
# MAGIC - **Deletes:** add `.whenMatchedDelete(condition="src.is_deleted = true")` before `whenMatchedUpdateAll` if your feed marks deleted rows.
# MAGIC - **Maintenance:** run `OPTIMIZE` periodically and `VACUUM` to clean up old files after many merges.
# MAGIC - **Bad records:** for strict validation, set `.option("mode", "FAILFAST")`, or use `badRecordsPath` to quarantine malformed rows.
