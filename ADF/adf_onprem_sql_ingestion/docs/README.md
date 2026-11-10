# ADF On-Prem SQL Server -> ADLS Gen2 Configuration-Driven Ingestion

## Simple full-load implementation

Start with `pipelines/PL_Master_Full_Load.json`. It uses the existing metadata
table and datasets; no watermark or stored procedure is needed for this pipeline.

```text
Lookup_Config (active FULL rows in dbo.ETL_Config)
  -> ForEach_Table (one table at a time)
         -> Copy_Table_To_ADLS (SQL Server table -> Parquet)
```

### Setup and first run

1. In ADF Manage -> Integration runtimes, create `SelfHostedIR` and install it
    on a Windows machine that can reach your SQL Server and ADLS endpoints.
2. Configure the placeholders in `linkedservices/linkedService_AzureKeyVault.json`,
    `linkedService_OnPremSQL.json`, and `linkedService_ADLS.json`. Store
    `onprem-sql-password` and `adls-account-key` in Key Vault. Give the factory's
    managed identity permission to read those secrets. Use managed identity for
    ADLS instead of an account key when your environment supports it.
3. Create the linked services in ADF with the names `LS_AzureKeyVault`,
    `LS_OnPremSQL`, and `LS_ADLSGen2`. Test the SQL and storage connections.
    These JSON files are templates, not a complete ARM deployment package.
4. Run `sql/01_create_control_tables.sql` once in the SQL database configured
    by `LS_OnPremSQL`. The simple pipeline uses only `dbo.ETL_Config`.
5. Add one metadata row using the example below. Replace `Customers` with an
    existing table and create the `raw` container in ADLS. The SQL login needs
    SELECT permission on the metadata table and configured source tables.
6. In ADF Author, create the SQL table datasets `DS_ETL_Config` and
    `DS_SQLServer_Source`, and the Parquet dataset `DS_ADLS_Raw`. Use the
    corresponding JSON templates in the `datasets` folder for their parameters
    and dynamic paths.
7. Create a pipeline named `PL_Master_Full_Load`, open its Code view and use
    the contents of `pipelines/PL_Master_Full_Load.json`.
    Validate All, then Debug. Verify Copy activity row counts and the ADLS file
    before publishing and adding a schedule trigger.

```sql
INSERT INTO dbo.ETL_Config
(
     SourceSystem, SourceSchema, SourceTable,
     TargetContainer, TargetFolder, LoadType, WatermarkColumn
)
VALUES
('OnPremSQL', 'dbo', 'Customers',
 'raw', 'sqlserver/customers', 'FULL', NULL);
```

Add another row for each additional source table. Do not insert duplicate rows
for the same destination. Set `IsActive = 0` to stop copying a table.
If using `sql/02_sample_configuration.sql` instead, it must be run once and its
source tables must exist: Customers and Products are full loads; Orders is
incremental and is deliberately skipped by this simple pipeline.

Example output:

```text
raw/sqlserver/customers/run_id=<ADF-run-id>/Customers.parquet
```

Each run is a complete snapshot in a separate folder, not an incremental merge.
Read a single successful run's folder downstream, not every historical folder,
to avoid duplicate rows. A failed run may leave partial output; do not consume
its folder. Retries reuse the same destination file within the run.
Pipeline concurrency is one and tables are copied sequentially for simplicity.
This version does not capture deletes incrementally, maintain watermarks, or
write audit records. ADF Monitor provides execution status and copy counts.

## Architecture

On-prem SQL Server
    |
    v
Self-hosted Integration Runtime
    |
    v
Azure Data Factory
    |
    +--> Lookup ETL_Config
    |
    +--> ForEach table
           |
           +--> Lookup watermark
           +--> Get current watermark
           +--> Copy SQL Server -> ADLS Raw
           +--> Update watermark only after successful copy
    |
    v
ADLS Gen2 Raw
    |
    v
Databricks Bronze / Silver / Gold

## Required ADF resources

Linked Services:
- LS_OnPremSQL
- LS_ADLSGen2
- LS_AzureKeyVault

Datasets:
- DS_ETL_Config
- DS_SQLServer_Source
- DS_ADLS_Raw

Pipeline:
- PL_Master_Full_Load (simple full-load option)
- PL_Master_Ingestion

## Important

The JSON files are deployment templates. Resource IDs, linked-service names,
integration-runtime names, credentials, and storage account details must be
adapted to your ADF environment.

Install the Self-hosted Integration Runtime on a Windows server that can
connect to the on-prem SQL Server.

For production:
- Prefer Managed Identity for ADLS Gen2 where supported.
- Use Key Vault for secrets.
- Restrict configuration-table access.
- Validate table/column metadata before executing dynamic SQL.
- Keep watermark updates after successful copy only.
- Add retry policies and audit/error handling.
- Consider a stored procedure or metadata validation layer for dynamic source queries.

## Folder layout

raw/
  sqlserver/
    customers/
    products/
    orders/

## Incremental logic

Each incremental table has a WatermarkColumn, e.g. ModifiedDate.

The pipeline captures MAX(ModifiedDate) before the copy and reads:

ModifiedDate > LastWatermark
AND ModifiedDate <= CurrentWatermark

After Copy succeeds, LastWatermark is advanced to CurrentWatermark.

This creates a stable upper boundary for the run.
