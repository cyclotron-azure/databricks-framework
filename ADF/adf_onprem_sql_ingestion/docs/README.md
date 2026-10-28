# ADF On-Prem SQL Server -> ADLS Gen2 Configuration-Driven Ingestion

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
- LS_OnPrem_SQLServer
- LS_ADLS_Gen2

Datasets:
- DS_ETL_Config
- DS_SQLServer_Source
- DS_ADLS_Raw

Pipeline:
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
