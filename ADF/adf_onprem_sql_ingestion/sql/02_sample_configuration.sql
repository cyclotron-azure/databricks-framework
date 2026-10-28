INSERT INTO dbo.ETL_Config
(
    SourceSystem, SourceSchema, SourceTable,
    TargetContainer, TargetFolder, LoadType, WatermarkColumn
)
VALUES
('ECommerce', 'dbo', 'Customers', 'raw', 'sqlserver/customers', 'FULL', NULL),
('ECommerce', 'dbo', 'Products',  'raw', 'sqlserver/products',  'FULL', NULL),
('ECommerce', 'dbo', 'Orders',    'raw', 'sqlserver/orders',    'INCREMENTAL', 'ModifiedDate');

INSERT INTO dbo.ETL_Watermark
(
    ConfigID, LastWatermark, LastRunStatus
)
SELECT ConfigID, NULL, 'NOT_STARTED'
FROM dbo.ETL_Config
WHERE IsActive = 1;
