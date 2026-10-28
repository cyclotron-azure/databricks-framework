CREATE TABLE dbo.ETL_Config
(
    ConfigID            INT IDENTITY(1,1) PRIMARY KEY,
    SourceSystem        VARCHAR(100) NOT NULL,
    SourceSchema        VARCHAR(100) NOT NULL,
    SourceTable         VARCHAR(200) NOT NULL,
    TargetContainer     VARCHAR(100) NOT NULL,
    TargetFolder        VARCHAR(500) NOT NULL,
    LoadType            VARCHAR(20) NOT NULL,
    WatermarkColumn     VARCHAR(200) NULL,
    IsActive             BIT NOT NULL DEFAULT 1,
    CreatedDate          DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate         DATETIME2(7) NULL
);

CREATE TABLE dbo.ETL_Watermark
(
    ConfigID             INT PRIMARY KEY,
    LastWatermark        DATETIME2(7) NULL,
    CurrentWatermark     DATETIME2(7) NULL,
    LastSuccessfulRun    DATETIME2(7) NULL,
    LastRunStatus        VARCHAR(30) NULL,
    ModifiedDate         DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE dbo.ETL_Audit
(
    AuditID              BIGINT IDENTITY(1,1) PRIMARY KEY,
    ConfigID             INT,
    PipelineName         VARCHAR(200),
    SourceTable          VARCHAR(300),
    RunID                VARCHAR(100),
    StartTime             DATETIME2(7),
    EndTime               DATETIME2(7),
    RowsCopied            BIGINT NULL,
    LastWatermark         DATETIME2(7) NULL,
    CurrentWatermark      DATETIME2(7) NULL,
    Status                 VARCHAR(30),
    ErrorMessage           VARCHAR(MAX)
);
