CREATE OR ALTER PROCEDURE dbo.usp_UpdateWatermark
(
    @ConfigID INT,
    @CurrentWatermark DATETIME2(7)
)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE dbo.ETL_Watermark
    SET
        LastWatermark = @CurrentWatermark,
        CurrentWatermark = @CurrentWatermark,
        LastSuccessfulRun = SYSUTCDATETIME(),
        LastRunStatus = 'SUCCESS',
        ModifiedDate = SYSUTCDATETIME()
    WHERE ConfigID = @ConfigID;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_MarkFailure
(
    @ConfigID INT
)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE dbo.ETL_Watermark
    SET
        LastRunStatus = 'FAILED',
        ModifiedDate = SYSUTCDATETIME()
    WHERE ConfigID = @ConfigID;
END;
GO
