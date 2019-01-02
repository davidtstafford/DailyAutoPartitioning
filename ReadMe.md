For full details hop over to my blog https://www.sql-cubed.com/loading-auto-generated-daily-partition-in-sql-server




```sql
BEGIN TRY DROP TABLE Staging.MainTable__SwitchIN END TRY BEGIN CATCH END CATCH 
GO
BEGIN TRY DROP TABLE Staging.MainTable__SwitchOUT END TRY BEGIN CATCH END CATCH 
GO
BEGIN TRY DROP TABLE dbo.MainTable END TRY BEGIN CATCH END CATCH 
GO
BEGIN TRY DROP VIEW MetaData.vw_PartitionDetails END TRY BEGIN CATCH END CATCH 
GO
BEGIN TRY DROP PROC dbo.sp_PrepareMainTablePartititions END TRY BEGIN CATCH END CATCH 
GO
BEGIN TRY DROP PROC dbo.sp_LoadMainTable END TRY BEGIN CATCH END CATCH 
GO
BEGIN TRY DROP PARTITION SCHEME psRunDates END TRY BEGIN CATCH END CATCH 
GO
BEGIN TRY DROP PARTITION FUNCTION pfRunDates END TRY BEGIN CATCH END CATCH 
GO
BEGIN TRY DROP SCHEMA [Staging] END TRY BEGIN CATCH END CATCH 
GO
BEGIN TRY DROP SCHEMA [MetaData] END TRY BEGIN CATCH END CATCH 
GO




CREATE SCHEMA [Staging]
GO

CREATE SCHEMA [MetaData]
GO


CREATE PARTITION FUNCTION pfRunDates (DATE)
AS RANGE RIGHT FOR VALUES ('2019-01-01')
GO

CREATE PARTITION SCHEME psRunDates
AS PARTITION pfRunDates
TO ([Primary],[Primary],[Primary])
GO

CREATE TABLE dbo.MainTable
(
      ID            INT
    , UserName      VARCHAR(100)
    , RunDate       DATE
) ON psRunDates(RunDate)
GO
CREATE CLUSTERED INDEX CIX__MainTable_ID ON dbo.MainTable(ID)
GO
CREATE NONCLUSTERED INDEX NCIX__MainTable_RunDate ON dbo.MainTable(RunDate)
GO

CREATE TABLE Staging.MainTable__SwitchIN
(
      ID            INT
    , UserName      VARCHAR(100)
    , RunDate       DATE
) ON psRunDates(RunDate)
GO
CREATE CLUSTERED INDEX CIX__MainTable__SwitchIN_ID ON Staging.MainTable__SwitchIN(ID)
GO
CREATE NONCLUSTERED INDEX NCIX__MainTable__SwitchIN_RunDate ON Staging.MainTable__SwitchIN(RunDate)
GO

CREATE TABLE Staging.MainTable__SwitchOUT
(
      ID            INT
    , UserName      VARCHAR(100)
    , RunDate       DATE
) ON [Primary]
GO
-- Note this index does not look identical, but RunDate is part of the index 
-- to equal RunDate being the partition key in the partitioned table
CREATE CLUSTERED INDEX CIX__MainTable__SwitchOUT_ID 
    ON Staging.MainTable__SwitchOUT(ID,RunDate)
GO
CREATE NONCLUSTERED INDEX NCIX__MainTable__SwitchOUT_RunDate 
    ON Staging.MainTable__SwitchOUT(RunDate)
GO

CREATE VIEW MetaData.vw_PartitionDetails
AS
    SELECT 
          FunctionName      =   pf.name
        , SchemeName        =   ps.name
        , TableName         =   o.name
        , PartitionNumber   =   stat.partition_number
        , [RowCount]        =   stat.row_count
        , RangeValue        =   prv.value
    FROM            sys.partition_functions     pf

        INNER JOIN  sys.partition_schemes       ps  ON  ps.function_id  =   pf.function_id

        INNER JOIN  sys.indexes                 i   ON  i.data_space_id =   ps.data_space_id
                                                    AND i.[type]        =   1 -- Limit to clustered index

        INNER JOIN  sys.partitions              p   ON  i.object_id     =   p.object_id 
                                                    AND p.index_id      =   1 -- Limit to first index

        INNER JOIN  sys.objects                 o   ON  o.object_id     =   i.object_id
                                                    AND o.[type]        =   'U'

        LEFT  JOIN sys.partition_range_values   prv ON prv.function_id      =   pf.function_id
                                                    AND p.partition_number  =   CASE pf.boundary_value_on_right 
                                                                                    WHEN 1 THEN prv.boundary_id + 1
                                                                                    ELSE prv.boundary_id
                                                                                END

        INNER JOIN sys.dm_db_partition_stats    stat    ON  stat.object_id          =   p.object_id
                                                        AND stat.index_id           =   p.index_id
                                                        AND stat.partition_id       =   p.partition_id
                                                        AND stat.partition_number   =   p.partition_number
GO


CREATE PROCEDURE dbo.sp_PrepareMainTablePartititions
(
      @PartitionDate DATE
    , @OverwriteData BIT = 1 -- 1 = Get rid of previous data.. 0 = Thrown error if previous data found
)
AS
BEGIN

    DECLARE
          @PartitionNumber INT
        , @ErrorMessage NVARCHAR(4000) 
    ;

    SELECT @PartitionNumber = PartitionNumber
    FROM MetaData.vw_PartitionDetails
    WHERE TableName = 'MainTable'
    AND RangeValue = @PartitionDate;

    IF @OverwriteData = 0 AND @PartitionNumber IS NOT NULL
    BEGIN
        SET @ErrorMessage = 'Partition for this date already exists.  Please purge any data and remove the partition, or recall the proc with OverwriteData = 1';
        RAISERROR(@ErrorMessage,16,1);
        RETURN;
    END

    IF @PartitionNumber IS NULL
    BEGIN
        ALTER PARTITION FUNCTION pfRunDates()
            SPLIT RANGE (@PartitionDate);

        ALTER PARTITION SCHEME psRunDates
            NEXT USED [PRIMARY];

        SELECT @PartitionNumber = PartitionNumber
        FROM MetaData.vw_PartitionDetails
        WHERE TableName = 'MainTable'
        AND RangeValue = @PartitionDate;

        
    END


    TRUNCATE TABLE Staging.MainTable__SwitchOUT;

    TRUNCATE TABLE Staging.MainTable__SwitchIN;

    ALTER TABLE dbo.MainTable SWITCH PARTITION @PartitionNumber TO Staging.MainTable__SwitchOUT;

END
GO



CREATE PROCEDURE dbo.sp_LoadMainTable
(
      @PartitionDate DATE
)
AS
BEGIN

    DECLARE
          @PartitionNumber INT
        , @ErrorMessage NVARCHAR(4000) 
    ;

    IF EXISTS ( SELECT 1 FROM dbo.MainTable WHERE RunDate = @PartitionDate)
    BEGIN
        SET @ErrorMessage = 'Data has already been loaded for the date.  Pplease purge it first';
        RAISERROR(@ErrorMessage,16,1);
        RETURN;
    END

    SELECT @PartitionNumber = PartitionNumber
    FROM MetaData.vw_PartitionDetails
    WHERE TableName = 'MainTable'
    AND RangeValue = @PartitionDate;

    IF @PartitionNumber IS NULL
    BEGIN
        SET @ErrorMessage = 'Partition has not been created.  Run the proc "dbo.sp_PrepareMainTablePartititions" first';
        RAISERROR(@ErrorMessage,16,1);
        RETURN;
    END   

    ALTER TABLE Staging.MainTable__SwitchIn SWITCH PARTITION @PartitionNumber 
        TO dbo.MainTable PARTITION @PartitionNumber;

END
GO


EXEC dbo.sp_PrepareMainTablePartititions '2019-01-02',1 ;

INSERT INTO dbo.MainTable
(ID, UserName, RunDate)
VALUES
      (1,'Bob','2019-01-01')
    , (2, 'Jane','2019-01-01')
;


INSERT INTO Staging.MainTable__SwitchIN
( ID, UserName, RunDate)
VALUES
      (3,'Vicky','2019-01-02')
    , (4, 'Eric','2019-01-02')
    , (5, 'Scott', '2019-01-02')
;


EXEC dbo.sp_LoadMainTable '2019-01-02' ;

SELECT *
FROM MetaData.vw_PartitionDetails ;

SELECT * 
FROM dbo.MainTable ;
