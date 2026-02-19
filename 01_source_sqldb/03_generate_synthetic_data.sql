/*
03_generate_synthetic_data.sql
Generates synthetic manufacturing operational data at realistic volume.
Safe to run once. It appends NEW rows using new IDs (no collisions).

Targets (edit these if you want):
- WorkOrders: 1200
- ProductionRuns: 1800
- QualityInspections: 2500
- Inventory snapshots: 1200
*/

SET NOCOUNT ON;

DECLARE @WorkOrdersToAdd INT = 1200;
DECLARE @RunsToAdd INT = 1800;
DECLARE @InspectionsToAdd INT = 2500;
DECLARE @InventoryToAdd INT = 1200;

-- Date range for realism (last 90 days)
DECLARE @DaysBack INT = 90;

-- Helper: current max IDs so we can append without collisions
DECLARE @MaxWorkOrderID INT = ISNULL((SELECT MAX(WorkOrderID) FROM WorkOrders), 1000);
DECLARE @MaxRunID INT = ISNULL((SELECT MAX(ProductionRunID) FROM ProductionRuns), 5000);
DECLARE @MaxInspectionID INT = ISNULL((SELECT MAX(InspectionID) FROM QualityInspections), 9000);
DECLARE @MaxInventoryID INT = ISNULL((SELECT MAX(InventoryID) FROM Inventory), 7000);

-- Create a reusable numbers table
IF OBJECT_ID('tempdb..#N') IS NOT NULL DROP TABLE #N;
CREATE TABLE #N (n INT NOT NULL PRIMARY KEY);

INSERT INTO #N(n)
SELECT TOP (10000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
FROM sys.all_objects a
CROSS JOIN sys.all_objects b;

------------------------------------------------------------
-- 1) WORKORDERS
------------------------------------------------------------
INSERT INTO WorkOrders (WorkOrderID, ProductID, PlantID, PlannedQuantity, PlannedStartDate, PlannedEndDate)
SELECT
    @MaxWorkOrderID + n.n AS WorkOrderID,
    p.ProductID,
    pl.PlantID,
    CASE
        WHEN (ABS(CHECKSUM(NEWID())) % 100) < 1 THEN NULL
        ELSE 500 + (ABS(CHECKSUM(NEWID())) % 2500)
    END AS PlannedQuantity,
    CAST(DATEADD(DAY, -(ABS(CHECKSUM(NEWID())) % @DaysBack), GETDATE()) AS DATE) AS PlannedStartDate,
    CAST(DATEADD(DAY, 1 + (ABS(CHECKSUM(NEWID())) % 7),
         DATEADD(DAY, -(ABS(CHECKSUM(NEWID())) % @DaysBack), GETDATE())) AS DATE) AS PlannedEndDate
FROM #N n
CROSS APPLY (SELECT TOP 1 ProductID FROM Products ORDER BY NEWID()) p
CROSS APPLY (SELECT TOP 1 PlantID FROM Plants ORDER BY NEWID()) pl
WHERE n.n <= @WorkOrdersToAdd;

------------------------------------------------------------
-- 2) PRODUCTIONRUNS
------------------------------------------------------------
INSERT INTO ProductionRuns (ProductionRunID, WorkOrderID, ActualQuantity, ScrapQuantity, RunDate)
SELECT
    @MaxRunID + n.n AS ProductionRunID,
    wo.WorkOrderID,
    400 + (ABS(CHECKSUM(NEWID())) % 2600) AS ActualQuantity,
    CASE
        WHEN (ABS(CHECKSUM(NEWID())) % 100) < 1 THEN -1 * (1 + (ABS(CHECKSUM(NEWID())) % 10))
        ELSE (ABS(CHECKSUM(NEWID())) % 60)
    END AS ScrapQuantity,
    CAST(DATEADD(DAY, -(ABS(CHECKSUM(NEWID())) % @DaysBack), GETDATE()) AS DATE) AS RunDate
FROM #N n
CROSS APPLY (SELECT TOP 1 WorkOrderID FROM WorkOrders ORDER BY NEWID()) wo
WHERE n.n <= @RunsToAdd;

------------------------------------------------------------
-- 3) QUALITYINSPECTIONS
------------------------------------------------------------
;WITH DefectTypes AS (
    SELECT v.DefectType
    FROM (VALUES
        ('Scratch'),
        ('Leak'),
        ('Crack'),
        ('Discolor'),
        ('Misalign'),
        ('Contamination'),
        ('SealFailure'),
        ('Warp')
    ) v(DefectType)
)
INSERT INTO QualityInspections (InspectionID, ProductionRunID, DefectType, DefectCount, InspectionDate)
SELECT
    @MaxInspectionID + n.n AS InspectionID,
    rp.ProductionRunID,
    dt.DefectType,
    CASE
        WHEN (ABS(CHECKSUM(NEWID())) % 100) < 1 THEN NULL
        ELSE (ABS(CHECKSUM(NEWID())) % 12)
    END AS DefectCount,
    CAST(DATEADD(DAY, -(ABS(CHECKSUM(NEWID())) % @DaysBack), GETDATE()) AS DATE) AS InspectionDate
FROM #N n
CROSS APPLY (SELECT TOP 1 ProductionRunID FROM ProductionRuns ORDER BY NEWID()) rp
CROSS APPLY (SELECT TOP 1 DefectType FROM DefectTypes ORDER BY NEWID()) dt
WHERE n.n <= @InspectionsToAdd;

------------------------------------------------------------
-- 4) INVENTORY SNAPSHOTS
------------------------------------------------------------
INSERT INTO Inventory (InventoryID, ProductID, PlantID, QuantityOnHand, LastUpdated)
SELECT
    @MaxInventoryID + n.n AS InventoryID,
    pp.ProductID,
    plp.PlantID,
    CASE
        WHEN (ABS(CHECKSUM(NEWID())) % 100) < 1 THEN -1 * (1 + (ABS(CHECKSUM(NEWID())) % 50))
        ELSE (ABS(CHECKSUM(NEWID())) % 5000)
    END AS QuantityOnHand,
    CAST(DATEADD(DAY, -(ABS(CHECKSUM(NEWID())) % @DaysBack), GETDATE()) AS DATE) AS LastUpdated
FROM #N n
CROSS APPLY (SELECT TOP 1 ProductID FROM Products ORDER BY NEWID()) pp
CROSS APPLY (SELECT TOP 1 PlantID FROM Plants ORDER BY NEWID()) plp
WHERE n.n <= @InventoryToAdd;

------------------------------------------------------------
-- VERIFY COUNTS
------------------------------------------------------------
SELECT 'Plants' AS TableName, COUNT(*) AS TotalRows FROM Plants
UNION ALL SELECT 'Products', COUNT(*) FROM Products
UNION ALL SELECT 'Suppliers', COUNT(*) FROM Suppliers
UNION ALL SELECT 'WorkOrders', COUNT(*) FROM WorkOrders
UNION ALL SELECT 'ProductionRuns', COUNT(*) FROM ProductionRuns
UNION ALL SELECT 'QualityInspections', COUNT(*) FROM QualityInspections
UNION ALL SELECT 'Inventory', COUNT(*) FROM Inventory;

DROP TABLE #N;
