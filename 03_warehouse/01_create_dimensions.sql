-- =========================
-- DIMENSIONS (Warehouse-safe)
-- =========================

-- Dim Plant
IF OBJECT_ID('dbo.dim_plant') IS NOT NULL DROP TABLE dbo.dim_plant;

CREATE TABLE dbo.dim_plant AS
SELECT DISTINCT
    CAST(PlantID AS int) AS PlantID,
    CAST(PlantName AS varchar(100)) AS PlantName,
    CAST(Location AS varchar(100)) AS Location
FROM manufacturing_lakehouse.dbo.silver_plants;

-- Dim Product
IF OBJECT_ID('dbo.dim_product') IS NOT NULL DROP TABLE dbo.dim_product;

CREATE TABLE dbo.dim_product AS
SELECT DISTINCT
    CAST(ProductID AS int) AS ProductID,
    CAST(ProductName AS varchar(100)) AS ProductName,
    CAST(Category AS varchar(100)) AS Category
FROM manufacturing_lakehouse.dbo.silver_products;

-- Dim Date (numeric attributes only)
IF OBJECT_ID('dbo.dim_date') IS NOT NULL DROP TABLE dbo.dim_date;

CREATE TABLE dbo.dim_date AS
WITH dates AS (
    SELECT DISTINCT CAST([date] AS date) AS [date]
    FROM manufacturing_lakehouse.dbo.gold_production_daily
    UNION
    SELECT DISTINCT CAST([date] AS date) AS [date]
    FROM manufacturing_lakehouse.dbo.gold_quality_daily
    UNION
    SELECT DISTINCT CAST([date] AS date) AS [date]
    FROM manufacturing_lakehouse.dbo.gold_inventory_snapshot
)
SELECT
    [date],
    YEAR([date]) AS [year],
    MONTH([date]) AS [month],
    DAY([date]) AS [day],
    DATEPART(weekday, [date]) AS weekday_num
FROM dates;