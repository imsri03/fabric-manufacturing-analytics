-- =========================
-- FACTS
-- =========================

-- Fact: Production Daily
IF OBJECT_ID('dbo.fact_production_daily') IS NOT NULL DROP TABLE dbo.fact_production_daily;

CREATE TABLE dbo.fact_production_daily AS
SELECT
    CAST([date] AS date) AS [date],
    CAST(PlantID AS int) AS PlantID,
    CAST(ProductID AS int) AS ProductID,
    CAST(total_actual_qty AS bigint) AS total_actual_qty,
    CAST(total_scrap_qty AS bigint) AS total_scrap_qty
FROM manufacturing_lakehouse.dbo.gold_production_daily;

-- Fact: Quality Daily
IF OBJECT_ID('dbo.fact_quality_daily') IS NOT NULL DROP TABLE dbo.fact_quality_daily;

CREATE TABLE dbo.fact_quality_daily AS
SELECT
    CAST([date] AS date) AS [date],
    CAST(PlantID AS int) AS PlantID,
    CAST(ProductID AS int) AS ProductID,
    CAST(DefectType AS varchar(100)) AS DefectType,
    CAST(total_defects AS bigint) AS total_defects
FROM manufacturing_lakehouse.dbo.gold_quality_daily;

-- Fact: Inventory Snapshot
IF OBJECT_ID('dbo.fact_inventory_snapshot') IS NOT NULL DROP TABLE dbo.fact_inventory_snapshot;

CREATE TABLE dbo.fact_inventory_snapshot AS
SELECT
    CAST([date] AS date) AS [date],
    CAST(PlantID AS int) AS PlantID,
    CAST(ProductID AS int) AS ProductID,
    CAST(qty_on_hand AS bigint) AS qty_on_hand
FROM manufacturing_lakehouse.dbo.gold_inventory_snapshot;