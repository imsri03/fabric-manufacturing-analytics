-- Production KPI view (Actual vs Scrap)
IF OBJECT_ID('dbo.vw_production_kpi_daily') IS NOT NULL DROP VIEW dbo.vw_production_kpi_daily;
GO
CREATE VIEW dbo.vw_production_kpi_daily AS
SELECT
    f.[date],
    d.[year],
    d.[month],
    f.PlantID,
    p.PlantName,
    f.ProductID,
    pr.ProductName,
    pr.Category,
    f.total_actual_qty,
    f.total_scrap_qty,
    CASE WHEN f.total_actual_qty = 0 THEN 0
         ELSE CAST(f.total_scrap_qty AS float) / CAST(f.total_actual_qty AS float)
    END AS scrap_rate
FROM dbo.fact_production_daily f
LEFT JOIN dbo.dim_date d ON f.[date] = d.[date]
LEFT JOIN dbo.dim_plant p ON f.PlantID = p.PlantID
LEFT JOIN dbo.dim_product pr ON f.ProductID = pr.ProductID;

SELECT TOP 10 * FROM dbo.vw_production_kpi_daily ORDER BY [date] DESC;