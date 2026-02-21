"""
03_gold_curated.py
Purpose: Create curated Gold tables for analytics and warehouse loading.
Creates: gold_production_daily, gold_quality_daily, gold_inventory_snapshot
"""

##---

from pyspark.sql.functions import col, sum as _sum, to_date

wo = spark.table("silver_workorders")
pr = spark.table("silver_productionruns")
plants = spark.table("silver_plants")
products = spark.table("silver_products")

prod_daily = (
    pr.join(wo, "WorkOrderID", "inner")
      .join(plants, "PlantID", "left")
      .join(products, "ProductID", "left")
      .groupBy(
          col("RunDate").alias("date"),
          col("PlantID"), col("PlantName"),
          col("ProductID"), col("ProductName"), col("Category")
      )
      .agg(
          _sum("ActualQuantity").alias("total_actual_qty"),
          _sum("ScrapQuantity").alias("total_scrap_qty")
      )
)

prod_daily.write.mode("overwrite").format("delta").saveAsTable("gold_production_daily")
print("gold_production_daily =", spark.table("gold_production_daily").count())

##---

from pyspark.sql.functions import col, sum as _sum

qi = spark.table("silver_qualityinspections")
pr = spark.table("silver_productionruns")
wo = spark.table("silver_workorders")
plants = spark.table("silver_plants")
products = spark.table("silver_products")

quality_daily = (
    qi.join(pr, "ProductionRunID", "inner")
      .join(wo, "WorkOrderID", "inner")
      .join(plants, "PlantID", "left")
      .join(products, "ProductID", "left")
      .groupBy(
          col("InspectionDate").alias("date"),
          col("PlantID"), col("PlantName"),
          col("ProductID"), col("ProductName"),
          col("DefectType")
      )
      .agg(_sum("DefectCount").alias("total_defects"))
)

quality_daily.write.mode("overwrite").format("delta").saveAsTable("gold_quality_daily")
print("gold_quality_daily =", spark.table("gold_quality_daily").count())

##---

from pyspark.sql.functions import col, sum as _sum

inv = spark.table("silver_inventory")
plants = spark.table("silver_plants")
products = spark.table("silver_products")

inv_snap = (
    inv.join(plants, "PlantID", "left")
       .join(products, "ProductID", "left")
       .groupBy(
           col("LastUpdated").alias("date"),
           col("PlantID"), col("PlantName"),
           col("ProductID"), col("ProductName"), col("Category")
       )
       .agg(_sum("QuantityOnHand").alias("qty_on_hand"))
)

inv_snap.write.mode("overwrite").format("delta").saveAsTable("gold_inventory_snapshot")
print("gold_inventory_snapshot =", spark.table("gold_inventory_snapshot").count())

##---