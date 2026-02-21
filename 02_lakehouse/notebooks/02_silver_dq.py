"""
02_silver_dq.py
Purpose: Create Silver tables with DQ rules + quarantine + dq_issues_log.
Creates: silver_* and silver_*_quarantine + dq_issues_log
"""
## ----
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Drop and recreate dq_issues_log to avoid schema conflicts
spark.sql("DROP TABLE IF EXISTS dq_issues_log")

dq_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("rule_name", StringType(), False),
    StructField("issue_count", IntegerType(), False),
    StructField("logged_at", TimestampType(), False)
])

spark.createDataFrame([], dq_schema) \
    .write.mode("overwrite") \
    .format("delta") \
    .saveAsTable("dq_issues_log")

print("dq_issues_log recreated")

## ---
from pyspark.sql.functions import col, when, current_timestamp

# Read bronze
wo = spark.table("bronze_workorders")

# Clean/cast
wo_clean = wo.select(
    col("WorkOrderID").cast("int").alias("WorkOrderID"),
    col("ProductID").cast("int").alias("ProductID"),
    col("PlantID").cast("int").alias("PlantID"),
    col("PlannedQuantity").cast("int").alias("PlannedQuantity"),
    col("PlannedStartDate").cast("date").alias("PlannedStartDate"),
    col("PlannedEndDate").cast("date").alias("PlannedEndDate"),
    col("ingestion_ts")
)

# DQ rules
rule_qty_null = col("PlannedQuantity").isNull()
rule_qty_nonpositive = col("PlannedQuantity") <= 0
rule_bad_dates = col("PlannedEndDate") < col("PlannedStartDate")

# Quarantine rows
wo_quarantine = wo_clean.where(rule_qty_null | rule_qty_nonpositive | rule_bad_dates) \
    .withColumn(
        "dq_reason",
        when(rule_qty_null, "PlannedQuantity is NULL")
        .when(rule_qty_nonpositive, "PlannedQuantity <= 0")
        .when(rule_bad_dates, "PlannedEndDate < PlannedStartDate")
        .otherwise("Unknown")
    ) \
    .withColumn("quarantined_at", current_timestamp())

# Valid rows
wo_silver = wo_clean.where(~(rule_qty_null | rule_qty_nonpositive | rule_bad_dates))

# Write silver + quarantine
wo_silver.write.mode("overwrite").format("delta").saveAsTable("silver_workorders")
wo_quarantine.write.mode("overwrite").format("delta").saveAsTable("silver_workorders_quarantine")

print("silver_workorders =", spark.table("silver_workorders").count())
print("silver_workorders_quarantine =", spark.table("silver_workorders_quarantine").count())

## ----

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp

issue_rows = [
    ("silver_workorders", "PlannedQuantity_not_null", int(wo_clean.where(rule_qty_null).count())),
    ("silver_workorders", "PlannedQuantity_positive", int(wo_clean.where(rule_qty_nonpositive & ~rule_qty_null).count())),
    ("silver_workorders", "EndDate_after_StartDate", int(wo_clean.where(rule_bad_dates).count()))
]

dq_insert_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("rule_name", StringType(), False),
    StructField("issue_count", IntegerType(), False)
])

dq_df = spark.createDataFrame(issue_rows, schema=dq_insert_schema) \
    .withColumn("logged_at", current_timestamp())

dq_df.write.mode("append").format("delta").saveAsTable("dq_issues_log")

display(spark.table("dq_issues_log"))

## ----

from pyspark.sql.functions import col, when, current_timestamp

pr = spark.table("bronze_productionruns")

pr_clean = pr.select(
    col("ProductionRunID").cast("int").alias("ProductionRunID"),
    col("WorkOrderID").cast("int").alias("WorkOrderID"),
    col("ActualQuantity").cast("int").alias("ActualQuantity"),
    col("ScrapQuantity").cast("int").alias("ScrapQuantity"),
    col("RunDate").cast("date").alias("RunDate"),
    col("ingestion_ts")
)

# Reference table for RI check
wo_keys = spark.table("silver_workorders").select("WorkOrderID").distinct()

# DQ rules
rule_actual_negative = col("ActualQuantity") < 0
rule_scrap_negative = col("ScrapQuantity") < 0

# WorkOrderID must exist
pr_with_ri = pr_clean.join(wo_keys.withColumnRenamed("WorkOrderID", "WO_OK"),
                           pr_clean.WorkOrderID == col("WO_OK"),
                           "left")

rule_missing_workorder = col("WO_OK").isNull()

# Quarantine
pr_quarantine = pr_with_ri.where(rule_actual_negative | rule_scrap_negative | rule_missing_workorder) \
    .withColumn(
        "dq_reason",
        when(rule_actual_negative, "ActualQuantity < 0")
        .when(rule_scrap_negative, "ScrapQuantity < 0")
        .when(rule_missing_workorder, "WorkOrderID not found in silver_workorders")
        .otherwise("Unknown")
    ) \
    .drop("WO_OK") \
    .withColumn("quarantined_at", current_timestamp())

# Valid
pr_silver = pr_with_ri.where(~(rule_actual_negative | rule_scrap_negative | rule_missing_workorder)) \
    .drop("WO_OK")

# Write
pr_silver.write.mode("overwrite").format("delta").saveAsTable("silver_productionruns")
pr_quarantine.write.mode("overwrite").format("delta").saveAsTable("silver_productionruns_quarantine")

print("silver_productionruns =", spark.table("silver_productionruns").count())
print("silver_productionruns_quarantine =", spark.table("silver_productionruns_quarantine").count())

##---
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp

# Recreate the same rules for counts (based on pr_with_ri from previous cell)
issue_rows = [
    ("silver_productionruns", "ActualQuantity_nonnegative", int(pr_clean.where(col("ActualQuantity") < 0).count())),
    ("silver_productionruns", "ScrapQuantity_nonnegative", int(pr_clean.where(col("ScrapQuantity") < 0).count())),
    ("silver_productionruns", "WorkOrderID_exists", int(pr_with_ri.where(col("WO_OK").isNull()).count()))
]

dq_insert_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("rule_name", StringType(), False),
    StructField("issue_count", IntegerType(), False)
])

dq_df = spark.createDataFrame(issue_rows, schema=dq_insert_schema) \
    .withColumn("logged_at", current_timestamp())

dq_df.write.mode("append").format("delta").saveAsTable("dq_issues_log")

print("Logged rows =", dq_df.count())
## --

from pyspark.sql.functions import col, when, current_timestamp

qi = spark.table("bronze_qualityinspections")

qi_clean = qi.select(
    col("InspectionID").cast("int").alias("InspectionID"),
    col("ProductionRunID").cast("int").alias("ProductionRunID"),
    col("DefectType").cast("string").alias("DefectType"),
    col("DefectCount").cast("int").alias("DefectCount"),
    col("InspectionDate").cast("date").alias("InspectionDate"),
    col("ingestion_ts")
)

# RI reference
pr_keys = spark.table("silver_productionruns").select("ProductionRunID").distinct()

qi_with_ri = qi_clean.join(
    pr_keys.withColumnRenamed("ProductionRunID", "PR_OK"),
    qi_clean.ProductionRunID == col("PR_OK"),
    "left"
)

# DQ rules
rule_defect_null = col("DefectCount").isNull()
rule_defect_negative = col("DefectCount") < 0
rule_missing_run = col("PR_OK").isNull()

# Quarantine
qi_quarantine = qi_with_ri.where(rule_defect_null | rule_defect_negative | rule_missing_run) \
    .withColumn(
        "dq_reason",
        when(rule_defect_null, "DefectCount is NULL")
        .when(rule_defect_negative, "DefectCount < 0")
        .when(rule_missing_run, "ProductionRunID not found in silver_productionruns")
        .otherwise("Unknown")
    ) \
    .drop("PR_OK") \
    .withColumn("quarantined_at", current_timestamp())

# Valid
qi_silver = qi_with_ri.where(~(rule_defect_null | rule_defect_negative | rule_missing_run)) \
    .drop("PR_OK")

qi_silver.write.mode("overwrite").format("delta").saveAsTable("silver_qualityinspections")
qi_quarantine.write.mode("overwrite").format("delta").saveAsTable("silver_qualityinspections_quarantine")

print("silver_qualityinspections =", spark.table("silver_qualityinspections").count())
print("silver_qualityinspections_quarantine =", spark.table("silver_qualityinspections_quarantine").count())

##---

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp

issue_rows = [
    ("silver_qualityinspections", "DefectCount_not_null", int(qi_clean.where(col("DefectCount").isNull()).count())),
    ("silver_qualityinspections", "DefectCount_nonnegative", int(qi_clean.where(col("DefectCount") < 0).count())),
    ("silver_qualityinspections", "ProductionRunID_exists", int(qi_with_ri.where(col("PR_OK").isNull()).count()))
]

dq_insert_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("rule_name", StringType(), False),
    StructField("issue_count", IntegerType(), False)
])

dq_df = spark.createDataFrame(issue_rows, schema=dq_insert_schema) \
    .withColumn("logged_at", current_timestamp())

dq_df.write.mode("append").format("delta").saveAsTable("dq_issues_log")

print("Logged rows =", dq_df.count())


##---

from pyspark.sql.functions import col, when, current_timestamp

inv = spark.table("bronze_inventory")

inv_clean = inv.select(
    col("InventoryID").cast("int").alias("InventoryID"),
    col("ProductID").cast("int").alias("ProductID"),
    col("PlantID").cast("int").alias("PlantID"),
    col("QuantityOnHand").cast("int").alias("QuantityOnHand"),
    col("LastUpdated").cast("date").alias("LastUpdated"),
    col("ingestion_ts")
)

# Reference keys (use bronze for now; we'll build silver refs next)
prod_keys = spark.table("bronze_products").select(col("ProductID").cast("int").alias("ProductID")).distinct()
plant_keys = spark.table("bronze_plants").select(col("PlantID").cast("int").alias("PlantID")).distinct()

inv_with_refs = inv_clean \
    .join(prod_keys.withColumnRenamed("ProductID", "PROD_OK"), inv_clean.ProductID == col("PROD_OK"), "left") \
    .join(plant_keys.withColumnRenamed("PlantID", "PLANT_OK"), inv_clean.PlantID == col("PLANT_OK"), "left")

# DQ rules
rule_qty_negative = col("QuantityOnHand") < 0
rule_missing_product = col("PROD_OK").isNull()
rule_missing_plant = col("PLANT_OK").isNull()

# Quarantine
inv_quarantine = inv_with_refs.where(rule_qty_negative | rule_missing_product | rule_missing_plant) \
    .withColumn(
        "dq_reason",
        when(rule_qty_negative, "QuantityOnHand < 0")
        .when(rule_missing_product, "ProductID not found in products")
        .when(rule_missing_plant, "PlantID not found in plants")
        .otherwise("Unknown")
    ) \
    .drop("PROD_OK", "PLANT_OK") \
    .withColumn("quarantined_at", current_timestamp())

# Valid
inv_silver = inv_with_refs.where(~(rule_qty_negative | rule_missing_product | rule_missing_plant)) \
    .drop("PROD_OK", "PLANT_OK")

inv_silver.write.mode("overwrite").format("delta").saveAsTable("silver_inventory")
inv_quarantine.write.mode("overwrite").format("delta").saveAsTable("silver_inventory_quarantine")

print("silver_inventory =", spark.table("silver_inventory").count())
print("silver_inventory_quarantine =", spark.table("silver_inventory_quarantine").count())

##---

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp

issue_rows = [
    ("silver_inventory", "QuantityOnHand_nonnegative", int(inv_clean.where(col("QuantityOnHand") < 0).count())),
    ("silver_inventory", "ProductID_exists", int(inv_with_refs.where(col("PROD_OK").isNull()).count())),
    ("silver_inventory", "PlantID_exists", int(inv_with_refs.where(col("PLANT_OK").isNull()).count()))
]

dq_insert_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("rule_name", StringType(), False),
    StructField("issue_count", IntegerType(), False)
])

dq_df = spark.createDataFrame(issue_rows, schema=dq_insert_schema) \
    .withColumn("logged_at", current_timestamp())

dq_df.write.mode("append").format("delta").saveAsTable("dq_issues_log")

print("Logged rows =", dq_df.count())

##--

from pyspark.sql.functions import col, current_timestamp

# Plants
spark.table("bronze_plants") \
    .select(
        col("PlantID").cast("int").alias("PlantID"),
        col("PlantName").cast("string").alias("PlantName"),
        col("Location").cast("string").alias("Location"),
        col("ingestion_ts")
    ) \
    .write.mode("overwrite").format("delta").saveAsTable("silver_plants")

# Products
spark.table("bronze_products") \
    .select(
        col("ProductID").cast("int").alias("ProductID"),
        col("ProductName").cast("string").alias("ProductName"),
        col("Category").cast("string").alias("Category"),
        col("ingestion_ts")
    ) \
    .write.mode("overwrite").format("delta").saveAsTable("silver_products")

# Suppliers (not referenced yet, but we keep it for future scorecard)
spark.table("bronze_suppliers") \
    .select(
        col("SupplierID").cast("int").alias("SupplierID"),
        col("SupplierName").cast("string").alias("SupplierName"),
        col("Region").cast("string").alias("Region"),
        col("ingestion_ts")
    ) \
    .write.mode("overwrite").format("delta").saveAsTable("silver_suppliers")

print("silver_plants =", spark.table("silver_plants").count())
print("silver_products =", spark.table("silver_products").count())
print("silver_suppliers =", spark.table("silver_suppliers").count())


##------

