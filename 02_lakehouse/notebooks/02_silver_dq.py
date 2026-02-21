"""
02_silver_dq.py
Purpose: Create Silver tables with DQ rules + quarantine + dq_issues_log.
Creates: silver_* and silver_*_quarantine + dq_issues_log
"""
## ----

spark.sql("DROP TABLE IF EXISTS dq_issues_log")

spark.sql("""
CREATE TABLE dq_issues_log (
  run_id STRING,
  table_name STRING,
  rule_name STRING,
  issue_count INT,
  logged_at TIMESTAMP
)
USING DELTA
""")

print("dq_issues_log recreated with run_id")

###---- 

"""
02_silver_dq.py
Purpose: Build Silver layer with data quality (DQ), quarantine tables, and DQ logging.
Creates:
  - dq_issues_log (append-only, with run_id)
  - silver_workorders + silver_workorders_quarantine
  - silver_productionruns + silver_productionruns_quarantine
  - silver_qualityinspections + silver_qualityinspections_quarantine
  - silver_inventory + silver_inventory_quarantine
  - silver_plants, silver_products, silver_suppliers
"""

# -----------------------------
# 0) Setup: run_id + DQ log table
# -----------------------------
import uuid
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, when, current_timestamp, lit

run_id = str(uuid.uuid4())
print("run_id =", run_id)

def ensure_dq_log_table():
    """
    Creates dq_issues_log if it doesn't exist.
    Append-only table: each pipeline run writes a new set of DQ metrics.
    """
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dq_issues_log (
            run_id STRING,
            table_name STRING,
            rule_name STRING,
            issue_count INT,
            logged_at TIMESTAMP
        )
        USING DELTA
    """)

def log_dq(table_name: str, rule_counts: list[tuple[str, int]]):
    """
    rule_counts: list of (rule_name, issue_count)
    Writes one row per rule into dq_issues_log with the current run_id.
    """
    ensure_dq_log_table()

    rows = [(run_id, table_name, rule_name, int(count)) for rule_name, count in rule_counts]

    schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("rule_name", StringType(), False),
        StructField("issue_count", IntegerType(), False),
    ])

    df = spark.createDataFrame(rows, schema=schema).withColumn("logged_at", current_timestamp())
    df.write.mode("append").format("delta").saveAsTable("dq_issues_log")


# -----------------------------
# 1) WorkOrders -> Silver + Quarantine + DQ
# -----------------------------
wo = spark.table("bronze_workorders")

wo_clean = wo.select(
    col("WorkOrderID").cast("int").alias("WorkOrderID"),
    col("ProductID").cast("int").alias("ProductID"),
    col("PlantID").cast("int").alias("PlantID"),
    col("PlannedQuantity").cast("int").alias("PlannedQuantity"),
    col("PlannedStartDate").cast("date").alias("PlannedStartDate"),
    col("PlannedEndDate").cast("date").alias("PlannedEndDate"),
    col("ingestion_ts")
)

rule_qty_null = col("PlannedQuantity").isNull()
rule_qty_nonpositive = col("PlannedQuantity") <= 0
rule_bad_dates = col("PlannedEndDate") < col("PlannedStartDate")

wo_quarantine = (
    wo_clean.where(rule_qty_null | rule_qty_nonpositive | rule_bad_dates)
    .withColumn(
        "dq_reason",
        when(rule_qty_null, "PlannedQuantity is NULL")
        .when(rule_qty_nonpositive, "PlannedQuantity <= 0")
        .when(rule_bad_dates, "PlannedEndDate < PlannedStartDate")
        .otherwise("Unknown")
    )
    .withColumn("quarantined_at", current_timestamp())
)

wo_silver = wo_clean.where(~(rule_qty_null | rule_qty_nonpositive | rule_bad_dates))

wo_silver.write.mode("overwrite").format("delta").saveAsTable("silver_workorders")
wo_quarantine.write.mode("overwrite").format("delta").saveAsTable("silver_workorders_quarantine")

log_dq("silver_workorders", [
    ("PlannedQuantity_not_null", wo_clean.where(rule_qty_null).count()),
    ("PlannedQuantity_positive", wo_clean.where(rule_qty_nonpositive & ~rule_qty_null).count()),
    ("EndDate_after_StartDate", wo_clean.where(rule_bad_dates).count()),
])

print("silver_workorders =", spark.table("silver_workorders").count())
print("silver_workorders_quarantine =", spark.table("silver_workorders_quarantine").count())


# -----------------------------
# 2) ProductionRuns -> Silver + Quarantine + DQ (RI to silver_workorders)
# -----------------------------
pr = spark.table("bronze_productionruns")

pr_clean = pr.select(
    col("ProductionRunID").cast("int").alias("ProductionRunID"),
    col("WorkOrderID").cast("int").alias("WorkOrderID"),
    col("ActualQuantity").cast("int").alias("ActualQuantity"),
    col("ScrapQuantity").cast("int").alias("ScrapQuantity"),
    col("RunDate").cast("date").alias("RunDate"),
    col("ingestion_ts")
)

wo_keys = spark.table("silver_workorders").select("WorkOrderID").distinct()

rule_actual_negative = col("ActualQuantity") < 0
rule_scrap_negative = col("ScrapQuantity") < 0

pr_with_ri = pr_clean.join(
    wo_keys.withColumnRenamed("WorkOrderID", "WO_OK"),
    pr_clean.WorkOrderID == col("WO_OK"),
    "left"
)
rule_missing_workorder = col("WO_OK").isNull()

pr_quarantine = (
    pr_with_ri.where(rule_actual_negative | rule_scrap_negative | rule_missing_workorder)
    .withColumn(
        "dq_reason",
        when(rule_actual_negative, "ActualQuantity < 0")
        .when(rule_scrap_negative, "ScrapQuantity < 0")
        .when(rule_missing_workorder, "WorkOrderID not found in silver_workorders")
        .otherwise("Unknown")
    )
    .drop("WO_OK")
    .withColumn("quarantined_at", current_timestamp())
)

pr_silver = pr_with_ri.where(~(rule_actual_negative | rule_scrap_negative | rule_missing_workorder)).drop("WO_OK")

pr_silver.write.mode("overwrite").format("delta").saveAsTable("silver_productionruns")
pr_quarantine.write.mode("overwrite").format("delta").saveAsTable("silver_productionruns_quarantine")

log_dq("silver_productionruns", [
    ("ActualQuantity_nonnegative", pr_clean.where(col("ActualQuantity") < 0).count()),
    ("ScrapQuantity_nonnegative", pr_clean.where(col("ScrapQuantity") < 0).count()),
    ("WorkOrderID_exists", pr_with_ri.where(col("WO_OK").isNull()).count()),
])

print("silver_productionruns =", spark.table("silver_productionruns").count())
print("silver_productionruns_quarantine =", spark.table("silver_productionruns_quarantine").count())


# -----------------------------
# 3) QualityInspections -> Silver + Quarantine + DQ (RI to silver_productionruns)
# -----------------------------
qi = spark.table("bronze_qualityinspections")

qi_clean = qi.select(
    col("InspectionID").cast("int").alias("InspectionID"),
    col("ProductionRunID").cast("int").alias("ProductionRunID"),
    col("DefectType").cast("string").alias("DefectType"),
    col("DefectCount").cast("int").alias("DefectCount"),
    col("InspectionDate").cast("date").alias("InspectionDate"),
    col("ingestion_ts")
)

pr_keys = spark.table("silver_productionruns").select("ProductionRunID").distinct()

qi_with_ri = qi_clean.join(
    pr_keys.withColumnRenamed("ProductionRunID", "PR_OK"),
    qi_clean.ProductionRunID == col("PR_OK"),
    "left"
)

rule_defect_null = col("DefectCount").isNull()
rule_defect_negative = col("DefectCount") < 0
rule_missing_run = col("PR_OK").isNull()

qi_quarantine = (
    qi_with_ri.where(rule_defect_null | rule_defect_negative | rule_missing_run)
    .withColumn(
        "dq_reason",
        when(rule_defect_null, "DefectCount is NULL")
        .when(rule_defect_negative, "DefectCount < 0")
        .when(rule_missing_run, "ProductionRunID not found in silver_productionruns")
        .otherwise("Unknown")
    )
    .drop("PR_OK")
    .withColumn("quarantined_at", current_timestamp())
)

qi_silver = qi_with_ri.where(~(rule_defect_null | rule_defect_negative | rule_missing_run)).drop("PR_OK")

qi_silver.write.mode("overwrite").format("delta").saveAsTable("silver_qualityinspections")
qi_quarantine.write.mode("overwrite").format("delta").saveAsTable("silver_qualityinspections_quarantine")

log_dq("silver_qualityinspections", [
    ("DefectCount_not_null", qi_clean.where(col("DefectCount").isNull()).count()),
    ("DefectCount_nonnegative", qi_clean.where(col("DefectCount") < 0).count()),
    ("ProductionRunID_exists", qi_with_ri.where(col("PR_OK").isNull()).count()),
])

print("silver_qualityinspections =", spark.table("silver_qualityinspections").count())
print("silver_qualityinspections_quarantine =", spark.table("silver_qualityinspections_quarantine").count())


# -----------------------------
# 4) Inventory -> Silver + Quarantine + DQ (RI to products/plants)
# -----------------------------
inv = spark.table("bronze_inventory")

inv_clean = inv.select(
    col("InventoryID").cast("int").alias("InventoryID"),
    col("ProductID").cast("int").alias("ProductID"),
    col("PlantID").cast("int").alias("PlantID"),
    col("QuantityOnHand").cast("int").alias("QuantityOnHand"),
    col("LastUpdated").cast("date").alias("LastUpdated"),
    col("ingestion_ts")
)

prod_keys = spark.table("bronze_products").select(col("ProductID").cast("int").alias("ProductID")).distinct()
plant_keys = spark.table("bronze_plants").select(col("PlantID").cast("int").alias("PlantID")).distinct()

inv_with_refs = (
    inv_clean
    .join(prod_keys.withColumnRenamed("ProductID", "PROD_OK"), inv_clean.ProductID == col("PROD_OK"), "left")
    .join(plant_keys.withColumnRenamed("PlantID", "PLANT_OK"), inv_clean.PlantID == col("PLANT_OK"), "left")
)

rule_qty_negative = col("QuantityOnHand") < 0
rule_missing_product = col("PROD_OK").isNull()
rule_missing_plant = col("PLANT_OK").isNull()

inv_quarantine = (
    inv_with_refs.where(rule_qty_negative | rule_missing_product | rule_missing_plant)
    .withColumn(
        "dq_reason",
        when(rule_qty_negative, "QuantityOnHand < 0")
        .when(rule_missing_product, "ProductID not found in products")
        .when(rule_missing_plant, "PlantID not found in plants")
        .otherwise("Unknown")
    )
    .drop("PROD_OK", "PLANT_OK")
    .withColumn("quarantined_at", current_timestamp())
)

inv_silver = inv_with_refs.where(~(rule_qty_negative | rule_missing_product | rule_missing_plant)).drop("PROD_OK", "PLANT_OK")

inv_silver.write.mode("overwrite").format("delta").saveAsTable("silver_inventory")
inv_quarantine.write.mode("overwrite").format("delta").saveAsTable("silver_inventory_quarantine")

log_dq("silver_inventory", [
    ("QuantityOnHand_nonnegative", inv_clean.where(col("QuantityOnHand") < 0).count()),
    ("ProductID_exists", inv_with_refs.where(col("PROD_OK").isNull()).count()),
    ("PlantID_exists", inv_with_refs.where(col("PLANT_OK").isNull()).count()),
])

print("silver_inventory =", spark.table("silver_inventory").count())
print("silver_inventory_quarantine =", spark.table("silver_inventory_quarantine").count())


# -----------------------------
# 5) Reference tables -> Silver
# -----------------------------
spark.table("bronze_plants").select(
    col("PlantID").cast("int").alias("PlantID"),
    col("PlantName").cast("string").alias("PlantName"),
    col("Location").cast("string").alias("Location"),
    col("ingestion_ts")
).write.mode("overwrite").format("delta").saveAsTable("silver_plants")

spark.table("bronze_products").select(
    col("ProductID").cast("int").alias("ProductID"),
    col("ProductName").cast("string").alias("ProductName"),
    col("Category").cast("string").alias("Category"),
    col("ingestion_ts")
).write.mode("overwrite").format("delta").saveAsTable("silver_products")

spark.table("bronze_suppliers").select(
    col("SupplierID").cast("int").alias("SupplierID"),
    col("SupplierName").cast("string").alias("SupplierName"),
    col("Region").cast("string").alias("Region"),
    col("ingestion_ts")
).write.mode("overwrite").format("delta").saveAsTable("silver_suppliers")

print("silver_plants =", spark.table("silver_plants").count())
print("silver_products =", spark.table("silver_products").count())
print("silver_suppliers =", spark.table("silver_suppliers").count())

print("✅ Silver + DQ completed for run_id:", run_id)

##----

print(spark.table("dq_issues_log").select("run_id").distinct().count())
spark.table("dq_issues_log").orderBy("logged_at", ascending=False).show(10, truncate=False)