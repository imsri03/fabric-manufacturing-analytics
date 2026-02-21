###
##01_bronze_ingestion.py
##Purpose: Ingest SQL DB tables into Lakehouse Bronze Delta tables.
##Creates: bronze_plants, bronze_products, bronze_suppliers, bronze_workorders,
   ##    bronze_productionruns, bronze_qualityinspections, bronze_inventory
###

# Step 1: Get a Fabric token from the notebook runtime
import notebookutils

token = notebookutils.credentials.getToken("pbi")
print("Token length:", len(token))

## ----
from pyspark.sql.functions import current_timestamp

jdbc_url = (
    "jdbc:sqlserver://h5e44t2dc4zu3ppmphrirbr4de-icvn3ax7gavexj34hygzobhupe.database.fabric.microsoft.com:1433;"
    "database=manufacturing_sqldb-16e27d13-1f90-4869-9b46-30ecf981c59f;"
    "encrypt=true;"
    "trustServerCertificate=false;"
)

df_workorders = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.WorkOrders")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("accessToken", token)
    .load()
)

df_workorders = df_workorders.withColumn("ingestion_ts", current_timestamp())

df_workorders.write.mode("overwrite").format("delta").saveAsTable("bronze_workorders")

print("bronze_workorders count =", spark.table("bronze_workorders").count())

## ---
from pyspark.sql.functions import current_timestamp

jdbc_url = (
    "jdbc:sqlserver://h5e44t2dc4zu3ppmphrirbr4de-icvn3ax7gavexj34hygzobhupe.database.fabric.microsoft.com:1433;"
    "database=manufacturing_sqldb-16e27d13-1f90-4869-9b46-30ecf981c59f;"
    "encrypt=true;"
    "trustServerCertificate=false;"
)

tables = [
    ("Plants", "bronze_plants"),
    ("Products", "bronze_products"),
    ("Suppliers", "bronze_suppliers"),
    ("WorkOrders", "bronze_workorders"),
    ("ProductionRuns", "bronze_productionruns"),
    ("QualityInspections", "bronze_qualityinspections"),
    ("Inventory", "bronze_inventory")
]

results = []

for src_table, bronze_table in tables:
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"dbo.{src_table}")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("accessToken", token)
        .load()
    )
    
    df = df.withColumn("ingestion_ts", current_timestamp())
    
    df.write.mode("overwrite").format("delta").saveAsTable(bronze_table)
    
    cnt = spark.table(bronze_table).count()
    results.append((src_table, bronze_table, cnt))

results
