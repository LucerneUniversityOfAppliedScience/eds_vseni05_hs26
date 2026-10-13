# Databricks notebook source
# MAGIC %md
# MAGIC # 02 – Setup: load AdventureWorks into Delta tables
# MAGIC
# MAGIC Run automatically by the setup job after `01_download_data`.
# MAGIC Every CSV file `<Schema>.<Table>.csv` in the volume folder `adventureworks/` becomes a table
# MAGIC `<catalog>.adventureworks.<schema>_<table>` (lower case), e.g.
# MAGIC `Production.Product.csv` → `workspace.adventureworks.production_product`.
# MAGIC
# MAGIC These tables are used by exercise `e01_sql_basics_adventureworks`.

# COMMAND ----------

import os

dbutils.widgets.text("catalog", "workspace", "Catalog")
catalog = dbutils.widgets.get("catalog").strip() or "workspace"

source_dir = f"/Volumes/{catalog}/mobility_raw/landing/adventureworks"
target_schema = f"{catalog}.adventureworks"

files = sorted(f for f in os.listdir(source_dir) if f.endswith(".csv"))
print(f"{len(files)} files in {source_dir}")

# COMMAND ----------

for name in files:
    schema_part, table_part, _ = name.split(".")
    table = f"{target_schema}.{schema_part.lower()}_{table_part.lower()}"
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("escape", '"')            # values contain "" as escaped quotes
        .option("nullValue", "[NULL]")
        .load(f"{source_dir}/{name}")
    )
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"{table}: {spark.table(table).count():,} rows")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {target_schema}"))
