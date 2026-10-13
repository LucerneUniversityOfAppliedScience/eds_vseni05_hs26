# Databricks notebook source
# MAGIC %md
# MAGIC # 00 – Setup: create the `mzmv` catalog
# MAGIC
# MAGIC Creates the catalog **`mzmv`** (Mikrozensus Mobilität und Verkehr) with a medallion structure:
# MAGIC
# MAGIC ```
# MAGIC mzmv
# MAGIC ├── raw      (+ volumes "landing" for the uploaded ZIP and "files" for the extracted files)
# MAGIC ├── bronze
# MAGIC ├── silver
# MAGIC └── gold
# MAGIC ```
# MAGIC
# MAGIC Why not in the bundle (`resources.catalogs`)? In Databricks Free Edition the catalog API requires an explicit
# MAGIC storage location, while SQL `CREATE CATALOG` automatically uses the workspace's *Default Storage*.
# MAGIC So we create the catalog – and its schemas – here with SQL. Everything is idempotent (`IF NOT EXISTS`).
# MAGIC The cleanup job (`workshop_cleanup`) removes it again.

# COMMAND ----------

dbutils.widgets.text("mzmv_catalog", "mzmv", "Catalog name")
dbutils.widgets.text("mzmv_schemas", "raw,bronze,silver,gold", "Schemas (comma-separated)")

catalog = dbutils.widgets.get("mzmv_catalog").strip()
schemas = [s.strip() for s in dbutils.widgets.get("mzmv_schemas").split(",") if s.strip()]

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}` COMMENT 'Mikrozensus Mobilitaet und Verkehr (created by the workshop setup job)'")
print(f"catalog ready: {catalog}")

for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}` COMMENT '{schema} layer'")
    print(f"schema ready:  {catalog}.{schema}")

if "raw" in schemas:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`raw`.`landing` COMMENT 'Upload the original MZMV ZIP file here'")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`raw`.`files` COMMENT 'Extracted MZMV files (written by the mzmv_load job)'")
    print(f"volumes ready: /Volumes/{catalog}/raw/landing, /Volumes/{catalog}/raw/files")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN `{catalog}`"))
