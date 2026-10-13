# Databricks notebook source
# MAGIC %md
# MAGIC # 99 – Cleanup: remove all workshop data
# MAGIC
# MAGIC Run by the job `workshop_cleanup`. The job has **no schedule** – it only runs when you start it,
# MAGIC and only if the parameter `confirm` is set to `yes`:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle run cleanup --params confirm=yes --profile <your-profile>
# MAGIC ```
# MAGIC
# MAGIC | What | Action |
# MAGIC |---|---|
# MAGIC | catalog `mzmv` | dropped completely (`DROP CATALOG ... CASCADE`) |
# MAGIC | all tables and views in the workshop schemas (`mobility_bronze`, `mobility_silver`, `mobility_gold`, `exercises`, `adventureworks`) | dropped |
# MAGIC | tables of the reference pipeline (`mobility_pipeline`) | **not touched** – they are managed by the pipeline. Reset them with a *full refresh* of the pipeline, remove them with `databricks bundle destroy` |
# MAGIC | all files in the volume `mobility_raw.landing` | deleted |
# MAGIC
# MAGIC The **schemas, the volume, the jobs and the dashboard stay** – they belong to the bundle. After a cleanup you can
# MAGIC start fresh with the setup job. To remove *everything* including the bundle resources, run
# MAGIC `databricks bundle destroy` afterwards.

# COMMAND ----------

dbutils.widgets.text("confirm", "no", "Type 'yes' to delete everything")
dbutils.widgets.text("catalog", "workspace", "Workshop catalog")
dbutils.widgets.text("mzmv_catalog", "mzmv", "MZMV catalog")
dbutils.widgets.text(
    "schemas",
    "mobility_bronze,mobility_silver,mobility_gold,exercises,adventureworks",
    "Schemas to empty",
)

if dbutils.widgets.get("confirm").strip().lower() != "yes":
    raise RuntimeError("Cleanup NOT executed: set the parameter 'confirm' to 'yes' to delete all workshop data.")

catalog = dbutils.widgets.get("catalog").strip()
mzmv_catalog = dbutils.widgets.get("mzmv_catalog").strip()
schemas = [s.strip() for s in dbutils.widgets.get("schemas").split(",") if s.strip()]

# COMMAND ----------

# 1) Drop the MZMV catalog (not managed by the bundle)
if mzmv_catalog:
    spark.sql(f"DROP CATALOG IF EXISTS `{mzmv_catalog}` CASCADE")
    print(f"dropped catalog {mzmv_catalog}")

# COMMAND ----------

# 2) Drop all tables and views in the workshop schemas (the schemas themselves stay)
existing = {r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()}

for schema in schemas:
    if schema not in existing:
        continue
    for row in spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").collect():
        if row.isTemporary:
            continue
        name = f"`{catalog}`.`{schema}`.`{row.tableName}`"
        kind = spark.sql(f"DESCRIBE TABLE EXTENDED {name}").filter("col_name = 'Type'").collect()
        is_view = bool(kind) and "VIEW" in kind[0].data_type.upper()
        try:
            spark.sql(f"DROP {'VIEW' if is_view else 'TABLE'} IF EXISTS {name}")
            print(f"dropped {name}")
        except Exception as e:  # e.g. tables owned by a pipeline – removed with the pipeline
            print(f"skipped {name}: {str(e).splitlines()[0][:120]}")

# COMMAND ----------

# 3) Delete all files in the landing volume (the volume itself stays)
volume = f"/Volumes/{catalog}/mobility_raw/landing"
try:
    for entry in dbutils.fs.ls(volume):
        dbutils.fs.rm(entry.path, recurse=True)
        print(f"deleted {entry.path}")
except Exception as e:
    print(f"volume not cleaned: {e}")

print("\nCleanup finished. Run the setup job to start again.")
