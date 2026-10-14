# Databricks notebook source
# MAGIC %md
# MAGIC # MZMV in class – one file from upload to silver
# MAGIC
# MAGIC **Goal (≈ 15 min):** the complete path of the MZMV data on **one single file**, so you understand every step:
# MAGIC
# MAGIC ```
# MAGIC  wege21.sas7bdat  ──upload──►  Volume mzmv.raw.landing  ──►  mzmv.bronze.example_wege21  ──►  mzmv.silver.example_trips_2021
# MAGIC  (your laptop)                 (file, as delivered)          (1:1 Delta table)                (clean, decoded)
# MAGIC ```
# MAGIC
# MAGIC The file contains all **trips (*Wege*) of the MZMV 2021** from the harmonised time series: ≈ 150 000 trips,
# MAGIC 17 columns. Loading *all* MZMV tables takes ≈ 30 minutes on Free Edition – you do that at home
# MAGIC (see the [MZMV guide](README.md)).
# MAGIC
# MAGIC **Prerequisite:** the job `01_workshop_setup` ran (it creates the catalog `mzmv`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 – Upload the file (in the browser)
# MAGIC
# MAGIC 1. Left sidebar → **Catalog** → `mzmv` → `raw` → **Volumes** → `landing`.
# MAGIC 2. **Upload to this volume** → choose `wege21.sas7bdat` (you get it from your lecturer) → **Upload**.
# MAGIC 3. Run the cell below – the file should be listed.

# COMMAND ----------

FILE = "/Volumes/mzmv/raw/landing/wege21.sas7bdat"
display(dbutils.fs.ls("/Volumes/mzmv/raw/landing/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 – Read the SAS file
# MAGIC
# MAGIC Spark cannot read SAS files directly. The Python library **pyreadstat** can – it returns a pandas DataFrame
# MAGIC **and** the metadata of the file (column descriptions, formats).

# COMMAND ----------

# MAGIC %pip install pyreadstat --quiet

# COMMAND ----------

import pyreadstat

FILE = "/Volumes/mzmv/raw/landing/wege21.sas7bdat"
pdf, meta = pyreadstat.read_sas7bdat(FILE, encoding="WINDOWS-1252")

print(f"{len(pdf):,} rows, {len(pdf.columns)} columns")
pdf.head()

# COMMAND ----------

# The metadata: every column has a German description in SAS
for column, label in meta.column_names_to_labels.items():
    print(f"{column:<12} {label}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 – Bronze: store the file 1:1 as a Delta table
# MAGIC
# MAGIC We convert the pandas DataFrame into a Spark DataFrame and save it – **without changing anything**. We only add
# MAGIC the column descriptions as **comments** and the two technical bronze columns.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructField, StructType

# All columns of this file are numbers (SAS stores numbers as double); the SAS label becomes the column comment.
schema = StructType([
    StructField(col, DoubleType(), True, {"comment": meta.column_names_to_labels.get(col) or ""})
    for col in pdf.columns
])

df_bronze = (
    spark.createDataFrame(pdf.astype("float64"), schema)
    .withColumn("_source_file", F.lit(FILE))
    .withColumn("_ingested_at", F.current_timestamp())
)
df_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("mzmv.bronze.example_wege21")

display(spark.table("mzmv.bronze.example_wege21").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC 👀 Open **Catalog** → `mzmv` → `bronze` → `example_wege21` → tab **Columns**: the German descriptions are there.
# MAGIC
# MAGIC Notice what is still "raw": codes instead of text (`w_vkm_aggr` = 1, 2, 3 …), special values like `-99`,
# MAGIC cryptic column names.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 – Silver: clean and decode
# MAGIC
# MAGIC Three typical silver steps:
# MAGIC 1. **Rename** columns to readable names
# MAGIC 2. Turn **special codes** (negative values: `-97` no answer, `-99` not applicable) into `NULL`
# MAGIC 3. **Decode** codes with a lookup table (here: the main mode of transport and the trip purpose)
# MAGIC
# MAGIC First the lookup tables – the codes come from the MZMV documentation (`Variablen_MZMV_1994-2021.xlsx`):

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mzmv.silver.example_mode_groups (code INT, mode_group STRING) AS
# MAGIC VALUES (1, 'Slow traffic (walking, cycling)'),
# MAGIC        (2, 'Motorised private transport'),
# MAGIC        (3, 'Public transport'),
# MAGIC        (4, 'Other');
# MAGIC
# MAGIC CREATE OR REPLACE TABLE mzmv.silver.example_purposes (code INT, purpose STRING) AS
# MAGIC VALUES (1, 'Change of mode'), (2, 'Work'), (3, 'Education'), (4, 'Shopping'), (5, 'Errands / services'),
# MAGIC        (6, 'Business'), (7, 'Business trip'), (8, 'Leisure'), (9, 'Accompanying'), (10, 'Return home'),
# MAGIC        (11, 'Other'), (12, 'Border crossing');
# MAGIC
# MAGIC SELECT * FROM mzmv.silver.example_mode_groups;

# COMMAND ----------

# MAGIC %md
# MAGIC Now the silver table – rename, special codes → `NULL`, join the lookup tables:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mzmv.silver.example_trips_2021 AS
# MAGIC SELECT CAST(w.hhnr AS BIGINT)                                  AS household_id,
# MAGIC        CAST(w.zpnr AS BIGINT)                                  AS person_id,
# MAGIC        w.wp                                                    AS person_weight,
# MAGIC        CAST(w.wegnr AS INT)                                    AS trip_no,
# MAGIC        CASE WHEN w.w_distanz >= 0 THEN w.w_distanz END         AS distance_km,
# MAGIC        CASE WHEN w.w_dauer2  >= 0 THEN w.w_dauer2  END         AS duration_min,
# MAGIC        CASE WHEN w.w_zweck   >  0 THEN CAST(w.w_zweck AS INT) END    AS purpose_code,
# MAGIC        p.purpose,
# MAGIC        CASE WHEN w.w_vkm_aggr > 0 THEN CAST(w.w_vkm_aggr AS INT) END AS mode_group_code,
# MAGIC        m.mode_group
# MAGIC FROM mzmv.bronze.example_wege21 w
# MAGIC LEFT JOIN mzmv.silver.example_purposes    p ON p.code = w.w_zweck
# MAGIC LEFT JOIN mzmv.silver.example_mode_groups m ON m.code = w.w_vkm_aggr;
# MAGIC
# MAGIC SELECT * FROM mzmv.silver.example_trips_2021 LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 – A first result: the modal split 2021
# MAGIC
# MAGIC The MZMV is a **sample** – always use the weight (`person_weight`). Compare the weighted and the unweighted share:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT mode_group,
# MAGIC        ROUND(SUM(person_weight) * 100 / SUM(SUM(person_weight)) OVER (), 1) AS share_weighted_pct,
# MAGIC        ROUND(COUNT(*)           * 100 / SUM(COUNT(*))           OVER (), 1) AS share_unweighted_pct
# MAGIC FROM mzmv.silver.example_trips_2021
# MAGIC WHERE mode_group IS NOT NULL
# MAGIC GROUP BY mode_group
# MAGIC ORDER BY share_weighted_pct DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Expected (weighted): ≈ 50 % motorised private transport, ≈ 38 % slow traffic, ≈ 11 % public transport.
# MAGIC
# MAGIC ✏️ **Try it:**
# MAGIC 1. Which **trip purpose** is the most frequent (weighted)?
# MAGIC 2. What is the average **distance** of a trip per mode group (weighted: `SUM(person_weight * distance_km) / SUM(person_weight)`)?

# COMMAND ----------

# your code here


# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏠 At home
# MAGIC
# MAGIC You have now done every step once. At home you load **all** MZMV tables the same way – automatically with a job:
# MAGIC upload `MZMV_workshop.zip` to `mzmv.raw.landing`, run `02_mzmv_load` (≈ 30 min) and continue with
# MAGIC `03_silver_time_series`. Details: [MZMV guide](README.md).
