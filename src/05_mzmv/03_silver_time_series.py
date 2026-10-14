# Databricks notebook source
# MAGIC %md
# MAGIC # MZMV 03 – Silver example: harmonize the trips 1994–2021
# MAGIC
# MAGIC **Goal (≈ 30 min):** a worked example of the typical bronze → silver steps on two tables of the MZMV time series.
# MAGIC Afterwards you continue on your own with the other tables (persons, households, stages) and the gold layer.
# MAGIC
# MAGIC | Silver table | Built from | Grain |
# MAGIC |---|---|---|
# MAGIC | `mzmv.silver.code_labels` | `zeitreihen_formats` | one row per format and code (prepared) |
# MAGIC | `mzmv.silver.trips` | `zeitreihen_wege94` … `zeitreihen_wege21` | one row per trip (*Weg*) – **your task** |
# MAGIC
# MAGIC Typical silver work you will do: **combine** tables of different years, **rename** columns, turn **special codes**
# MAGIC (negative values such as `-97` "no answer", `-99` "not applicable") into `NULL`, and **decode** codes into labels.
# MAGIC
# MAGIC 💡 Look up a variable: Catalog Explorer → `mzmv` → `bronze` → table → **Columns** (German description),
# MAGIC or the documentation `Variablen_MZMV_1994-2021.xlsx` in `/Volumes/mzmv/raw/files/`.
# MAGIC
# MAGIC **Prerequisite:** the job `02_mzmv_load` ran successfully.

# COMMAND ----------

from functools import reduce

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "mzmv", "Catalog")
CATALOG = dbutils.widgets.get("catalog")
BRONZE = f"`{CATALOG}`.bronze"
SILVER = f"`{CATALOG}`.silver"

# file suffix in the bronze table names -> survey year
YEARS = {"94": 1994, "00": 2000, "05": 2005, "10": 2010, "15": 2015, "21": 2021}


def save(df, table):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER}.{table}")
    print(f"{CATALOG}.silver.{table}: {spark.table(f'{SILVER}.{table}').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 – Code labels (prepared)
# MAGIC
# MAGIC The SAS format table maps codes to labels. `START` is text with leading spaces (`'   -97'`), so we trim it and
# MAGIC convert it to a number. Example: format `SEX`, code `2` → `Frau`.

# COMMAND ----------

df_labels = (
    spark.table(f"{BRONZE}.zeitreihen_formats")
    .select(
        F.col("FMTNAME").alias("format_name"),
        F.trim("START").alias("code_text"),
        F.col("LABEL").alias("label"),
    )
    .filter(F.col("code_text").rlike("^-?[0-9]+$"))
    .select("format_name", F.col("code_text").cast("int").alias("code"), "label")
)
save(df_labels, "code_labels")
display(spark.table(f"{SILVER}.code_labels").filter("format_name IN ('WMITTELA', 'SEX')"))

# COMMAND ----------

# MAGIC %md
# MAGIC Two helper functions you will use below:

# COMMAND ----------

def valid(col_name):
    """Negative values are special codes (-97 no answer, -98 don't know, -99 not applicable) -> NULL."""
    return F.when(F.col(col_name) >= 0, F.col(col_name))


labels = spark.table(f"{SILVER}.code_labels")


def add_label(df, code_col, format_name, label_col):
    """Add a column `label_col` with the label of `code_col` according to the SAS format `format_name`."""
    lookup = (
        labels.filter(F.col("format_name") == format_name)
        .select(F.col("code").alias("_code"), F.col("label").alias(label_col))
    )
    return df.join(lookup, df[code_col] == lookup["_code"], "left").drop("_code")


def union_years(table_prefix):
    """Combine the bronze tables of all survey years and add the column `survey_year`."""
    dfs = [
        spark.table(f"{BRONZE}.zeitreihen_{table_prefix}{suffix}").withColumn("survey_year", F.lit(year))
        for suffix, year in YEARS.items()
    ]
    # the columns differ slightly between the years -> allowMissingColumns fills them with NULL
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3.2 – Combine and clean the trips
# MAGIC
# MAGIC Build `df_trips` from **all six** `zeitreihen_wege..` tables (use `union_years("wege")`) with these columns:
# MAGIC
# MAGIC | New column | Bronze column | Rule |
# MAGIC |---|---|---|
# MAGIC | `survey_year` | – | added by `union_years` |
# MAGIC | `household_id` | `hhnr` | as `long` |
# MAGIC | `person_id` | `zpnr` | as `long` |
# MAGIC | `person_weight` | `wp` | – |
# MAGIC | `trip_no` | `wegnr` | as `int` |
# MAGIC | `purpose_code` | `w_zweck` | negative → NULL (`valid(...)`) |
# MAGIC | `main_mode_code` | `w_vkm` | negative → NULL |
# MAGIC | `mode_group_code` | `w_vkm_aggr` | negative → NULL |
# MAGIC | `distance_km` | `w_distanz` | negative → NULL |
# MAGIC | `duration_min` | `w_dauer2` | negative → NULL |
# MAGIC
# MAGIC Print the number of trips per `survey_year` to check your result (≈ 58 000 in 1994, ≈ 150 000 in 2021).

# COMMAND ----------

# TODO Task 3.2 – df_trips: union all years, rename, special codes -> NULL
raise NotImplementedError("Task 3.2")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>union_years(&quot;wege&quot;)</code> already returns all six years in one DataFrame. Then one <code>select</code> with <code>F.col(...).cast(...).alias(...)</code> for the IDs and <code>valid(...)</code> for the coded columns.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>df_trips = union_years(&quot;wege&quot;).select(
# MAGIC     &quot;survey_year&quot;,
# MAGIC     F.col(&quot;hhnr&quot;).cast(&quot;long&quot;).alias(&quot;household_id&quot;),
# MAGIC     F.col(&quot;zpnr&quot;).cast(___).alias(&quot;person_id&quot;),
# MAGIC     F.col(&quot;wp&quot;).alias(&quot;person_weight&quot;),
# MAGIC     F.col(&quot;wegnr&quot;).cast(&quot;int&quot;).alias(___),
# MAGIC     valid(&quot;w_zweck&quot;).cast(&quot;int&quot;).alias(&quot;purpose_code&quot;),
# MAGIC     ___,  # main_mode_code, mode_group_code
# MAGIC     valid(&quot;w_distanz&quot;).alias(&quot;distance_km&quot;),
# MAGIC     ___,  # duration_min
# MAGIC )
# MAGIC display(df_trips.groupBy(&quot;survey_year&quot;).count().orderBy(&quot;survey_year&quot;))</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3.3 – Decode the codes and save `silver.trips`
# MAGIC
# MAGIC Add readable labels with `add_label(df, code_col, format_name, label_col)`:
# MAGIC
# MAGIC | Code column | SAS format | New label column |
# MAGIC |---|---|---|
# MAGIC | `purpose_code` | `ZWECK` | `purpose` (e.g. *Arbeiten*, *Freizeitaktivität*) |
# MAGIC | `main_mode_code` | `WMITTELB` | `main_mode` (e.g. *Velo, E-Bike*, *Auto*, *Bahn*) |
# MAGIC | `mode_group_code` | `WMITTELA` | `mode_group` (*Langsamverkehr*, *Motorisierter Individualverkehr*, *Öffentlicher Verkehr*, *Anderes*) |
# MAGIC
# MAGIC Save the result as **`mzmv.silver.trips`** and look at a few rows.

# COMMAND ----------

# TODO Task 3.3 – decode purpose, main mode and mode group; save SILVER.trips
raise NotImplementedError("Task 3.3")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Call <code>add_label</code> three times – each call returns a new DataFrame with one additional label column. Then <code>save(df_trips, &quot;trips&quot;)</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>df_trips = add_label(df_trips, &quot;purpose_code&quot;, &quot;ZWECK&quot;, &quot;purpose&quot;)
# MAGIC df_trips = add_label(df_trips, ___, &quot;WMITTELB&quot;, ___)
# MAGIC df_trips = add_label(df_trips, ___, ___, &quot;mode_group&quot;)
# MAGIC save(df_trips, ___)</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT survey_year,
# MAGIC        COUNT(*)                                     AS trips,
# MAGIC        ROUND(SUM(person_weight * distance_km) / SUM(person_weight), 1) AS avg_trip_km_weighted,
# MAGIC        COUNT_IF(main_mode = 'Velo, E-Bike')        AS bike_trips_in_sample
# MAGIC FROM mzmv.silver.trips
# MAGIC GROUP BY survey_year
# MAGIC ORDER BY survey_year

# COMMAND ----------

# MAGIC %md
# MAGIC Expected: six survey years, ≈ 58 000 trips (1994) to ≈ 210 000 (2010).
# MAGIC
# MAGIC ## 🚀 Your turn
# MAGIC
# MAGIC Apply the same pattern to more tables and build your own gold layer, for example:
# MAGIC
# MAGIC 1. **`silver.persons`** from `zeitreihen_personen94` … `21`: age, sex (`SEX`), GA / Halbtax (`p_abogen`, `p_abohalb`),
# MAGIC    car availability (`VERFUEGG`), trips and distance per day.
# MAGIC 2. **Gold: modal split** – the *weighted* share of trips per `mode_group` and survey year:
# MAGIC    `SUM(person_weight)` per group divided by `SUM(person_weight)` per year.
# MAGIC 3. **Gold: bike and walking share** over the years – and compare with the Zurich counts of the project.
# MAGIC
# MAGIC ⚖️ The MZMV is a sample: always use the weights (`person_weight` for persons and trips, `WM` for households).
