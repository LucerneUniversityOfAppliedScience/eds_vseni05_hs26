# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2 – Bronze: load the raw data 1:1
# MAGIC
# MAGIC **Goal (≈ 20 min):** Load all four sources from the volume into Delta tables in the schema
# MAGIC `mobility_bronze` – without changing anything.
# MAGIC
# MAGIC ```
# MAGIC  Volume (files)                                   mobility_bronze (Delta tables)
# MAGIC  mobility/bike_pedestrian_counts/*.parquet   ──►  bike_counts
# MAGIC  mobility/motorized_traffic_counts/*.parquet ──►  car_counts
# MAGIC  mobility/bike_pedestrian_stations/*.csv     ──►  bike_stations
# MAGIC  mobility/weather/*.csv                      ──►  weather
# MAGIC ```
# MAGIC
# MAGIC **Bronze principle:** change nothing, throw nothing away. We only add two technical columns:
# MAGIC - `_source_file` – which file did the row come from?
# MAGIC - `_ingested_at` – when was it loaded?

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

LANDING = f"/Volumes/{CATALOG}/mobility_raw/landing/mobility"
BRONZE = f"{CATALOG}.mobility_bronze"


def add_metadata(df):
    """Add the two technical bronze columns."""
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2.1 – Bike & pedestrian counts (example – already solved)
# MAGIC
# MAGIC Parquet files contain their own schema, so reading them is a one-liner.

# COMMAND ----------

df_bike = spark.read.parquet(f"{LANDING}/bike_pedestrian_counts/")

(
    add_metadata(df_bike)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{BRONZE}.bike_counts")
)

print(f"{BRONZE}.bike_counts: {spark.table(f'{BRONZE}.bike_counts').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2.2 – Motorized traffic counts
# MAGIC
# MAGIC Load `mobility/motorized_traffic_counts/` (Parquet) into **`mobility_bronze.car_counts`**,
# MAGIC exactly like Task 2.1. Print the number of rows (expected: ≈ 23 million).

# COMMAND ----------

# TODO Task 2.2 – load motorized_traffic_counts (Parquet) into BRONZE.car_counts
raise NotImplementedError("Task 2.2")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Exactly the same as Task 2.1 – only the folder (<code>motorized_traffic_counts</code>) and the table name (<code>car_counts</code>) change.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>df_car = spark.read.parquet(f&quot;{LANDING}/___/&quot;)
# MAGIC add_metadata(df_car).write.mode(&quot;overwrite&quot;).option(&quot;overwriteSchema&quot;, &quot;true&quot;).saveAsTable(f&quot;{BRONZE}.___&quot;)
# MAGIC print(spark.table(f&quot;{BRONZE}.car_counts&quot;).count())</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>df_car = spark.read.parquet(f&quot;{LANDING}/motorized_traffic_counts/&quot;)
# MAGIC 
# MAGIC (
# MAGIC     add_metadata(df_car)
# MAGIC     .write.mode(&quot;overwrite&quot;)
# MAGIC     .option(&quot;overwriteSchema&quot;, &quot;true&quot;)
# MAGIC     .saveAsTable(f&quot;{BRONZE}.car_counts&quot;)
# MAGIC )
# MAGIC 
# MAGIC print(f&quot;{BRONZE}.car_counts: {spark.table(f&#x27;{BRONZE}.car_counts&#x27;).count():,} rows&quot;)</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2.3 – Counting stations and weather (CSV)
# MAGIC
# MAGIC Load the two CSV sources:
# MAGIC - `mobility/bike_pedestrian_stations/` → **`mobility_bronze.bike_stations`**
# MAGIC - `mobility/weather/` → **`mobility_bronze.weather`**
# MAGIC
# MAGIC Hints:
# MAGIC - `spark.read.format("csv").option("header", "true").load(path)`
# MAGIC - In bronze we keep **all columns as text** – so do *not* use `inferSchema`.
# MAGIC - Don't forget `add_metadata(...)`.
# MAGIC - *Optional:* write a small helper function, you need the same code twice.

# COMMAND ----------

# TODO Task 2.3 – load bike_pedestrian_stations and weather (CSV, all columns as string)
raise NotImplementedError("Task 2.3")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Read the CSV with <code>spark.read.format(&quot;csv&quot;).option(&quot;header&quot;, &quot;true&quot;).load(path)</code> – without <code>inferSchema</code>, so every column stays a string. Write a small function <code>load_csv_to_bronze(folder, table)</code> and call it twice.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def load_csv_to_bronze(folder, table):
# MAGIC     df = spark.read.format(&quot;csv&quot;).option(&quot;header&quot;, &quot;___&quot;).load(f&quot;{LANDING}/{folder}/&quot;)
# MAGIC     add_metadata(df).write.mode(&quot;overwrite&quot;).option(&quot;overwriteSchema&quot;, &quot;true&quot;).saveAsTable(f&quot;{BRONZE}.{table}&quot;)
# MAGIC 
# MAGIC load_csv_to_bronze(&quot;bike_pedestrian_stations&quot;, &quot;___&quot;)
# MAGIC load_csv_to_bronze(&quot;___&quot;, &quot;weather&quot;)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def load_csv_to_bronze(folder: str, table: str):
# MAGIC     df = spark.read.format(&quot;csv&quot;).option(&quot;header&quot;, &quot;true&quot;).load(f&quot;{LANDING}/{folder}/&quot;)
# MAGIC     (
# MAGIC         add_metadata(df)
# MAGIC         .write.mode(&quot;overwrite&quot;)
# MAGIC         .option(&quot;overwriteSchema&quot;, &quot;true&quot;)
# MAGIC         .saveAsTable(f&quot;{BRONZE}.{table}&quot;)
# MAGIC     )
# MAGIC     print(f&quot;{BRONZE}.{table}: {spark.table(f&#x27;{BRONZE}.{table}&#x27;).count():,} rows&quot;)
# MAGIC 
# MAGIC 
# MAGIC load_csv_to_bronze(&quot;bike_pedestrian_stations&quot;, &quot;bike_stations&quot;)
# MAGIC load_csv_to_bronze(&quot;weather&quot;, &quot;weather&quot;)</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Check
# MAGIC
# MAGIC Run the cell below. All four tables should exist and have rows.

# COMMAND ----------

expected = {"bike_counts": 15_000_000, "car_counts": 20_000_000, "bike_stations": 100, "weather": 100_000}
for table, minimum in expected.items():
    n = spark.table(f"{BRONZE}.{table}").count()
    status = "✅" if n >= minimum else "❌"
    print(f"{status} {BRONZE}.{table:<14} {n:>12,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC 👀 Open the **Catalog Explorer** → `workspace` → `mobility_bronze` → `car_counts`.
# MAGIC Look at the tabs **Columns**, **Sample Data**, **Details** and **History**.
# MAGIC
# MAGIC 👉 Continue with **step_3_silver**
