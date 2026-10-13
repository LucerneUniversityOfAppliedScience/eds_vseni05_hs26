# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1 – Explore the sources
# MAGIC
# MAGIC **Goal (≈ 20 min):** Get to know the four data sources before building anything. Good data engineering starts
# MAGIC with looking at the data!
# MAGIC
# MAGIC | # | Source | Format | Grain |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Bike & pedestrian counts | Parquet | station × 15 minutes, 2009–2026 |
# MAGIC | 2 | Motorized traffic counts | Parquet | measuring point (site × direction) × hour, 2012–2026 |
# MAGIC | 3 | Bike & pedestrian counting stations | CSV | station |
# MAGIC | 4 | Weather Zurich | CSV | hour, 2009–2026 |
# MAGIC
# MAGIC This notebook is fully prepared – run it cell by cell and answer the questions marked with ✏️
# MAGIC (write your answers into the markdown cells or discuss them with your neighbour).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
CATALOG = dbutils.widgets.get("catalog")
LANDING = f"/Volumes/{CATALOG}/mobility_raw/landing/mobility"

display(dbutils.fs.ls(LANDING))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 – Bike & pedestrian counts

# COMMAND ----------

df_bike = spark.read.parquet(f"{LANDING}/bike_pedestrian_counts/")
df_bike.printSchema()
print(f"Rows: {df_bike.count():,}")
display(df_bike.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC | Column | Meaning |
# MAGIC |---|---|
# MAGIC | `FK_STANDORT` | station ID (→ source 3) |
# MAGIC | `DATUM` | start of the 15-minute interval, local time, **as text** |
# MAGIC | `VELO_IN` / `VELO_OUT` | bikes in direction "in" / "out" |
# MAGIC | `FUSS_IN` / `FUSS_OUT` | pedestrians in direction "in" / "out" |
# MAGIC | `OST` / `NORD` | Swiss coordinates (LV95) |
# MAGIC
# MAGIC ✏️ Why are many `FUSS_*` values empty (NULL)? What could a NULL in `VELO_IN` mean?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 – Motorized traffic counts

# COMMAND ----------

df_car = spark.read.parquet(f"{LANDING}/motorized_traffic_counts/")
print(f"Rows: {df_car.count():,}")
display(df_car.limit(10))

# COMMAND ----------

display(df_car.groupBy("AnzFahrzeugeStatus").count())

# COMMAND ----------

# MAGIC %md
# MAGIC | Column | Meaning |
# MAGIC |---|---|
# MAGIC | `ZSID` / `ZSName` | counting site ID / name |
# MAGIC | `MSID` | measuring point = one direction of a site |
# MAGIC | `Richtung` | direction (`einwärts` = into the city, `auswärts` = out of the city) |
# MAGIC | `EKoord` / `NKoord` | Swiss coordinates (LV95) |
# MAGIC | `MessungDatZeit` | start of the hour incl. time zone, **as text** |
# MAGIC | `AnzFahrzeuge` | number of vehicles in this hour |
# MAGIC | `AnzFahrzeugeStatus` | `Gemessen` = measured, `Imputiert` = estimated, `Fehlend` = missing |
# MAGIC
# MAGIC ✏️ How many percent of the values are missing? What should we do with them?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 – Bike & pedestrian counting stations

# COMMAND ----------

df_stations = spark.read.format("csv").option("header", "true").load(f"{LANDING}/bike_pedestrian_stations/")
display(df_stations)

# COMMAND ----------

# MAGIC %md
# MAGIC ✏️ Which column links the stations to the counts in source 1? Which stations count bikes, which pedestrians?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 – Weather

# COMMAND ----------

df_weather = spark.read.format("csv").option("header", "true").load(f"{LANDING}/weather/")
df_weather.printSchema()
display(df_weather.orderBy(df_weather.time.desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ✏️ All columns are `string`. Why? What will we have to do in the silver layer?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 – The big picture: different grains
# MAGIC
# MAGIC To compare bikes, cars and weather we need a **common grain**:
# MAGIC
# MAGIC ```
# MAGIC  bikes:   station × 15 min  ──┐
# MAGIC  cars:    direction × 1 h  ───┼──►  per hour  ──►  per day  ──►  dashboard
# MAGIC  weather: 1 h  ───────────────┘
# MAGIC ```
# MAGIC
# MAGIC That is exactly what we build in the next steps:
# MAGIC **step 2 – bronze** (load 1:1) → **step 3 – silver** (clean, hourly) → **step 4 – gold** (daily, joined) →
# MAGIC **step 5 – analysis** → **step 6 – dashboard**.
# MAGIC
# MAGIC ✏️ Quick SQL warm-up: in which year were the most bikes counted? (Try it with `read_files` below.)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT left(DATUM, 4) AS year,
# MAGIC        SUM(coalesce(VELO_IN, 0) + coalesce(VELO_OUT, 0)) AS bikes,
# MAGIC        COUNT(DISTINCT FK_STANDORT)                        AS stations
# MAGIC FROM read_files('/Volumes/workspace/mobility_raw/landing/mobility/bike_pedestrian_counts/', format => 'parquet')
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ✏️ Look at the `stations` column. Is "most bikes" really "most cycling"? 🤔
# MAGIC
# MAGIC 👉 Continue with **step_2_bronze**
