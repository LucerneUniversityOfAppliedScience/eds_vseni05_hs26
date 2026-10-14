# Databricks notebook source
# MAGIC %md
# MAGIC # Step 3 – Silver: clean, type and harmonize
# MAGIC
# MAGIC **Goal (≈ 40 min):** Turn the raw bronze tables into clean, typed tables with English column names
# MAGIC and – most importantly – a **common time grain (1 hour)**, so that bikes, cars and weather can be joined later.
# MAGIC
# MAGIC | Bronze | → | Silver | Grain |
# MAGIC |---|---|---|---|
# MAGIC | `bike_stations` | → | `bike_stations` | one row per station |
# MAGIC | `car_counts` | → | `car_sites` | one row per counting site (with WGS84 coordinates) |
# MAGIC | `weather` | → | `weather_hourly` | one row per hour |
# MAGIC | `bike_counts` (15 min) | → | `bike_counts_hourly` | station × hour |
# MAGIC | `car_counts` (1 h, per direction) | → | `car_counts_hourly` | site × hour |
# MAGIC
# MAGIC The first three tables are prepared for you – study them, then build the two hourly count tables yourself.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

BRONZE = f"{CATALOG}.mobility_bronze"
SILVER = f"{CATALOG}.mobility_silver"
spark.sql(f"USE CATALOG {CATALOG}")


def save(df, table):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER}.{table}")
    print(f"{SILVER}.{table}: {spark.table(f'{SILVER}.{table}').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 – Bike stations (prepared)
# MAGIC Select the relevant columns, give them English names and correct data types.

# COMMAND ----------

df_bike_stations = (
    spark.table(f"{BRONZE}.bike_stations")
    .select(
        F.col("station_id").cast("int").alias("station_id"),
        F.col("bezeichnung").alias("station_name"),
        (F.col("velo") == "1").alias("counts_bikes"),
        (F.col("fuss") == "1").alias("counts_pedestrians"),
        F.col("richtung_in").alias("direction_in"),
        F.col("richtung_out").alias("direction_out"),
        F.col("lon").cast("double").alias("lon"),
        F.col("lat").cast("double").alias("lat"),
    )
)
save(df_bike_stations, "bike_stations")
display(spark.table(f"{SILVER}.bike_stations").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 – Car counting sites (prepared)
# MAGIC
# MAGIC The motorized traffic data uses **Swiss coordinates (LV95)**: `EKoord` (east) and `NKoord` (north).
# MAGIC Maps need **WGS84** (longitude/latitude). We use the official approximation formula of swisstopo.
# MAGIC
# MAGIC Each site (`ZSID`) has one measuring point per direction (`MSID`) – we keep one row per site.

# COMMAND ----------

def lv95_to_wgs84(e_col, n_col):
    """Approximate LV95 -> WGS84 conversion (swisstopo formula, accuracy ~1 m)."""
    y = (F.col(e_col) - 2600000) / 1000000
    x = (F.col(n_col) - 1200000) / 1000000
    lon = (2.6779094 + 4.728982 * y + 0.791484 * y * x + 0.1306 * y * x * x - 0.0436 * y * y * y) * 100 / 36
    lat = (16.9023892 + 3.238272 * x - 0.270978 * y * y - 0.002528 * x * x
           - 0.0447 * y * y * x - 0.0140 * x * x * x) * 100 / 36
    return lon, lat


lon, lat = lv95_to_wgs84("e", "n")

df_car_sites = (
    spark.table(f"{BRONZE}.car_counts")
    .groupBy(F.col("ZSID").alias("site_id"))
    .agg(
        F.max("ZSName").alias("site_name"),
        F.max("Achse").alias("street"),
        F.avg("EKoord").alias("e"),
        F.avg("NKoord").alias("n"),
    )
    .withColumn("lon", lon)
    .withColumn("lat", lat)
    .drop("e", "n")
)
save(df_car_sites, "car_sites")
display(spark.table(f"{SILVER}.car_sites").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 – Weather (prepared, in SQL)
# MAGIC The same principle works just as well in SQL – use whatever you prefer!

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mobility_silver.weather_hourly AS
# MAGIC SELECT
# MAGIC   to_timestamp(time, "yyyy-MM-dd'T'HH:mm") AS ts_hour,
# MAGIC   CAST(temperature_c    AS DOUBLE)          AS temperature_c,
# MAGIC   CAST(precipitation_mm AS DOUBLE)          AS precipitation_mm,
# MAGIC   CAST(snowfall_cm      AS DOUBLE)          AS snowfall_cm,
# MAGIC   CAST(wind_speed_kmh   AS DOUBLE)          AS wind_speed_kmh,
# MAGIC   CAST(cloud_cover_pct  AS INT)             AS cloud_cover_pct
# MAGIC FROM mobility_bronze.weather
# MAGIC WHERE time IS NOT NULL;
# MAGIC
# MAGIC SELECT * FROM mobility_silver.weather_hourly ORDER BY ts_hour DESC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3.4 – Bike & pedestrian counts per hour
# MAGIC
# MAGIC Create **`mobility_silver.bike_counts_hourly`** with **one row per station and hour**:
# MAGIC
# MAGIC | Column | How |
# MAGIC |---|---|
# MAGIC | `station_id` | `FK_STANDORT` as int |
# MAGIC | `ts_hour` | `DATUM` (text like `2025-03-14T07:45`) → timestamp → truncated to the hour |
# MAGIC | `bikes` | sum of `VELO_IN` + `VELO_OUT` (NULL counts as 0) |
# MAGIC | `bikes_in`, `bikes_out` | sums per direction |
# MAGIC | `pedestrians` | sum of `FUSS_IN` + `FUSS_OUT` (NULL counts as 0) |
# MAGIC | `has_bike_data` | `true` if the station delivered any bike value (`VELO_IN` or `VELO_OUT` not NULL) in this hour |
# MAGIC | `has_pedestrian_data` | same for `FUSS_IN` / `FUSS_OUT` |
# MAGIC
# MAGIC Why the flags? Many stations count **only pedestrians** or **only bikes**. Without the flag, a pedestrian-only
# MAGIC station would look like a bike station with 0 bikes and distort averages.
# MAGIC
# MAGIC Then add the helper columns `date`, `year`, `hour` and `is_weekend` with the function `add_time_columns` below.
# MAGIC
# MAGIC Hints:
# MAGIC - `F.to_timestamp("DATUM", "yyyy-MM-dd'T'HH:mm")`, `F.date_trunc("hour", ...)`
# MAGIC - `F.coalesce(F.col("VELO_IN"), F.lit(0))`
# MAGIC - `.groupBy("station_id", "ts_hour").agg(F.sum(...).alias(...), F.max(<boolean>)...)`
# MAGIC - 15.8 million rows in, ≈ 4 million rows out

# COMMAND ----------

def add_time_columns(df, ts_col="ts_hour"):
    """Add date, year, hour and is_weekend (Spark: dayofweek 1 = Sunday, 7 = Saturday)."""
    return (
        df.withColumn("date", F.to_date(ts_col))
          .withColumn("year", F.year(ts_col))
          .withColumn("hour", F.hour(ts_col))
          .withColumn("is_weekend", F.dayofweek(ts_col).isin(1, 7))
    )

# COMMAND ----------

# TODO Task 3.4 – build SILVER.bike_counts_hourly (station × hour)
raise NotImplementedError("Task 3.4")

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary><b>💡 Hint 1 – the idea</b></summary>
# MAGIC
# MAGIC Three steps: (1) `select` the typed columns – timestamp truncated to the hour, NULL counts replaced by 0, the two `has_..._data` flags; (2) `groupBy("station_id", "ts_hour")` and `sum` the counts, `max` the flags; (3) add `bikes = bikes_in + bikes_out` and call `add_time_columns`.
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🧩 Hint 2 – code skeleton</b></summary>
# MAGIC
# MAGIC ```python
# MAGIC zero = F.lit(0)
# MAGIC df_bike_hourly = (
# MAGIC     spark.table(f"{BRONZE}.bike_counts")
# MAGIC     .select(
# MAGIC         F.col("FK_STANDORT").cast("int").alias("station_id"),
# MAGIC         F.date_trunc("hour", F.to_timestamp("DATUM", "yyyy-MM-dd'T'HH:mm")).alias("ts_hour"),
# MAGIC         F.coalesce("VELO_IN", zero).alias("bikes_in"),
# MAGIC         F.coalesce("VELO_OUT", zero).alias("bikes_out"),
# MAGIC         (F.coalesce("FUSS_IN", zero) + F.coalesce("FUSS_OUT", zero)).alias("pedestrians"),
# MAGIC         (F.col("VELO_IN").isNotNull() | F.col("VELO_OUT").isNotNull()).alias("has_bike_data"),
# MAGIC         ___  # same for FUSS_IN / FUSS_OUT -> has_pedestrian_data
# MAGIC     )
# MAGIC     .groupBy("station_id", "ts_hour")
# MAGIC     .agg(F.sum("bikes_in").alias("bikes_in"), ___, F.max("has_bike_data").alias("has_bike_data"), ___)
# MAGIC     .withColumn("bikes", F.col("bikes_in") + F.col("bikes_out"))
# MAGIC )
# MAGIC save(add_time_columns(df_bike_hourly), "bike_counts_hourly")
# MAGIC ```
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🔑 Solution</b></summary>
# MAGIC
# MAGIC Try it yourself first – then compare.
# MAGIC
# MAGIC ```python
# MAGIC zero = F.lit(0)
# MAGIC
# MAGIC df_bike_hourly = (
# MAGIC     spark.table(f"{BRONZE}.bike_counts")
# MAGIC     .select(
# MAGIC         F.col("FK_STANDORT").cast("int").alias("station_id"),
# MAGIC         F.date_trunc("hour", F.to_timestamp("DATUM", "yyyy-MM-dd'T'HH:mm")).alias("ts_hour"),
# MAGIC         F.coalesce("VELO_IN", zero).alias("bikes_in"),
# MAGIC         F.coalesce("VELO_OUT", zero).alias("bikes_out"),
# MAGIC         (F.coalesce("FUSS_IN", zero) + F.coalesce("FUSS_OUT", zero)).alias("pedestrians"),
# MAGIC         (F.col("VELO_IN").isNotNull() | F.col("VELO_OUT").isNotNull()).alias("has_bike_data"),
# MAGIC         (F.col("FUSS_IN").isNotNull() | F.col("FUSS_OUT").isNotNull()).alias("has_pedestrian_data"),
# MAGIC     )
# MAGIC     .filter(F.col("ts_hour").isNotNull() & F.col("station_id").isNotNull())
# MAGIC     .groupBy("station_id", "ts_hour")
# MAGIC     .agg(
# MAGIC         F.sum("bikes_in").alias("bikes_in"),
# MAGIC         F.sum("bikes_out").alias("bikes_out"),
# MAGIC         F.sum("pedestrians").alias("pedestrians"),
# MAGIC         F.max("has_bike_data").alias("has_bike_data"),
# MAGIC         F.max("has_pedestrian_data").alias("has_pedestrian_data"),
# MAGIC     )
# MAGIC     .withColumn("bikes", F.col("bikes_in") + F.col("bikes_out"))
# MAGIC )
# MAGIC
# MAGIC save(add_time_columns(df_bike_hourly), "bike_counts_hourly")
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3.5 – Motorized traffic per hour
# MAGIC
# MAGIC Create **`mobility_silver.car_counts_hourly`** with **one row per counting site and hour**:
# MAGIC
# MAGIC | Column | How |
# MAGIC |---|---|
# MAGIC | `site_id` | `ZSID` |
# MAGIC | `ts_hour` | `MessungDatZeit` looks like `2025-03-14T07:00:00+01:00`. Take the **first 19 characters** (local time) and convert them to a timestamp. |
# MAGIC | `vehicles` | sum of `AnzFahrzeuge` over both directions (`MSID`) of the site |
# MAGIC
# MAGIC **Data quality:** keep only rows where `AnzFahrzeugeStatus` is `Gemessen` (measured) or `Imputiert` (imputed)
# MAGIC and `AnzFahrzeuge` is not NULL. Rows marked `Fehlend` (missing) must be dropped.
# MAGIC
# MAGIC Finally add the time columns with `add_time_columns`.
# MAGIC
# MAGIC Hints: `F.substring("MessungDatZeit", 1, 19)`, format `"yyyy-MM-dd'T'HH:mm:ss"`, `.isin(...)`

# COMMAND ----------

# TODO Task 3.5 – build SILVER.car_counts_hourly (site × hour)
raise NotImplementedError("Task 3.5")

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary><b>💡 Hint 1 – the idea</b></summary>
# MAGIC
# MAGIC First `filter` the valid rows (`AnzFahrzeugeStatus` in `Gemessen`/`Imputiert`, `AnzFahrzeuge` not NULL), then select `site_id`, `ts_hour` (first 19 characters of `MessungDatZeit` → timestamp) and `vehicles`, then `groupBy("site_id", "ts_hour")` and sum the vehicles of both directions.
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🧩 Hint 2 – code skeleton</b></summary>
# MAGIC
# MAGIC ```python
# MAGIC df_car_hourly = (
# MAGIC     spark.table(f"{BRONZE}.car_counts")
# MAGIC     .filter(F.col("AnzFahrzeugeStatus").isin(___, ___) & F.col("AnzFahrzeuge").isNotNull())
# MAGIC     .select(
# MAGIC         F.col("ZSID").alias("site_id"),
# MAGIC         F.to_timestamp(F.substring("MessungDatZeit", 1, ___), "yyyy-MM-dd'T'HH:mm:ss").alias("ts_hour"),
# MAGIC         F.col("AnzFahrzeuge").alias("vehicles"),
# MAGIC     )
# MAGIC     .groupBy(___, ___)
# MAGIC     .agg(F.sum("vehicles").alias("vehicles"))
# MAGIC )
# MAGIC save(add_time_columns(df_car_hourly), "car_counts_hourly")
# MAGIC ```
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🔑 Solution</b></summary>
# MAGIC
# MAGIC Try it yourself first – then compare.
# MAGIC
# MAGIC ```python
# MAGIC df_car_hourly = (
# MAGIC     spark.table(f"{BRONZE}.car_counts")
# MAGIC     .filter(F.col("AnzFahrzeugeStatus").isin("Gemessen", "Imputiert") & F.col("AnzFahrzeuge").isNotNull())
# MAGIC     .select(
# MAGIC         F.col("ZSID").alias("site_id"),
# MAGIC         F.to_timestamp(F.substring("MessungDatZeit", 1, 19), "yyyy-MM-dd'T'HH:mm:ss").alias("ts_hour"),
# MAGIC         F.col("AnzFahrzeuge").alias("vehicles"),
# MAGIC     )
# MAGIC     .groupBy("site_id", "ts_hour")
# MAGIC     .agg(F.sum("vehicles").alias("vehicles"))
# MAGIC )
# MAGIC
# MAGIC save(add_time_columns(df_car_hourly), "car_counts_hourly")
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bike_counts_hourly' AS table_name, COUNT(*) AS rows, MIN(ts_hour) AS first_hour, MAX(ts_hour) AS last_hour,
# MAGIC        COUNT(DISTINCT station_id) AS locations, SUM(bikes) AS total
# MAGIC FROM mobility_silver.bike_counts_hourly
# MAGIC UNION ALL
# MAGIC SELECT 'car_counts_hourly', COUNT(*), MIN(ts_hour), MAX(ts_hour), COUNT(DISTINCT site_id), SUM(vehicles)
# MAGIC FROM mobility_silver.car_counts_hourly

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🤔 Data quality: how many stations count per year?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT b.year, b.bike_stations, c.car_sites
# MAGIC FROM (SELECT year, COUNT(DISTINCT station_id) AS bike_stations FROM mobility_silver.bike_counts_hourly GROUP BY year) b
# MAGIC FULL OUTER JOIN (SELECT year, COUNT(DISTINCT site_id) AS car_sites FROM mobility_silver.car_counts_hourly GROUP BY year) c
# MAGIC   USING (year)
# MAGIC ORDER BY year

# COMMAND ----------

# MAGIC %md
# MAGIC The number of bike stations changes a lot over the years (from 1 to over 60). **What does this mean if we simply add up
# MAGIC all bikes per year?** Keep this in mind for the gold layer – we will compare **averages per station** instead of totals.
# MAGIC
# MAGIC 👉 Continue with **step_4_gold**
