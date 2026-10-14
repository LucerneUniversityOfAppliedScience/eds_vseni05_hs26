# Databricks notebook source
# MAGIC %md
# MAGIC # Step 4 – Gold: join the sources into analysis-ready tables
# MAGIC
# MAGIC **Goal (≈ 40 min):** Combine bikes, pedestrians, cars and weather into a few tables that directly answer questions
# MAGIC and feed the dashboard in step 6.
# MAGIC
# MAGIC | Gold table | Grain | Used for |
# MAGIC |---|---|---|
# MAGIC | `daily_traffic` | one row per day | trends, weather effects, KPIs |
# MAGIC | `yearly_trend` | one row per year | long-term development bike vs. car |
# MAGIC | `location_map` | one row per counting location (2025) | map |
# MAGIC | `hourly_profile` | mode × day type × hour (2025) | rush hours |
# MAGIC
# MAGIC ### Why "per station"?
# MAGIC The number of active counting stations changes over the years (step 3). Totals would mostly show *how many stations
# MAGIC existed*. We therefore also compute **averages per active station / site and day**.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

SILVER = f"{CATALOG}.mobility_silver"
GOLD = f"{CATALOG}.mobility_gold"
spark.sql(f"USE CATALOG {CATALOG}")


def save(df, table):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.{table}")
    print(f"{GOLD}.{table}: {spark.table(f'{GOLD}.{table}').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 – Daily bikes and pedestrians (prepared)
# MAGIC Only hours in which a station actually delivered bike (or pedestrian) values are counted.

# COMMAND ----------

bike_hourly = spark.table(f"{SILVER}.bike_counts_hourly")

df_bike_daily = (
    bike_hourly.filter("has_bike_data")
    .groupBy("date")
    .agg(F.sum("bikes").alias("bikes"), F.countDistinct("station_id").alias("bike_stations"))
)

df_ped_daily = (
    bike_hourly.filter("has_pedestrian_data")
    .groupBy("date")
    .agg(F.sum("pedestrians").alias("pedestrians"), F.countDistinct("station_id").alias("pedestrian_stations"))
)

display(df_bike_daily.orderBy(F.desc("date")).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4.2 – Daily motorized traffic
# MAGIC
# MAGIC Build `df_car_daily` from `mobility_silver.car_counts_hourly` with one row per `date`:
# MAGIC - `vehicles` = sum of vehicles
# MAGIC - `car_sites` = number of distinct `site_id`
# MAGIC
# MAGIC *(Same pattern as `df_bike_daily` above.)*

# COMMAND ----------

# TODO Task 4.2 – df_car_daily with vehicles and car_sites per date
raise NotImplementedError("Task 4.2")

display(df_car_daily.orderBy(F.desc("date")).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary><b>💡 Hint 1 – the idea</b></summary>
# MAGIC
# MAGIC Copy the code of `df_bike_daily` above and adapt it: table `car_counts_hourly`, sum of `vehicles`, distinct count of `site_id`. No filter needed.
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🧩 Hint 2 – code skeleton</b></summary>
# MAGIC
# MAGIC ```python
# MAGIC df_car_daily = (
# MAGIC     spark.table(f"{SILVER}.car_counts_hourly")
# MAGIC     .groupBy("date")
# MAGIC     .agg(F.sum(___).alias("vehicles"), F.countDistinct(___).alias("car_sites"))
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🔑 Solution</b></summary>
# MAGIC
# MAGIC Try it yourself first – then compare.
# MAGIC
# MAGIC ```python
# MAGIC df_car_daily = (
# MAGIC     spark.table(f"{SILVER}.car_counts_hourly")
# MAGIC     .groupBy("date")
# MAGIC     .agg(F.sum("vehicles").alias("vehicles"), F.countDistinct("site_id").alias("car_sites"))
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4.3 – Daily weather and the big join
# MAGIC
# MAGIC 1. Build `df_weather_daily` from `mobility_silver.weather_hourly` with one row per `date`:
# MAGIC    `temp_mean_c` (average, rounded to 1 decimal), `temp_max_c` (max), `precipitation_mm` (sum, rounded), `snowfall_cm` (sum)
# MAGIC 2. Join **all four daily DataFrames** on `date`. Use the weather as the "spine" (it has every day) and
# MAGIC    **left join** the traffic data to it.
# MAGIC 3. Add these columns:
# MAGIC
# MAGIC | Column | Definition |
# MAGIC |---|---|
# MAGIC | `year`, `month` | from `date` |
# MAGIC | `weekday` | name of the day, e.g. `Monday` → `F.date_format("date", "EEEE")` |
# MAGIC | `is_weekend` | Saturday or Sunday → `F.dayofweek("date").isin(1, 7)` |
# MAGIC | `bikes_per_station` | `bikes / bike_stations`, rounded |
# MAGIC | `pedestrians_per_station` | `pedestrians / pedestrian_stations`, rounded |
# MAGIC | `vehicles_per_site` | `vehicles / car_sites`, rounded |
# MAGIC | `is_rainy_day` | `precipitation_mm >= 1.0` |
# MAGIC
# MAGIC 4. Save the result as **`mobility_gold.daily_traffic`**.

# COMMAND ----------

# TODO Task 4.3 – df_weather_daily, join everything, add columns, save GOLD.daily_traffic
raise NotImplementedError("Task 4.3")

display(spark.table(f"{GOLD}.daily_traffic").orderBy(F.desc("date")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary><b>💡 Hint 1 – the idea</b></summary>
# MAGIC
# MAGIC (1) `df_weather_daily`: group the hourly weather by `F.to_date("ts_hour")`. (2) Start with `df_weather_daily` and add the three traffic DataFrames with `.join(other, "date", "left")`. (3) Add the derived columns with `withColumn`. (4) `save(df_daily, "daily_traffic")`.
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🧩 Hint 2 – code skeleton</b></summary>
# MAGIC
# MAGIC ```python
# MAGIC df_weather_daily = (
# MAGIC     spark.table(f"{SILVER}.weather_hourly")
# MAGIC     .groupBy(F.to_date("ts_hour").alias("date"))
# MAGIC     .agg(F.round(F.avg("temperature_c"), 1).alias("temp_mean_c"), F.max(___).alias("temp_max_c"),
# MAGIC          F.round(F.sum("precipitation_mm"), 1).alias("precipitation_mm"), F.round(F.sum(___), 1).alias("snowfall_cm"))
# MAGIC )
# MAGIC df_daily = (
# MAGIC     df_weather_daily
# MAGIC     .join(df_bike_daily, "date", "left")
# MAGIC     .join(___, "date", "left")
# MAGIC     .join(___, "date", "left")
# MAGIC     .withColumn("year", F.year("date"))
# MAGIC     .withColumn("weekday", F.date_format("date", "EEEE"))
# MAGIC     .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
# MAGIC     .withColumn("bikes_per_station", F.round(F.col("bikes") / F.col("bike_stations")))
# MAGIC     # ... month, pedestrians_per_station, vehicles_per_site, is_rainy_day
# MAGIC )
# MAGIC save(df_daily, "daily_traffic")
# MAGIC ```
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🔑 Solution</b></summary>
# MAGIC
# MAGIC Try it yourself first – then compare.
# MAGIC
# MAGIC ```python
# MAGIC df_weather_daily = (
# MAGIC     spark.table(f"{SILVER}.weather_hourly")
# MAGIC     .groupBy(F.to_date("ts_hour").alias("date"))
# MAGIC     .agg(
# MAGIC         F.round(F.avg("temperature_c"), 1).alias("temp_mean_c"),
# MAGIC         F.max("temperature_c").alias("temp_max_c"),
# MAGIC         F.round(F.sum("precipitation_mm"), 1).alias("precipitation_mm"),
# MAGIC         F.round(F.sum("snowfall_cm"), 1).alias("snowfall_cm"),
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC df_daily = (
# MAGIC     df_weather_daily
# MAGIC     .join(df_bike_daily, "date", "left")
# MAGIC     .join(df_ped_daily, "date", "left")
# MAGIC     .join(df_car_daily, "date", "left")
# MAGIC     .withColumn("year", F.year("date"))
# MAGIC     .withColumn("month", F.month("date"))
# MAGIC     .withColumn("weekday", F.date_format("date", "EEEE"))
# MAGIC     .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
# MAGIC     .withColumn("bikes_per_station", F.round(F.col("bikes") / F.col("bike_stations")))
# MAGIC     .withColumn("pedestrians_per_station", F.round(F.col("pedestrians") / F.col("pedestrian_stations")))
# MAGIC     .withColumn("vehicles_per_site", F.round(F.col("vehicles") / F.col("car_sites")))
# MAGIC     .withColumn("is_rainy_day", F.col("precipitation_mm") >= 1.0)
# MAGIC     .select(
# MAGIC         "date", "year", "month", "weekday", "is_weekend",
# MAGIC         "bikes", "bike_stations", "bikes_per_station",
# MAGIC         "pedestrians", "pedestrian_stations", "pedestrians_per_station",
# MAGIC         "vehicles", "car_sites", "vehicles_per_site",
# MAGIC         "temp_mean_c", "temp_max_c", "precipitation_mm", "snowfall_cm", "is_rainy_day",
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC save(df_daily, "daily_traffic")
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 – Yearly trend (prepared, in SQL)
# MAGIC Average traffic **per station/site and day**, only for complete years.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mobility_gold.yearly_trend AS
# MAGIC SELECT year,
# MAGIC        ROUND(AVG(bikes_per_station))        AS avg_daily_bikes_per_station,
# MAGIC        ROUND(AVG(pedestrians_per_station))  AS avg_daily_pedestrians_per_station,
# MAGIC        ROUND(AVG(vehicles_per_site))        AS avg_daily_vehicles_per_site,
# MAGIC        ROUND(AVG(bike_stations))            AS avg_active_bike_stations,
# MAGIC        ROUND(AVG(car_sites))                AS avg_active_car_sites,
# MAGIC        SUM(bikes)                           AS bikes_total,
# MAGIC        SUM(vehicles)                        AS vehicles_total
# MAGIC FROM mobility_gold.daily_traffic
# MAGIC WHERE year BETWEEN 2011 AND 2025
# MAGIC GROUP BY year;
# MAGIC
# MAGIC SELECT * FROM mobility_gold.yearly_trend ORDER BY year;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 – Map of all counting locations in 2025 (prepared)
# MAGIC One row per location with its average daily volume – bikes and cars in **one** table, distinguished by `mode`.

# COMMAND ----------

YEAR = 2025

df_bike_locations = (
    bike_hourly.filter(F.col("has_bike_data") & (F.col("year") == YEAR))
    .groupBy("station_id")
    .agg((F.sum("bikes") / F.countDistinct("date")).alias("avg_daily_count"))
    .join(spark.table(f"{SILVER}.bike_stations"), "station_id")
    .select(F.lit("Bike").alias("mode"), F.col("station_name").alias("location_name"), "lat", "lon",
            F.round("avg_daily_count").alias("avg_daily_count"))
)

df_car_locations = (
    spark.table(f"{SILVER}.car_counts_hourly").filter(F.col("year") == YEAR)
    .groupBy("site_id")
    .agg((F.sum("vehicles") / F.countDistinct("date")).alias("avg_daily_count"))
    .join(spark.table(f"{SILVER}.car_sites"), "site_id")
    .select(F.lit("Car").alias("mode"), F.col("site_name").alias("location_name"), "lat", "lon",
            F.round("avg_daily_count").alias("avg_daily_count"))
)

save(df_bike_locations.unionByName(df_car_locations), "location_map")
display(spark.table(f"{GOLD}.location_map").orderBy(F.desc("avg_daily_count")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4.6 – Hourly profile: when do people bike, when do they drive?
# MAGIC
# MAGIC Create **`mobility_gold.hourly_profile`** for the year **2025** with the columns
# MAGIC `mode` (`'Bike'` or `'Car'`), `day_type` (`'Weekday'` or `'Weekend'`), `hour` and
# MAGIC `avg_per_location` = average count **per station (or site) and hour**.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Bikes: from `bike_counts_hourly` (2025, `has_bike_data`) → group by `is_weekend`, `hour` → `avg("bikes")`
# MAGIC 2. Cars: from `car_counts_hourly` (2025) → group by `is_weekend`, `hour` → `avg("vehicles")`
# MAGIC 3. Add `mode`, turn `is_weekend` into `day_type` with `F.when(...).otherwise(...)`, round the average to 1 decimal
# MAGIC 4. Combine both with `unionByName` and save.
# MAGIC
# MAGIC *Using SQL instead? Sure – `CREATE OR REPLACE TABLE ... AS SELECT ... UNION ALL SELECT ...` works just as well.*

# COMMAND ----------

# TODO Task 4.6 – build GOLD.hourly_profile (mode, day_type, hour, avg_per_location) for 2025
raise NotImplementedError("Task 4.6")

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary><b>💡 Hint 1 – the idea</b></summary>
# MAGIC
# MAGIC Write a small function `profile(df, value_col, mode)` that groups by `is_weekend` and `hour`, averages the value column and adds `mode` and `day_type`. Call it once for bikes and once for cars and combine with `unionByName`.
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🧩 Hint 2 – code skeleton</b></summary>
# MAGIC
# MAGIC ```python
# MAGIC def profile(df, value_col, mode):
# MAGIC     return (
# MAGIC         df.groupBy("is_weekend", "hour")
# MAGIC         .agg(F.round(F.avg(value_col), 1).alias("avg_per_location"))
# MAGIC         .select(F.lit(mode).alias("mode"),
# MAGIC                 F.when(F.col("is_weekend"), "Weekend").otherwise(___).alias("day_type"),
# MAGIC                 "hour", "avg_per_location")
# MAGIC     )
# MAGIC
# MAGIC df_bikes = bike_hourly.filter(F.col("has_bike_data") & (F.col("year") == YEAR))
# MAGIC df_cars = spark.table(f"{SILVER}.car_counts_hourly").filter(F.col("year") == YEAR)
# MAGIC save(profile(df_bikes, "bikes", "Bike").unionByName(profile(df_cars, ___, ___)), "hourly_profile")
# MAGIC ```
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🔑 Solution</b></summary>
# MAGIC
# MAGIC Try it yourself first – then compare.
# MAGIC
# MAGIC ```python
# MAGIC def profile(df, value_col, mode):
# MAGIC     return (
# MAGIC         df.groupBy("is_weekend", "hour")
# MAGIC         .agg(F.round(F.avg(value_col), 1).alias("avg_per_location"))
# MAGIC         .select(
# MAGIC             F.lit(mode).alias("mode"),
# MAGIC             F.when(F.col("is_weekend"), "Weekend").otherwise("Weekday").alias("day_type"),
# MAGIC             "hour",
# MAGIC             "avg_per_location",
# MAGIC         )
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC df_profile = profile(
# MAGIC     bike_hourly.filter(F.col("has_bike_data") & (F.col("year") == YEAR)), "bikes", "Bike"
# MAGIC ).unionByName(
# MAGIC     profile(spark.table(f"{SILVER}.car_counts_hourly").filter(F.col("year") == YEAR), "vehicles", "Car")
# MAGIC )
# MAGIC
# MAGIC save(df_profile, "hourly_profile")
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Check

# COMMAND ----------

for table in ["daily_traffic", "yearly_trend", "location_map", "hourly_profile"]:
    n = spark.table(f"{GOLD}.{table}").count()
    print(f"{'✅' if n > 0 else '❌'} {GOLD}.{table:<15} {n:>7,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC 🔎 **Lineage:** open `mobility_gold.daily_traffic` in the Catalog Explorer → tab **Lineage** → **See lineage graph**.
# MAGIC Unity Catalog recorded automatically which tables it was built from.
# MAGIC
# MAGIC 👉 Continue with **step_5_analysis**
