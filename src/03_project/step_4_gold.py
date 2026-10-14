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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Copy the code of <code>df_bike_daily</code> above and adapt it: table <code>car_counts_hourly</code>, sum of <code>vehicles</code>, distinct count of <code>site_id</code>. No filter needed.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>df_car_daily = (
# MAGIC     spark.table(f&quot;{SILVER}.car_counts_hourly&quot;)
# MAGIC     .groupBy(&quot;date&quot;)
# MAGIC     .agg(F.sum(___).alias(&quot;vehicles&quot;), F.countDistinct(___).alias(&quot;car_sites&quot;))
# MAGIC )</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>df_car_daily = (
# MAGIC     spark.table(f&quot;{SILVER}.car_counts_hourly&quot;)
# MAGIC     .groupBy(&quot;date&quot;)
# MAGIC     .agg(F.sum(&quot;vehicles&quot;).alias(&quot;vehicles&quot;), F.countDistinct(&quot;site_id&quot;).alias(&quot;car_sites&quot;))
# MAGIC )</code></pre></details>

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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>(1) <code>df_weather_daily</code>: group the hourly weather by <code>F.to_date(&quot;ts_hour&quot;)</code>. (2) Start with <code>df_weather_daily</code> and add the three traffic DataFrames with <code>.join(other, &quot;date&quot;, &quot;left&quot;)</code>. (3) Add the derived columns with <code>withColumn</code>. (4) <code>save(df_daily, &quot;daily_traffic&quot;)</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>df_weather_daily = (
# MAGIC     spark.table(f&quot;{SILVER}.weather_hourly&quot;)
# MAGIC     .groupBy(F.to_date(&quot;ts_hour&quot;).alias(&quot;date&quot;))
# MAGIC     .agg(F.round(F.avg(&quot;temperature_c&quot;), 1).alias(&quot;temp_mean_c&quot;), F.max(___).alias(&quot;temp_max_c&quot;),
# MAGIC          F.round(F.sum(&quot;precipitation_mm&quot;), 1).alias(&quot;precipitation_mm&quot;), F.round(F.sum(___), 1).alias(&quot;snowfall_cm&quot;))
# MAGIC )
# MAGIC df_daily = (
# MAGIC     df_weather_daily
# MAGIC     .join(df_bike_daily, &quot;date&quot;, &quot;left&quot;)
# MAGIC     .join(___, &quot;date&quot;, &quot;left&quot;)
# MAGIC     .join(___, &quot;date&quot;, &quot;left&quot;)
# MAGIC     .withColumn(&quot;year&quot;, F.year(&quot;date&quot;))
# MAGIC     .withColumn(&quot;weekday&quot;, F.date_format(&quot;date&quot;, &quot;EEEE&quot;))
# MAGIC     .withColumn(&quot;is_weekend&quot;, F.dayofweek(&quot;date&quot;).isin(1, 7))
# MAGIC     .withColumn(&quot;bikes_per_station&quot;, F.round(F.col(&quot;bikes&quot;) / F.col(&quot;bike_stations&quot;)))
# MAGIC     # ... month, pedestrians_per_station, vehicles_per_site, is_rainy_day
# MAGIC )
# MAGIC save(df_daily, &quot;daily_traffic&quot;)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>df_weather_daily = (
# MAGIC     spark.table(f&quot;{SILVER}.weather_hourly&quot;)
# MAGIC     .groupBy(F.to_date(&quot;ts_hour&quot;).alias(&quot;date&quot;))
# MAGIC     .agg(
# MAGIC         F.round(F.avg(&quot;temperature_c&quot;), 1).alias(&quot;temp_mean_c&quot;),
# MAGIC         F.max(&quot;temperature_c&quot;).alias(&quot;temp_max_c&quot;),
# MAGIC         F.round(F.sum(&quot;precipitation_mm&quot;), 1).alias(&quot;precipitation_mm&quot;),
# MAGIC         F.round(F.sum(&quot;snowfall_cm&quot;), 1).alias(&quot;snowfall_cm&quot;),
# MAGIC     )
# MAGIC )
# MAGIC 
# MAGIC df_daily = (
# MAGIC     df_weather_daily
# MAGIC     .join(df_bike_daily, &quot;date&quot;, &quot;left&quot;)
# MAGIC     .join(df_ped_daily, &quot;date&quot;, &quot;left&quot;)
# MAGIC     .join(df_car_daily, &quot;date&quot;, &quot;left&quot;)
# MAGIC     .withColumn(&quot;year&quot;, F.year(&quot;date&quot;))
# MAGIC     .withColumn(&quot;month&quot;, F.month(&quot;date&quot;))
# MAGIC     .withColumn(&quot;weekday&quot;, F.date_format(&quot;date&quot;, &quot;EEEE&quot;))
# MAGIC     .withColumn(&quot;is_weekend&quot;, F.dayofweek(&quot;date&quot;).isin(1, 7))
# MAGIC     .withColumn(&quot;bikes_per_station&quot;, F.round(F.col(&quot;bikes&quot;) / F.col(&quot;bike_stations&quot;)))
# MAGIC     .withColumn(&quot;pedestrians_per_station&quot;, F.round(F.col(&quot;pedestrians&quot;) / F.col(&quot;pedestrian_stations&quot;)))
# MAGIC     .withColumn(&quot;vehicles_per_site&quot;, F.round(F.col(&quot;vehicles&quot;) / F.col(&quot;car_sites&quot;)))
# MAGIC     .withColumn(&quot;is_rainy_day&quot;, F.col(&quot;precipitation_mm&quot;) &gt;= 1.0)
# MAGIC     .select(
# MAGIC         &quot;date&quot;, &quot;year&quot;, &quot;month&quot;, &quot;weekday&quot;, &quot;is_weekend&quot;,
# MAGIC         &quot;bikes&quot;, &quot;bike_stations&quot;, &quot;bikes_per_station&quot;,
# MAGIC         &quot;pedestrians&quot;, &quot;pedestrian_stations&quot;, &quot;pedestrians_per_station&quot;,
# MAGIC         &quot;vehicles&quot;, &quot;car_sites&quot;, &quot;vehicles_per_site&quot;,
# MAGIC         &quot;temp_mean_c&quot;, &quot;temp_max_c&quot;, &quot;precipitation_mm&quot;, &quot;snowfall_cm&quot;, &quot;is_rainy_day&quot;,
# MAGIC     )
# MAGIC )
# MAGIC 
# MAGIC save(df_daily, &quot;daily_traffic&quot;)</code></pre></details>

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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Write a small function <code>profile(df, value_col, mode)</code> that groups by <code>is_weekend</code> and <code>hour</code>, averages the value column and adds <code>mode</code> and <code>day_type</code>. Call it once for bikes and once for cars and combine with <code>unionByName</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def profile(df, value_col, mode):
# MAGIC     return (
# MAGIC         df.groupBy(&quot;is_weekend&quot;, &quot;hour&quot;)
# MAGIC         .agg(F.round(F.avg(value_col), 1).alias(&quot;avg_per_location&quot;))
# MAGIC         .select(F.lit(mode).alias(&quot;mode&quot;),
# MAGIC                 F.when(F.col(&quot;is_weekend&quot;), &quot;Weekend&quot;).otherwise(___).alias(&quot;day_type&quot;),
# MAGIC                 &quot;hour&quot;, &quot;avg_per_location&quot;)
# MAGIC     )
# MAGIC 
# MAGIC df_bikes = bike_hourly.filter(F.col(&quot;has_bike_data&quot;) &amp; (F.col(&quot;year&quot;) == YEAR))
# MAGIC df_cars = spark.table(f&quot;{SILVER}.car_counts_hourly&quot;).filter(F.col(&quot;year&quot;) == YEAR)
# MAGIC save(profile(df_bikes, &quot;bikes&quot;, &quot;Bike&quot;).unionByName(profile(df_cars, ___, ___)), &quot;hourly_profile&quot;)</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def profile(df, value_col, mode):
# MAGIC     return (
# MAGIC         df.groupBy(&quot;is_weekend&quot;, &quot;hour&quot;)
# MAGIC         .agg(F.round(F.avg(value_col), 1).alias(&quot;avg_per_location&quot;))
# MAGIC         .select(
# MAGIC             F.lit(mode).alias(&quot;mode&quot;),
# MAGIC             F.when(F.col(&quot;is_weekend&quot;), &quot;Weekend&quot;).otherwise(&quot;Weekday&quot;).alias(&quot;day_type&quot;),
# MAGIC             &quot;hour&quot;,
# MAGIC             &quot;avg_per_location&quot;,
# MAGIC         )
# MAGIC     )
# MAGIC 
# MAGIC 
# MAGIC df_profile = profile(
# MAGIC     bike_hourly.filter(F.col(&quot;has_bike_data&quot;) &amp; (F.col(&quot;year&quot;) == YEAR)), &quot;bikes&quot;, &quot;Bike&quot;
# MAGIC ).unionByName(
# MAGIC     profile(spark.table(f&quot;{SILVER}.car_counts_hourly&quot;).filter(F.col(&quot;year&quot;) == YEAR), &quot;vehicles&quot;, &quot;Car&quot;)
# MAGIC )
# MAGIC 
# MAGIC save(df_profile, &quot;hourly_profile&quot;)</code></pre></details>

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
