# Databricks notebook source
# MAGIC %md
# MAGIC # Step 5 – Analysis: what do the data tell us about sustainable mobility?
# MAGIC
# MAGIC **Goal (≈ 45 min):** Answer questions with the gold tables from step 4. Every question comes with a
# MAGIC cell to fill in – use **SQL** (`%sql` cells, prepared) or switch to Python if you prefer.
# MAGIC Create a **visualization** wherever it helps (+ → Visualization). The ones marked 📊 are good candidates
# MAGIC for your dashboard in step 6.
# MAGIC
# MAGIC | Table | Grain | Important columns |
# MAGIC |---|---|---|
# MAGIC | `mobility_gold.daily_traffic` | day | `date`, `year`, `month`, `weekday`, `is_weekend`, `bikes_per_station`, `pedestrians_per_station`, `vehicles_per_site`, `bikes`, `vehicles`, `temp_mean_c`, `precipitation_mm`, `is_rainy_day` |
# MAGIC | `mobility_gold.yearly_trend` | year | `avg_daily_bikes_per_station`, `avg_daily_vehicles_per_site`, `avg_active_bike_stations`, `bikes_total`, … |
# MAGIC | `mobility_gold.location_map` | location (2025) | `mode`, `location_name`, `lat`, `lon`, `avg_daily_count` |
# MAGIC | `mobility_gold.hourly_profile` | mode × day type × hour (2025) | `mode`, `day_type`, `hour`, `avg_per_location` |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
spark.sql(f"USE CATALOG {dbutils.widgets.get('catalog')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Question 1 – Long-term trend 📊
# MAGIC
# MAGIC How did the average daily traffic **per station** develop for bikes and **per site** for cars between 2011 and 2025?
# MAGIC 1. Show both columns from `yearly_trend` over the years as a **line chart**.
# MAGIC 2. By how many percent did each change from **2015 to 2025**?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO Question 1a – yearly trend bikes per station vs vehicles per site
# MAGIC SELECT 'replace this line with your query' AS todo

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO Question 1b – percent change 2015 -> 2025
# MAGIC SELECT 'replace this line with your query' AS todo

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Question 2 – The COVID-19 lockdown in spring 2020
# MAGIC
# MAGIC Switzerland was in lockdown from **16 March to 10 May 2020**. Compare this period on **weekdays** with the same
# MAGIC period in **2019**: average `bikes_per_station` and `vehicles_per_site`. What happened to bikes, what to cars?
# MAGIC
# MAGIC *Tip:* `date_format(date, 'MM-dd') BETWEEN '03-16' AND '05-10'`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO Question 2 – lockdown 2020 vs same period 2019 (weekdays)
# MAGIC SELECT 'replace this line with your query' AS todo

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Question 3 – Does rain stop cyclists – and drivers? 🌧️ 📊
# MAGIC
# MAGIC For **weekdays in 2023–2025**, compare the average `bikes_per_station` and `vehicles_per_site`
# MAGIC on **rainy days** (`is_rainy_day`) with **dry days**. How large is the difference in percent for each mode?
# MAGIC
# MAGIC 🤔 Why is it a good idea to exclude weekends? And why only recent years?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO Question 3 – rainy vs dry weekdays 2023-2025
# MAGIC SELECT 'replace this line with your query' AS todo

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Question 4 – Temperature 🌡️
# MAGIC
# MAGIC 1. Compute the **correlation** between `temp_mean_c` and `bikes_per_station`, and between `temp_mean_c` and
# MAGIC    `vehicles_per_site` (weekdays, 2023–2025). *SQL:* `corr(x, y)`
# MAGIC 2. Create a **scatter plot** with `temp_mean_c` on the x-axis and `bikes_per_station` on the y-axis.
# MAGIC 3. Correlation is not causation: what else could be behind the relationship?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO Question 4 – correlation temperature vs bikes / vehicles
# MAGIC SELECT 'replace this line with your query' AS todo

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Question 5 – Rush hours ⏰ 📊
# MAGIC
# MAGIC Using `hourly_profile`: at which **hour** is the traffic highest – for each `mode` and `day_type`?
# MAGIC Then draw a line chart for bikes (x: `hour`, y: `avg_per_location`, color: `day_type`).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO Question 5 – peak hour per mode and day type
# MAGIC SELECT 'replace this line with your query' AS todo

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Question 6 – Hotspots 📍 📊
# MAGIC
# MAGIC Which are the **5 busiest bike stations** and the **5 busiest car counting sites** in 2025 (`location_map`)?
# MAGIC *Bonus:* create a **map** visualization (+ → Visualization → Map (Markers)) from `location_map`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO Question 6 – top 5 per mode
# MAGIC SELECT 'replace this line with your query' AS todo

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Question 7 – Data quality: totals lie 🤥
# MAGIC
# MAGIC Show `bikes_total` and `avg_active_bike_stations` per year from `yearly_trend`.
# MAGIC Compare 2014 with 2024: the totals and the per-station average tell **different stories**. Which one is right – and why?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO Question 7 – totals vs number of stations
# MAGIC SELECT 'replace this line with your query' AS todo

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussion:** the total grows mainly because stations are added or removed. A station-weighted average is better,
# MAGIC but still not perfect (new stations may be placed where traffic is high or low). The cleanest approach is a
# MAGIC **panel**: only stations that counted in *all* years you compare – a nice extension if you have time!

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Question 8 – And what does this mean for the climate? 🌍
# MAGIC
# MAGIC A **rough, deliberately simplified estimate**: how much CO₂ was *possibly* saved by the bikes counted at the busiest
# MAGIC bike station in 2025? Use the assumptions below (change them!) and compute
# MAGIC `avg_daily_count × 365 × share_replacing_car × trip_km × kg_co2_per_km / 1000` (tonnes).
# MAGIC
# MAGIC 🤔 Why can't we add up the bikes of all stations to get the number of bike trips in Zurich?
# MAGIC Which assumption influences the result the most?

# COMMAND ----------

# Illustrative assumptions – NOT official figures, change them and discuss!
SHARE_REPLACING_CAR = 0.25   # share of bike trips that would otherwise be made by car
AVG_TRIP_KM = 4.0            # average length of a bike trip in km
CAR_KG_CO2_PER_KM = 0.19     # emissions of an average passenger car in kg CO2 per km

# TODO Question 8 – CO2 estimate for the busiest bike station
raise NotImplementedError("Question 8")

# COMMAND ----------

# MAGIC %md
# MAGIC 👉 Continue with **step 6 – dashboard** (see the project README).
