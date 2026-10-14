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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Simply select the year and the two average columns from <code>yearly_trend</code>, ordered by year.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT year, ___, ___
# MAGIC FROM mobility_gold.yearly_trend
# MAGIC ORDER BY year</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT year, avg_daily_bikes_per_station, avg_daily_vehicles_per_site
# MAGIC FROM mobility_gold.yearly_trend
# MAGIC ORDER BY year</code></pre></details>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO Question 1b – percent change 2015 -> 2025
# MAGIC SELECT 'replace this line with your query' AS todo

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Pick the 2025 and the 2015 value with a conditional aggregate <code>MAX(x) FILTER (WHERE year = 2025)</code> and compute <code>(new / old - 1) * 100</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT ROUND((MAX(avg_daily_bikes_per_station) FILTER (WHERE year = 2025)
# MAGIC             / MAX(avg_daily_bikes_per_station) FILTER (WHERE year = ___) - 1) * 100, 1) AS bikes_change_pct,
# MAGIC        ___ AS vehicles_change_pct
# MAGIC FROM mobility_gold.yearly_trend</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
# MAGIC   ROUND((MAX(avg_daily_bikes_per_station) FILTER (WHERE year = 2025)
# MAGIC        / MAX(avg_daily_bikes_per_station) FILTER (WHERE year = 2015) - 1) * 100, 1) AS bikes_change_pct,
# MAGIC   ROUND((MAX(avg_daily_vehicles_per_site) FILTER (WHERE year = 2025)
# MAGIC        / MAX(avg_daily_vehicles_per_site) FILTER (WHERE year = 2015) - 1) * 100, 1) AS vehicles_change_pct
# MAGIC FROM mobility_gold.yearly_trend</code></pre></details>

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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Filter <code>daily_traffic</code> on the two years, on weekdays and on the date range (month-day as text), then group by year.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT year, ROUND(AVG(bikes_per_station)) AS avg_bikes_per_station, ROUND(AVG(___)) AS avg_vehicles_per_site
# MAGIC FROM mobility_gold.daily_traffic
# MAGIC WHERE year IN (2019, 2020)
# MAGIC   AND NOT is_weekend
# MAGIC   AND date_format(date, &#x27;MM-dd&#x27;) BETWEEN &#x27;___&#x27; AND &#x27;___&#x27;
# MAGIC GROUP BY year</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT year,
# MAGIC        ROUND(AVG(bikes_per_station))  AS avg_bikes_per_station,
# MAGIC        ROUND(AVG(vehicles_per_site))  AS avg_vehicles_per_site
# MAGIC FROM mobility_gold.daily_traffic
# MAGIC WHERE year IN (2019, 2020)
# MAGIC   AND NOT is_weekend
# MAGIC   AND date_format(date, &#x27;MM-dd&#x27;) BETWEEN &#x27;03-16&#x27; AND &#x27;05-10&#x27;
# MAGIC GROUP BY year
# MAGIC ORDER BY year</code></pre></details>

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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Start simple: <code>GROUP BY is_rainy_day</code> with <code>AVG(bikes_per_station)</code> and <code>AVG(vehicles_per_site)</code> (weekdays, 2023–2025). Then compute the percent difference rainy vs. dry – by hand or with <code>FILTER</code>.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT is_rainy_day, COUNT(*) AS days,
# MAGIC        ROUND(AVG(bikes_per_station)) AS bikes, ROUND(AVG(vehicles_per_site)) AS vehicles
# MAGIC FROM mobility_gold.daily_traffic
# MAGIC WHERE year BETWEEN 2023 AND 2025 AND NOT is_weekend
# MAGIC GROUP BY ___
# MAGIC -- percent difference: (rainy / dry - 1) * 100</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>WITH by_weather AS (
# MAGIC   SELECT is_rainy_day,
# MAGIC          COUNT(*)               AS days,
# MAGIC          AVG(bikes_per_station) AS bikes,
# MAGIC          AVG(vehicles_per_site) AS vehicles
# MAGIC   FROM mobility_gold.daily_traffic
# MAGIC   WHERE year BETWEEN 2023 AND 2025 AND NOT is_weekend
# MAGIC   GROUP BY is_rainy_day
# MAGIC )
# MAGIC SELECT
# MAGIC   MAX(days)     FILTER (WHERE is_rainy_day)     AS rainy_days,
# MAGIC   MAX(days)     FILTER (WHERE NOT is_rainy_day) AS dry_days,
# MAGIC   ROUND((MAX(bikes) FILTER (WHERE is_rainy_day) / MAX(bikes) FILTER (WHERE NOT is_rainy_day) - 1) * 100, 1)       AS bikes_diff_pct,
# MAGIC   ROUND((MAX(vehicles) FILTER (WHERE is_rainy_day) / MAX(vehicles) FILTER (WHERE NOT is_rainy_day) - 1) * 100, 1) AS vehicles_diff_pct
# MAGIC FROM by_weather</code></pre></details>

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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>corr(x, y)</code> is an aggregate function – one row with two correlations.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT ROUND(corr(temp_mean_c, ___), 2) AS corr_temp_bikes,
# MAGIC        ROUND(corr(temp_mean_c, ___), 2) AS corr_temp_vehicles
# MAGIC FROM mobility_gold.daily_traffic
# MAGIC WHERE year BETWEEN 2023 AND 2025 AND NOT is_weekend</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT ROUND(corr(temp_mean_c, bikes_per_station), 2) AS corr_temp_bikes,
# MAGIC        ROUND(corr(temp_mean_c, vehicles_per_site), 2) AS corr_temp_vehicles
# MAGIC FROM mobility_gold.daily_traffic
# MAGIC WHERE year BETWEEN 2023 AND 2025 AND NOT is_weekend</code></pre></details>

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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'><code>MAX_BY(hour, avg_per_location)</code> returns the hour at which <code>avg_per_location</code> is highest.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT mode, day_type, MAX_BY(___, ___) AS peak_hour, MAX(avg_per_location) AS avg_at_peak
# MAGIC FROM mobility_gold.hourly_profile
# MAGIC GROUP BY mode, day_type</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT mode, day_type,
# MAGIC        MAX_BY(hour, avg_per_location) AS peak_hour,
# MAGIC        MAX(avg_per_location)          AS avg_at_peak
# MAGIC FROM mobility_gold.hourly_profile
# MAGIC GROUP BY mode, day_type
# MAGIC ORDER BY mode, day_type</code></pre></details>

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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Number the rows per <code>mode</code> by volume with <code>ROW_NUMBER() OVER (PARTITION BY mode ORDER BY avg_daily_count DESC)</code> and keep rows 1–5.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT mode, location_name, avg_daily_count
# MAGIC FROM (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY ___ ORDER BY ___ DESC) AS rn
# MAGIC   FROM mobility_gold.location_map
# MAGIC )
# MAGIC WHERE rn &lt;= ___</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT mode, location_name, avg_daily_count
# MAGIC FROM (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY mode ORDER BY avg_daily_count DESC) AS rn
# MAGIC   FROM mobility_gold.location_map
# MAGIC )
# MAGIC WHERE rn &lt;= 5
# MAGIC ORDER BY mode, avg_daily_count DESC</code></pre></details>

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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Just show the three columns from <code>yearly_trend</code> side by side and compare 2014 with 2024.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT year, bikes_total, avg_active_bike_stations, avg_daily_bikes_per_station
# MAGIC FROM mobility_gold.___
# MAGIC ORDER BY year</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT year, bikes_total, avg_active_bike_stations, avg_daily_bikes_per_station
# MAGIC FROM mobility_gold.yearly_trend
# MAGIC ORDER BY year</code></pre></details>

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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint 1 – the idea</b></summary><p style='margin:6px 0'>Get the busiest bike location with <code>spark.sql(...).first()</code>, then multiply step by step.</p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🧩 Hint 2 – code skeleton</b></summary><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>top = spark.sql(&quot;&quot;&quot;SELECT location_name, avg_daily_count FROM mobility_gold.location_map
# MAGIC                    WHERE mode = &#x27;Bike&#x27; ORDER BY avg_daily_count DESC LIMIT 1&quot;&quot;&quot;).first()
# MAGIC co2_tonnes = top[&quot;avg_daily_count&quot;] * 365 * ___ * ___ * ___ / 1000
# MAGIC print(top[&quot;location_name&quot;], round(co2_tonnes))</code></pre></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>top = spark.sql(&quot;&quot;&quot;
# MAGIC     SELECT location_name, avg_daily_count
# MAGIC     FROM mobility_gold.location_map
# MAGIC     WHERE mode = &#x27;Bike&#x27;
# MAGIC     ORDER BY avg_daily_count DESC
# MAGIC     LIMIT 1
# MAGIC &quot;&quot;&quot;).first()
# MAGIC 
# MAGIC co2_tonnes = top[&quot;avg_daily_count&quot;] * 365 * SHARE_REPLACING_CAR * AVG_TRIP_KM * CAR_KG_CO2_PER_KM / 1000
# MAGIC print(f&quot;Station: {top[&#x27;location_name&#x27;]} – {top[&#x27;avg_daily_count&#x27;]:,.0f} bikes per day&quot;)
# MAGIC print(f&quot;Estimated CO2 saved per year: {co2_tonnes:,.0f} t (under the assumptions above)&quot;)</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC 👉 Continue with **step 6 – dashboard** (see the project README).
