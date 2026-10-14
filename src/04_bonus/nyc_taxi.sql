-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bonus – New York taxi trips: how many could a bike replace?
-- MAGIC
-- MAGIC **Goal (optional, ≈ 20 min):** Work with a dataset that is available in every Databricks workspace:
-- MAGIC `samples.nyctaxi.trips` (yellow taxi trips in New York City). No setup needed!
-- MAGIC
-- MAGIC | Column | Meaning |
-- MAGIC |---|---|
-- MAGIC | `tpep_pickup_datetime` / `tpep_dropoff_datetime` | start / end of the trip |
-- MAGIC | `trip_distance` | distance in **miles** (1 mile = 1.609 km) |
-- MAGIC | `fare_amount` | fare in USD |
-- MAGIC | `pickup_zip` / `dropoff_zip` | ZIP code of start / destination |
-- MAGIC
-- MAGIC Below each exercise you find a **hint** and the **solution** (click to expand) – try it yourself first!

-- COMMAND ----------

SELECT * FROM samples.nyctaxi.trips LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 1 – Overview
-- MAGIC How many trips does the table contain? Which period is covered?
-- MAGIC How long is a trip on average (in **km**)?

-- COMMAND ----------

-- your code here


-- COMMAND ----------

-- MAGIC %md
-- MAGIC <details>
-- MAGIC <summary><b>💡 Hint – useful building blocks</b></summary>
-- MAGIC
-- MAGIC You will need: `ROUND`, `COUNT`, `AVG`, `MIN`, `MAX`
-- MAGIC
-- MAGIC </details>
-- MAGIC <details>
-- MAGIC <summary><b>🔑 Solution</b></summary>
-- MAGIC
-- MAGIC Try it yourself first – then compare.
-- MAGIC
-- MAGIC ```sql
-- MAGIC -- Exercise 1
-- MAGIC SELECT COUNT(*)                                   AS trips,
-- MAGIC        MIN(tpep_pickup_datetime)                  AS first_trip,
-- MAGIC        MAX(tpep_pickup_datetime)                  AS last_trip,
-- MAGIC        ROUND(AVG(trip_distance) * 1.609, 2)       AS avg_distance_km
-- MAGIC FROM samples.nyctaxi.trips;
-- MAGIC ```
-- MAGIC
-- MAGIC </details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 2 – Short trips
-- MAGIC Which share of the trips is **shorter than 3 km**? Such distances are easy to do by bike.
-- MAGIC
-- MAGIC *Tip:* `AVG(CASE WHEN ... THEN 1 ELSE 0 END)` returns a share between 0 and 1.

-- COMMAND ----------

-- your code here


-- COMMAND ----------

-- MAGIC %md
-- MAGIC <details>
-- MAGIC <summary><b>💡 Hint – useful building blocks</b></summary>
-- MAGIC
-- MAGIC You will need: `CASE WHEN`, `ROUND`, `AVG`
-- MAGIC
-- MAGIC </details>
-- MAGIC <details>
-- MAGIC <summary><b>🔑 Solution</b></summary>
-- MAGIC
-- MAGIC Try it yourself first – then compare.
-- MAGIC
-- MAGIC ```sql
-- MAGIC -- Exercise 2
-- MAGIC SELECT ROUND(AVG(CASE WHEN trip_distance * 1.609 < 3 THEN 1 ELSE 0 END) * 100, 1) AS share_below_3km_pct
-- MAGIC FROM samples.nyctaxi.trips
-- MAGIC WHERE trip_distance > 0;
-- MAGIC ```
-- MAGIC
-- MAGIC </details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 3 – How fast is a taxi?
-- MAGIC Compute the average speed in **km/h** per hour of the day (pickup time).
-- MAGIC When is the taxi slower than a bike (≈ 15–18 km/h in a city)?
-- MAGIC
-- MAGIC *Tip:* `timestampdiff(SECOND, start, end)`; filter out implausible trips (duration ≤ 0, distance = 0).

-- COMMAND ----------

-- your code here
-- COMMAND ----------

-- MAGIC %md
-- MAGIC <details>
-- MAGIC <summary><b>💡 Hint – useful building blocks</b></summary>
-- MAGIC
-- MAGIC You will need: `GROUP BY`, `ORDER BY`, `BETWEEN`, `HOUR`, `TIMESTAMPDIFF`, `ROUND`, `COUNT`, `AVG`
-- MAGIC
-- MAGIC </details>
-- MAGIC <details>
-- MAGIC <summary><b>🔑 Solution</b></summary>
-- MAGIC
-- MAGIC Try it yourself first – then compare.
-- MAGIC
-- MAGIC ```sql
-- MAGIC -- Exercise 3
-- MAGIC SELECT hour(tpep_pickup_datetime) AS pickup_hour,
-- MAGIC        ROUND(AVG(trip_distance * 1.609
-- MAGIC                  / (timestampdiff(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime) / 3600)), 1) AS avg_speed_kmh,
-- MAGIC        COUNT(*) AS trips
-- MAGIC FROM samples.nyctaxi.trips
-- MAGIC WHERE trip_distance > 0
-- MAGIC   AND timestampdiff(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime) BETWEEN 60 AND 3 * 3600
-- MAGIC GROUP BY ALL
-- MAGIC ORDER BY pickup_hour;
-- MAGIC ```
-- MAGIC
-- MAGIC </details>

