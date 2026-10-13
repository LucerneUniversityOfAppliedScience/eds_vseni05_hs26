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
-- MAGIC Solutions: `solutions/04_bonus/nyc_taxi_solution.sql`

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
-- MAGIC ## Exercise 2 – Short trips
-- MAGIC Which share of the trips is **shorter than 3 km**? Such distances are easy to do by bike.
-- MAGIC
-- MAGIC *Tip:* `AVG(CASE WHEN ... THEN 1 ELSE 0 END)` returns a share between 0 and 1.

-- COMMAND ----------

-- your code here


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise 3 – How fast is a taxi?
-- MAGIC Compute the average speed in **km/h** per hour of the day (pickup time).
-- MAGIC When is the taxi slower than a bike (≈ 15–18 km/h in a city)?
-- MAGIC
-- MAGIC *Tip:* `timestampdiff(SECOND, start, end)`; filter out implausible trips (duration ≤ 0, distance = 0).

-- COMMAND ----------

-- your code here
