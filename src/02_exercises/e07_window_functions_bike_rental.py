# Databricks notebook source

# MAGIC %md
# MAGIC # Exercise: Window Functions
# MAGIC
# MAGIC In this exercise you practise Spark **window functions** on a synthetic bike-rental dataset.
# MAGIC
# MAGIC **Learning objectives**
# MAGIC - Define a `Window` spec with `partitionBy` and `orderBy`
# MAGIC - Apply ranking functions (`dense_rank`, `row_number`)
# MAGIC - Compute cumulative (running) sums
# MAGIC - Use `lag()` to compare consecutive rows within a group
# MAGIC - Detect anomalies with day-over-day change
# MAGIC - Build rolling aggregations with `rowsBetween`
# MAGIC - Pivot data from long to wide format
# MAGIC - Deduplicate with `row_number`
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC Synthetic bike-rental trips across 5 Swiss cities, generated in the Setup cell below.
# MAGIC Each run is fully idempotent — just re-run from the top.
# MAGIC
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | `trip_id` | string | Unique trip identifier, e.g. `T00042` |
# MAGIC | `bike_id` | string | Bike identifier, e.g. `B023` |
# MAGIC | `user_id` | string | User identifier (≈5 % NULL) |
# MAGIC | `city` | string | One of 5 Swiss cities |
# MAGIC | `station_from` / `station_to` | string | Station names |
# MAGIC | `start_ts_str` / `end_ts_str` | string | Timestamps as strings (`"yyyy-MM-dd HH:mm:ss"`) |
# MAGIC | `distance_km` | double | Trip distance (≈10 % NULL) |
# MAGIC | `notes` | string | Log-style string, e.g. `"bike=B023 uid=U0042 dist=4.2"` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — cleanup & data generation
# MAGIC
# MAGIC **Do not modify this section.** Run it once at the start of every session.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA  = "exercises"
TABLE   = f"{CATALOG}.{SCHEMA}.trips_raw"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
print(f"Ready: {TABLE}")

# COMMAND ----------

import random
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

random.seed(42)
CITIES = ["Zurich", "Bern", "Basel", "Luzern", "Geneva"]
STATIONS = {
    "Zurich": ["ZH-Central",        "ZH-Hauptbahnhof", "ZH-Bellevue",       "ZH-Oerlikon"],
    "Bern":   ["BE-Bahnhof",         "BE-Bundesplatz",  "BE-Laenggasse",     "BE-Wankdorf"],
    "Basel":  ["BS-SBB",             "BS-Marktplatz",   "BS-Dreilaendereck", "BS-Aeschenplatz"],
    "Luzern": ["LU-Bahnhof",         "LU-Kapellbruecke","LU-Tribschen",      "LU-Horw"],
    "Geneva": ["GE-Cornavin",        "GE-Rive",         "GE-Plainpalais",    "GE-Meyrin"],
}
schema = StructType([
    StructField("trip_id",      StringType(), False),
    StructField("bike_id",      StringType(), False),
    StructField("user_id",      StringType(), True),
    StructField("city",         StringType(), False),
    StructField("station_from", StringType(), False),
    StructField("station_to",   StringType(), False),
    StructField("start_ts_str", StringType(), False),
    StructField("end_ts_str",   StringType(), False),
    StructField("distance_km",  DoubleType(), True),
    StructField("notes",        StringType(), False),
])
start_date = datetime(2026, 3, 1)
rows = []
for i in range(1500):
    city  = random.choice(CITIES)
    stns  = STATIONS[city]
    s_frm = random.choice(stns)
    s_to  = random.choice([s for s in stns if s != s_frm])
    ts0   = start_date + timedelta(
                days=random.randint(0, 30),
                hours=random.randint(5, 22),
                minutes=random.randint(0, 59))
    dur   = random.randint(4, 90)
    ts1   = ts0 + timedelta(minutes=dur)
    dist  = round(random.uniform(0.3, 18.0), 1) if random.random() > 0.10 else None
    uid   = f"U{random.randint(1, 200):04d}"     if random.random() > 0.05 else None
    bid   = f"B{random.randint(1,  50):03d}"
    notes = f"bike={bid} uid={uid or 'anon'} dist={dist or 'n/a'}"
    rows.append((
        f"T{i+1:05d}", bid, uid, city, s_frm, s_to,
        ts0.strftime("%Y-%m-%d %H:%M:%S"),
        ts1.strftime("%Y-%m-%d %H:%M:%S"),
        dist, notes,
    ))
df_raw = spark.createDataFrame(rows, schema)
df_raw.write.format("delta").mode("overwrite").saveAsTable(TABLE)
print(f"Generated {df_raw.count():,} rows → {TABLE}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 1 — Parse timestamps & add `duration_min`
# MAGIC
# MAGIC Read the raw table and build a DataFrame `df` with three additional columns:
# MAGIC - `start_ts` — parse `start_ts_str` as a timestamp (format `"yyyy-MM-dd HH:mm:ss"`)
# MAGIC - `end_ts` — parse `end_ts_str` the same way
# MAGIC - `duration_min` — trip duration in **minutes** (integer)
# MAGIC
# MAGIC **Hint:** `F.unix_timestamp(col, format)` returns seconds since epoch.
# MAGIC Subtract start from end, divide by 60, cast to int.
# MAGIC
# MAGIC Store the result in `df` and call `df.show(5)`.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Task 1: parse timestamps and compute duration_min")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 2 — `dense_rank`: top-3 longest trips per city
# MAGIC
# MAGIC For each **city**, rank trips by `distance_km` descending using `F.dense_rank()`.
# MAGIC Keep only the top-3 per city and display `city`, `trip_id`, `distance_km`, `rank`.
# MAGIC
# MAGIC - Exclude rows where `distance_km` is NULL first.
# MAGIC - Window spec: `partitionBy("city").orderBy(F.desc("distance_km"))`.
# MAGIC
# MAGIC Expected: 5 cities × 3 rows = **15 rows**.
# MAGIC
# MAGIC **Follow-up:** Change `dense_rank()` to `rank()` and then to `row_number()`.
# MAGIC Run the cell again each time. Can you spot the difference in the output?

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Task 2: dense_rank top-3 trips per city")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 3 — Cumulative distance per user
# MAGIC
# MAGIC For each **user**, compute the running total of `distance_km` ordered by `start_ts`.
# MAGIC Name the new column `km_cumulative` (round to 1 decimal).
# MAGIC
# MAGIC Use this window frame to accumulate from the first trip to the current row:
# MAGIC ```python
# MAGIC .rowsBetween(Window.unboundedPreceding, Window.currentRow)
# MAGIC ```
# MAGIC
# MAGIC Filter out NULL `user_id` and NULL `distance_km` before applying the window.
# MAGIC Show the result for user `"U0007"` only so the output stays readable.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Task 3: cumulative distance per user")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 4 — `lag()`: gap between consecutive trips per user
# MAGIC
# MAGIC For each **user**, find the previous trip's `end_ts` using `F.lag("end_ts", 1)`.
# MAGIC Then compute `gap_min`: minutes between that previous end and the current start (int).
# MAGIC
# MAGIC Steps:
# MAGIC 1. Filter out NULL `user_id`.
# MAGIC 2. Window: `partitionBy("user_id").orderBy("start_ts")`.
# MAGIC 3. Add `prev_end = F.lag("end_ts", 1).over(w)`.
# MAGIC 4. Compute `gap_min` from the difference in unix timestamps.
# MAGIC 5. Drop rows where `gap_min` is NULL (first trip per user has no predecessor).
# MAGIC 6. Show 15 rows ordered by `user_id`, `start_ts`.
# MAGIC
# MAGIC **Follow-up:** Which user waited the longest between two trips?
# MAGIC Add an `orderBy(F.desc("gap_min")).limit(1)` to find out.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Task 4: lag and gap between trips")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 5 — Day-over-day change & spike detection
# MAGIC
# MAGIC A common pattern in monitoring: flag days where a metric jumps by more than 50%
# MAGIC compared to the previous day.
# MAGIC
# MAGIC **5a.** Build a daily aggregate: one row per `(city, date)` with the trip count.
# MAGIC Register it as temp view `daily_trips`.
# MAGIC
# MAGIC **5b.** On that aggregate, use `lag(1)` (partitioned by `city`, ordered by `date`) to get
# MAGIC `prev_trips` — the previous day's count.
# MAGIC
# MAGIC **5c.** Compute `pct_change = (trips - prev_trips) / prev_trips` (as a double, rounded to 2).
# MAGIC Guard against division by zero: use `F.when(F.col("prev_trips") > 0, F.col("prev_trips"))`
# MAGIC as the denominator — returns NULL when `prev_trips` is 0 or NULL, which propagates cleanly.
# MAGIC
# MAGIC **5d.** Add `is_spike = pct_change > 0.5`.
# MAGIC Filter to spike days and show `city`, `date`, `prev_trips`, `trips`, `pct_change`.

# COMMAND ----------

# 5a — daily aggregate
# YOUR CODE HERE
raise NotImplementedError("Task 5a: build daily_trips temp view")

# COMMAND ----------

# 5b–5d — lag, pct_change, spike flag
# YOUR CODE HERE
raise NotImplementedError("Task 5b-d: lag on daily aggregate + spike flag")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 6 — Rolling 7-day trip count (SQL)
# MAGIC
# MAGIC Using the `daily_trips` view from Task 5, write a SQL query that adds a
# MAGIC `rolling_7d` column: the sum of trips over the current day and the 6 preceding days.
# MAGIC
# MAGIC Filter to `city = 'Zurich'`, order by `date`, and `LIMIT 20`.
# MAGIC
# MAGIC **Hint:** `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: rolling 7-day trip count for Zurich
# MAGIC -- SELECT city, date, trips, SUM(trips) OVER (...) AS rolling_7d
# MAGIC -- FROM daily_trips WHERE city = 'Zurich' ORDER BY date LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC **Reflection:** The first 6 rows of each city show a rolling sum computed over fewer than
# MAGIC 7 days — Spark uses whatever rows are available. Is this a problem here, or expected behaviour?

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 7 — Pivot: trips per city × weekday
# MAGIC
# MAGIC Reshape the data from long to wide: rows = cities, columns = weekdays, values = trip count.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Add a `weekday` column: `F.date_format("start_ts", "EEE")` gives `"Mon"`, `"Tue"`, etc.
# MAGIC 2. `groupBy("city").pivot("weekday", ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]).count()`
# MAGIC    — listing the values explicitly avoids a second scan to discover them.
# MAGIC 3. Show the result.
# MAGIC
# MAGIC **Follow-up:** Which city has the highest Saturday + Sunday share of weekly trips?
# MAGIC Add a `withColumn("weekend_pct", (Sat + Sun) / (Mon+Tue+Wed+Thu+Fri+Sat+Sun))` column.
# MAGIC Hint: use `F.col("Sat")`, `F.col("Sun")`, etc.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Task 7: pivot trips per city × weekday")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 8 — `row_number()`: most recent trip per user
# MAGIC
# MAGIC A very common real-world pattern: you have a history table and want only the
# MAGIC **latest record per entity** (e.g., current address, last known state).
# MAGIC
# MAGIC Use `row_number()` partitioned by `user_id`, ordered by `start_ts` **descending**.
# MAGIC Keep only the rows where the rank equals 1 — that is each user's most recent trip.
# MAGIC
# MAGIC Filter out NULL `user_id`. Show 10 rows with `user_id`, `start_ts`, `trip_id`, `city`.
# MAGIC
# MAGIC **Follow-up:** How many distinct users appear in the result?
# MAGIC Use `.select("user_id").distinct().count()` to check.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Task 8: row_number() to get most recent trip per user")
