# Databricks notebook source

# MAGIC %md
# MAGIC # Exercise: Transformations, NULL Handling, Date/Time & Regex
# MAGIC
# MAGIC This exercise covers the practical day-to-day work of PySpark DataFrames:
# MAGIC cleaning NULLs, parsing timestamps, engineering feature columns with `when/otherwise`,
# MAGIC manipulating strings, extracting structured fields with regex, deduplicating data,
# MAGIC and chaining everything into a single pipeline.
# MAGIC
# MAGIC **Learning objectives**
# MAGIC - Audit and handle NULL values (`isNull`, `fillna`, `dropna`, `coalesce`)
# MAGIC - Parse timestamp strings and extract calendar components
# MAGIC - Build conditional columns with `F.when / F.otherwise`
# MAGIC - Apply string functions (`concat_ws`, `upper`, `split`)
# MAGIC - Extract fields from semi-structured text using `regexp_extract` and `rlike`
# MAGIC - Deduplicate with `dropDuplicates`
# MAGIC - Chain all transformations into a single fluent pipeline
# MAGIC - Write and apply a Pandas UDF (bonus)

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

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 1 — NULL audit & cleanup
# MAGIC
# MAGIC **1a.** Count how many NULLs exist in `user_id` and `distance_km`.
# MAGIC Print one line per column, e.g.:
# MAGIC ```
# MAGIC user_id: 74 NULLs
# MAGIC distance_km: 152 NULLs
# MAGIC ```
# MAGIC Hint: `df.filter(F.col("col_name").isNull()).count()`
# MAGIC
# MAGIC **1b.** Create `df_clean` by:
# MAGIC - Replacing NULL in `distance_km` with `0.0` using `fillna`
# MAGIC - Dropping rows where `user_id` is NULL using `dropna`
# MAGIC
# MAGIC Print the row count before and after cleanup.
# MAGIC
# MAGIC **1c.** On the **original** `df_raw` (before dropping NULLs), add a column `user_display`
# MAGIC using `F.coalesce`: show `user_id` if not NULL, otherwise `"anonymous"`.
# MAGIC Show 8 rows with `user_id` and `user_display`.

# COMMAND ----------

df_raw = spark.table(TABLE)

# 1a — count NULLs
# YOUR CODE HERE
raise NotImplementedError("Task 1a: count NULLs in user_id and distance_km")

# COMMAND ----------

# 1b — create df_clean
# YOUR CODE HERE
raise NotImplementedError("Task 1b: fillna distance_km + dropna user_id → df_clean")

# COMMAND ----------

# 1c — coalesce for display
# YOUR CODE HERE
raise NotImplementedError("Task 1c: add user_display via coalesce, show(8)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 2 — Parse timestamps & extract calendar components
# MAGIC
# MAGIC Starting from `df_raw`, parse `start_ts_str` into `start_ts`
# MAGIC (format `"yyyy-MM-dd HH:mm:ss"`). Then derive:
# MAGIC
# MAGIC - `hour` — hour of day (0–23)
# MAGIC - `weekday_num` — 0 = Monday … 6 = Sunday
# MAGIC   Hint: `((F.dayofweek("start_ts") + 5) % 7)` converts Spark's 1=Sunday to 0=Monday
# MAGIC - `weekday_name` — full English name via `F.date_format(..., "EEEE")`
# MAGIC - `date` — date part only via `F.to_date("start_ts")`
# MAGIC
# MAGIC Store the result in `df_ts` and show 6 rows.
# MAGIC
# MAGIC **Follow-up:** How many trips happened on each day of the week?
# MAGIC Use `df_ts.groupBy("weekday_name").count().orderBy("weekday_name").show()`.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Task 2: parse timestamp → df_ts with hour, weekday_num, weekday_name, date")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 3 — `when / otherwise`: classify trips
# MAGIC
# MAGIC Starting from `df_ts`, add:
# MAGIC
# MAGIC 1. `end_ts` — parse `end_ts_str`
# MAGIC 2. `duration_min` — trip duration in minutes (int)
# MAGIC 3. `trip_category`:
# MAGIC
# MAGIC    | Condition | Label |
# MAGIC    |-----------|-------|
# MAGIC    | `duration_min < 15` | `"quick"` |
# MAGIC    | `duration_min < 45` | `"normal"` |
# MAGIC    | otherwise | `"long"` |
# MAGIC
# MAGIC 4. `is_peak_hour` — `True` when `hour` is 7–9 **or** 17–19 (inclusive)
# MAGIC    Hint: `F.col("hour").between(7, 9) | F.col("hour").between(17, 19)`
# MAGIC
# MAGIC Store as `df_cat`. Then show the breakdown:
# MAGIC ```python
# MAGIC df_cat.groupBy("trip_category", "is_peak_hour").count().orderBy("trip_category").show()
# MAGIC ```
# MAGIC
# MAGIC **Follow-up:** What percentage of "quick" trips happen during peak hours?

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Task 3: add duration_min, trip_category, is_peak_hour → df_cat")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 4 — String operations
# MAGIC
# MAGIC Starting from `df_raw`, create:
# MAGIC
# MAGIC | New column | How | Example |
# MAGIC |------------|-----|---------|
# MAGIC | `origin` | `city + " / " + station_from` via `F.concat_ws` | `"Zurich / ZH-Central"` |
# MAGIC | `city_upper` | uppercase city via `F.upper` | `"ZURICH"` |
# MAGIC | `station_code` | part before the dash via `F.split(..., "-").getItem(0)` | `"ZH"` |
# MAGIC | `route` | `station_from + " → " + station_to` via `F.concat_ws` | `"ZH-Central → ZH-Oerlikon"` |
# MAGIC
# MAGIC Show 8 rows with these four columns.
# MAGIC
# MAGIC **Follow-up:** Which `route` appears most often? Use `groupBy("route").count().orderBy(F.desc("count")).show(5)`.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Task 4: origin, city_upper, station_code, route")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 5 — Regex: extract fields from `notes`
# MAGIC
# MAGIC The `notes` column contains strings like:
# MAGIC ```
# MAGIC bike=B023 uid=U0042 dist=4.2
# MAGIC bike=B007 uid=anon dist=n/a
# MAGIC ```
# MAGIC
# MAGIC Use `F.regexp_extract(col, pattern, group)` to add:
# MAGIC - `bike_extracted` — bike ID, e.g. `"B023"`. Pattern: `r"bike=(B\d+)"`
# MAGIC - `uid_extracted` — raw uid value. Pattern: `r"uid=(\w+)"`
# MAGIC - `dist_extracted` — distance as a **string** from notes. Pattern: `r"dist=([\d.]+)"`
# MAGIC   (this returns an empty string when dist is `"n/a"` — that is correct)
# MAGIC - `is_known_user` — boolean: `True` when uid matches `U` followed by exactly 4 digits.
# MAGIC   Hint: `F.col("notes").rlike(r"uid=U\d{4}\b")`
# MAGIC
# MAGIC Show 10 rows with `notes`, `bike_extracted`, `uid_extracted`, `dist_extracted`, `is_known_user`.
# MAGIC
# MAGIC **Follow-up:** Cross-check: does `bike_extracted` always match `bike_id`?
# MAGIC Add a `F.col("bike_id") != F.col("bike_extracted")` filter and count the mismatches.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Task 5: regexp_extract bike, uid, dist; rlike is_known_user")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 6 — `dropDuplicates`: detecting and removing duplicate records
# MAGIC
# MAGIC In real systems, data often arrives more than once (re-ingestion, retries, bugs in upstream).
# MAGIC The cell below injects 200 exact duplicate rows into a new DataFrame to simulate this.

# COMMAND ----------

# Inject duplicates — do not modify
import random as _rnd
_rnd.seed(99)
df_with_dups = df_raw.union(
    df_raw.orderBy(F.rand(seed=99)).limit(200)
)
print(f"Rows with duplicates: {df_with_dups.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC **6a.** Use `dropDuplicates()` (no arguments) to remove exact duplicates.
# MAGIC Print the row count before and after — does it match the original 1 500?
# MAGIC
# MAGIC **6b.** Now imagine that a source system re-sends a trip but with a new `trip_id`
# MAGIC (a soft duplicate: same user, same start time, different ID).
# MAGIC The cell below creates 50 such soft duplicates.
# MAGIC Use `dropDuplicates(["user_id", "start_ts_str"])` to deduplicate on those columns only.
# MAGIC How many rows remain?

# COMMAND ----------

# Inject soft duplicates — do not modify
df_soft_dups = df_with_dups.union(
    df_raw
    .filter(F.col("user_id").isNotNull())
    .orderBy(F.rand(seed=7))
    .limit(50)
    .withColumn("trip_id", F.concat(F.col("trip_id"), F.lit("_v2")))
)
print(f"Rows with soft duplicates: {df_soft_dups.count()}")

# COMMAND ----------

# 6a — exact deduplication
# YOUR CODE HERE
raise NotImplementedError("Task 6a: dropDuplicates() to remove exact duplicates")

# COMMAND ----------

# 6b — soft deduplication by natural key
# YOUR CODE HERE
raise NotImplementedError("Task 6b: dropDuplicates(['user_id', 'start_ts_str'])")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 7 — Combined pipeline
# MAGIC
# MAGIC Put it all together. Write a **single fluent expression** (one chained statement)
# MAGIC that transforms `df_raw` into a clean summary table — no intermediate variables.
# MAGIC
# MAGIC The pipeline must:
# MAGIC 1. Parse `start_ts` and `end_ts`
# MAGIC 2. Fill NULL `distance_km` with `0.0`
# MAGIC 3. Drop rows where `user_id` is NULL
# MAGIC 4. Add `duration_min`
# MAGIC 5. Add `trip_category` (`"quick"` / `"normal"` / `"long"`)
# MAGIC 6. Add `weekday_name` via `F.date_format("start_ts", "EEE")`
# MAGIC 7. `groupBy("city", "trip_category", "weekday_name")`
# MAGIC    `.agg(F.count("*").alias("trips"), F.round(F.avg("distance_km"), 1).alias("avg_km"))`
# MAGIC 8. `orderBy("city", "trip_category", "weekday_name")`
# MAGIC
# MAGIC Call `.show(20)` at the end.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Task 7: single fluent pipeline from raw → summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 8 (Bonus) — Pandas UDF
# MAGIC
# MAGIC `F.when()` from Task 3 is the best solution for this classification.
# MAGIC This bonus task practises Pandas UDFs for situations where built-ins cannot
# MAGIC express the logic.
# MAGIC
# MAGIC Write a `@pandas_udf("string")` called `classify_duration` that replicates the
# MAGIC `trip_category` logic from Task 3.
# MAGIC Apply it to `duration_min` (rebuild it from `df_raw` first) and verify the output
# MAGIC matches the `F.when()` version exactly.

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

# YOUR CODE HERE
raise NotImplementedError("Task 8 (Bonus): Pandas UDF classify_duration")
