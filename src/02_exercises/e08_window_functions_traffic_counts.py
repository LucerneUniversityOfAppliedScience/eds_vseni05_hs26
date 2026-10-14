# Databricks notebook source

# MAGIC %md
# MAGIC # Window Functions and Pivot — Exercise
# MAGIC
# MAGIC In this exercise you learn how to use **window functions** and **pivot/unpivot**
# MAGIC for analytic queries on real-world time-series data.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC Window functions are the bridge between row-level transformations and
# MAGIC group-level aggregations. They let you compute things like running totals,
# MAGIC moving averages, ranks, and previous/next values **without collapsing rows**.
# MAGIC In SQL terms: anything that uses `OVER (PARTITION BY ... ORDER BY ...)`.
# MAGIC
# MAGIC Pivot reshapes data from long to wide. It's how you turn a tidy fact table
# MAGIC into a dashboard-ready cross-tab. Unpivot is the inverse — useful for
# MAGIC normalising spreadsheet exports back into a queryable shape.
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC Pedestrian and bike counts from the city of Zurich, 15-minute intervals
# MAGIC across all of 2025, ~850k rows. Open data from the city of Zurich, already
# MAGIC staged in your Unity Catalog volume by the workshop setup job.
# MAGIC
# MAGIC | Column | Description |
# MAGIC |--------|-------------|
# MAGIC | `FK_STANDORT` | Station ID |
# MAGIC | `DATUM` | Timestamp (ISO `T`-separated, 15-min intervals) |
# MAGIC | `VELO_IN`, `VELO_OUT` | Bikes counted in each direction |
# MAGIC | `FUSS_IN`, `FUSS_OUT` | Pedestrians counted (NULL on bike-only stations) |
# MAGIC | `OST`, `NORD` | Swiss coordinates of the station |
# MAGIC
# MAGIC ## Before you run
# MAGIC
# MAGIC The bundle must be deployed and the setup job `workshop_setup`
# MAGIC must have run once (see README).

# COMMAND ----------

CSV_PATH      = "/Volumes/workspace/mobility_raw/landing/sample_data/csv/2025_verkehrszaehlungen_werte_fussgaenger_velo.csv"
BRONZE_TABLE  = "workspace.exercises.bronze_traffic_counts"
GOLD_DAILY    = "workspace.exercises.gold_traffic_daily_by_station"

print(f"CSV   : {CSV_PATH}")
print(f"Bronze: {BRONZE_TABLE}")
print(f"Gold  : {GOLD_DAILY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup: drop existing tables
# MAGIC
# MAGIC Drop any existing target tables before re-running so a stale schema from a
# MAGIC previous run does not block the overwrite (Delta refuses schema changes on
# MAGIC `overwrite` when Table ACLs are enabled).

# COMMAND ----------

for table in [BRONZE_TABLE, GOLD_DAILY]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped (if existed): {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Read the CSV with an explicit schema
# MAGIC
# MAGIC Read the CSV with an explicit schema so types are correct from the start.
# MAGIC Parse `DATUM` as a timestamp using `option("timestampFormat", "yyyy-MM-dd'T'HH:mm")`.
# MAGIC Then derive `date`, `hour`, and `weekday` columns. Deriving these once
# MAGIC up-front means later window queries don't have to re-parse strings on every
# MAGIC row.
# MAGIC
# MAGIC Spark's `dayofweek` returns 1=Sunday … 7=Saturday. Convert to ISO-style
# MAGIC 1=Monday … 7=Sunday for readability — there's a one-line trick using `% 7`.
# MAGIC
# MAGIC Save the result to `BRONZE_TABLE` with `mode("overwrite")`.

# COMMAND ----------

# TODO: read the CSV with an explicit schema, parse DATUM as a timestamp,
#       derive date/hour/weekday columns, write to BRONZE_TABLE.
# Hint: use StructType + StructField, .option("timestampFormat", ...),
#       to_date(), hour(), dayofweek().

# YOUR CODE HERE
raise NotImplementedError("Step 1: read CSV and write Bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary><b>💡 Hint – useful building blocks</b></summary>
# MAGIC
# MAGIC You will need: `TO_DATE`, `HOUR`, `.withColumn()`, `.withColumnRenamed()`, `.saveAsTable()`, `.printSchema()`, `.count()`, `.schema()`, `.option()`
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🔑 Solution</b></summary>
# MAGIC
# MAGIC Try it yourself first – then compare.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType
# MAGIC from pyspark.sql.functions import col, to_date, hour, dayofweek, when, expr
# MAGIC
# MAGIC traffic_schema = StructType([
# MAGIC     StructField("FK_STANDORT", IntegerType(),   True),
# MAGIC     StructField("DATUM",       TimestampType(), True),
# MAGIC     StructField("VELO_IN",     IntegerType(),   True),
# MAGIC     StructField("VELO_OUT",    IntegerType(),   True),
# MAGIC     StructField("FUSS_IN",     IntegerType(),   True),
# MAGIC     StructField("FUSS_OUT",    IntegerType(),   True),
# MAGIC     StructField("OST",         DoubleType(),    True),
# MAGIC     StructField("NORD",        DoubleType(),    True),
# MAGIC ])
# MAGIC
# MAGIC df_raw = (spark.read
# MAGIC     .option("header", "true")
# MAGIC     .option("timestampFormat", "yyyy-MM-dd'T'HH:mm")
# MAGIC     .schema(traffic_schema)
# MAGIC     .csv(CSV_PATH))
# MAGIC
# MAGIC df_bronze = (df_raw
# MAGIC     .withColumnRenamed("FK_STANDORT", "station_id")
# MAGIC     .withColumnRenamed("DATUM",       "ts")
# MAGIC     .withColumn("date",    to_date(col("ts")))
# MAGIC     .withColumn("hour",    hour(col("ts")))
# MAGIC     # Spark's dayofweek is 1=Sunday..7=Saturday. Convert to ISO 1=Monday..7=Sunday.
# MAGIC     .withColumn("weekday", ((dayofweek(col("ts")) + 5) % 7) + 1)
# MAGIC )
# MAGIC
# MAGIC df_bronze.write.mode("overwrite").saveAsTable(BRONZE_TABLE)
# MAGIC print(f"Bronze rows: {spark.table(BRONZE_TABLE).count():,}")
# MAGIC df_bronze.printSchema()
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Aggregate to daily totals per station
# MAGIC
# MAGIC Window functions need a clean grain. Collapse the 15-minute observations
# MAGIC into one row per `(station_id, date)`, summing the four direction counters.
# MAGIC `coalesce(... , 0)` treats missing pedestrian counts as zero for stations
# MAGIC that only count bikes — otherwise the sum becomes NULL.
# MAGIC
# MAGIC Save to `GOLD_DAILY` with two columns `bikes_total` and `peds_total`.

# COMMAND ----------

# TODO: groupBy(station_id, date), aggregate sum(VELO_IN+VELO_OUT) and
#       sum(FUSS_IN+FUSS_OUT) with coalesce-to-0, save to GOLD_DAILY.

# YOUR CODE HERE
raise NotImplementedError("Step 2: build the daily Gold table")

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary><b>💡 Hint – useful building blocks</b></summary>
# MAGIC
# MAGIC You will need: `COALESCE`, `SUM`, `.groupBy()`, `.agg()`, `.orderBy()`, `.saveAsTable()`, `.count()`
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🔑 Solution</b></summary>
# MAGIC
# MAGIC Try it yourself first – then compare.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import sum as spark_sum, coalesce, lit
# MAGIC
# MAGIC df_daily = (spark.table(BRONZE_TABLE)
# MAGIC     .groupBy("station_id", "date")
# MAGIC     .agg(
# MAGIC         spark_sum(coalesce(col("VELO_IN"),  lit(0)) + coalesce(col("VELO_OUT"),  lit(0))).alias("bikes_total"),
# MAGIC         spark_sum(coalesce(col("FUSS_IN"),  lit(0)) + coalesce(col("FUSS_OUT"),  lit(0))).alias("peds_total"),
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC df_daily.write.mode("overwrite").saveAsTable(GOLD_DAILY)
# MAGIC print(f"Daily rows: {spark.table(GOLD_DAILY).count():,}")
# MAGIC display(spark.table(GOLD_DAILY).orderBy("station_id", "date").limit(10))
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Window — running totals (cumulative sum)
# MAGIC
# MAGIC Compute a running total of bikes per station, accumulating from the start
# MAGIC of the year to the current date. The frame `ROWS BETWEEN UNBOUNDED
# MAGIC PRECEDING AND CURRENT ROW` is the textbook running-total window.
# MAGIC
# MAGIC Display only one busy station (e.g. `station_id = 4241`) so the output is
# MAGIC readable.

# COMMAND ----------

# TODO: define a Window partitioned by station_id, ordered by date,
#       framed unboundedPreceding..currentRow. Add a bikes_ytd column.

# YOUR CODE HERE
raise NotImplementedError("Step 3: running totals")

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary><b>💡 Hint – useful building blocks</b></summary>
# MAGIC
# MAGIC You will need: `.withColumn()`, `.filter()`, `.orderBy()`, `.over()`, `.partitionBy()`, `.rowsBetween()`, `Window.unboundedPreceding()`, `Window.currentRow()`
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🔑 Solution</b></summary>
# MAGIC
# MAGIC Try it yourself first – then compare.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.window import Window
# MAGIC
# MAGIC w_running = (Window
# MAGIC     .partitionBy("station_id")
# MAGIC     .orderBy("date")
# MAGIC     .rowsBetween(Window.unboundedPreceding, Window.currentRow))
# MAGIC
# MAGIC df_running = (spark.table(GOLD_DAILY)
# MAGIC     .withColumn("bikes_ytd", spark_sum("bikes_total").over(w_running)))
# MAGIC
# MAGIC # Pick one busy station for a readable preview.
# MAGIC display(df_running.filter("station_id = 4241").orderBy("date").limit(15))
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC Notice how `bikes_ytd` strictly grows. If you change the order column, the
# MAGIC running total takes a different path through the same data — order matters.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Window — 7-day moving average
# MAGIC
# MAGIC Smooth the daily counts with a 7-day rolling mean, ending at the current
# MAGIC row. The frame `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` covers the
# MAGIC current day plus the six days before it — seven values total.
# MAGIC
# MAGIC Solve this in **SQL** (use a `%sql` cell) — practice both APIs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: SELECT station_id, date, bikes_total, and a 7-day moving average
# MAGIC --       (round to 1 decimal). PARTITION BY station_id ORDER BY date,
# MAGIC --       ROWS BETWEEN 6 PRECEDING AND CURRENT ROW.
# MAGIC --       Filter to one station for readability and LIMIT 20.

# COMMAND ----------

# MAGIC %md
# MAGIC The first six rows of any station show a moving average computed over fewer
# MAGIC than seven days — Spark just takes whatever rows are available.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Window — dense_rank (busiest days per station)
# MAGIC
# MAGIC Ranking functions assign positions to rows inside a partition. The three
# MAGIC common variants behave differently when there are ties:
# MAGIC
# MAGIC | Function | On ties | Gaps in rank? |
# MAGIC |---|---|---|
# MAGIC | `row_number()` | Arbitrary tiebreak | No |
# MAGIC | `rank()` | Same rank | Yes (skips next rank) |
# MAGIC | `dense_rank()` | Same rank | No |
# MAGIC
# MAGIC Use `dense_rank()` to find the busiest 5 days per station.

# COMMAND ----------

# TODO: rank by bikes_total desc within each station_id, keep rows where rank <= 5.

# YOUR CODE HERE
raise NotImplementedError("Step 5: dense_rank top 5")

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary><b>💡 Hint – useful building blocks</b></summary>
# MAGIC
# MAGIC You will need: `DENSE_RANK`, `.withColumn()`, `.filter()`, `.orderBy()`, `.over()`, `.partitionBy()`, `Window.partitionBy()`
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🔑 Solution</b></summary>
# MAGIC
# MAGIC Try it yourself first – then compare.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import dense_rank
# MAGIC
# MAGIC w_rank = Window.partitionBy("station_id").orderBy(col("bikes_total").desc())
# MAGIC
# MAGIC df_top5 = (spark.table(GOLD_DAILY)
# MAGIC     .withColumn("rank_busiest", dense_rank().over(w_rank))
# MAGIC     .filter("rank_busiest <= 5"))
# MAGIC
# MAGIC display(df_top5.filter("station_id = 4241").orderBy("rank_busiest"))
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Window — lag/lead and day-over-day spike flag
# MAGIC
# MAGIC `lag(col, n)` returns the value of `col` from `n` rows earlier in the
# MAGIC partition; `lead(col, n)` looks ahead. Both default to NULL when there is
# MAGIC no neighbour, which is why the first row per station has no previous-day
# MAGIC delta.
# MAGIC
# MAGIC Flag any day where the bike count grew by more than 50% relative to the
# MAGIC previous day at the same station — the kind of anomaly a data quality
# MAGIC check would surface. Watch out for division by zero on the previous day.

# COMMAND ----------

# TODO: add bikes_prev_day via lag(), pct_change (handle NULL and zero), is_spike.

# YOUR CODE HERE
raise NotImplementedError("Step 6: lag and spike detection")

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary><b>💡 Hint – useful building blocks</b></summary>
# MAGIC
# MAGIC You will need: `LAG`, `.withColumn()`, `.filter()`, `.orderBy()`, `.over()`, `.partitionBy()`, `Window.partitionBy()`
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🔑 Solution</b></summary>
# MAGIC
# MAGIC Try it yourself first – then compare.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import lag
# MAGIC
# MAGIC w_neighbours = Window.partitionBy("station_id").orderBy("date")
# MAGIC
# MAGIC df_dod = (spark.table(GOLD_DAILY)
# MAGIC     .withColumn("bikes_prev_day", lag("bikes_total", 1).over(w_neighbours))
# MAGIC     .withColumn(
# MAGIC         "pct_change",
# MAGIC         when(col("bikes_prev_day").isNull() | (col("bikes_prev_day") == 0), None)
# MAGIC         .otherwise((col("bikes_total") - col("bikes_prev_day")) / col("bikes_prev_day"))
# MAGIC     )
# MAGIC     .withColumn("is_spike", col("pct_change") > 0.5))
# MAGIC
# MAGIC display(df_dod.filter("station_id = 4241 AND is_spike = true").orderBy("date").limit(20))
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Pivot — hour × weekday matrix
# MAGIC
# MAGIC `groupBy(...).pivot(col).agg(...)` reshapes from long to wide. The pivot
# MAGIC column becomes one column per distinct value. Listing the pivot values
# MAGIC explicitly (`pivot("weekday", [1,2,3,4,5,6,7])`) is faster than letting
# MAGIC Spark discover them, because it avoids a second pass over the data.
# MAGIC
# MAGIC Build a table where rows are hours-of-day (0..23), columns are weekdays
# MAGIC (1..7 — rename to `mon`, `tue`, …), values are average bikes per hour.

# COMMAND ----------

# TODO: aggregate to one row per (date, hour, weekday) with bikes_hour,
#       then pivot(weekday).avg(bikes_hour). Rename columns 1..7 to mon..sun.

# YOUR CODE HERE
raise NotImplementedError("Step 7: pivot")

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary><b>💡 Hint – useful building blocks</b></summary>
# MAGIC
# MAGIC You will need: `COALESCE`, `HOUR`, `SUM`, `AVG`, `.groupBy()`, `.agg()`, `.withColumnRenamed()`, `.orderBy()`, `.pivot()`
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🔑 Solution</b></summary>
# MAGIC
# MAGIC Try it yourself first – then compare.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import avg, sum as spark_sum
# MAGIC
# MAGIC # Aggregate first to one row per (date, hour, weekday) per station, then pivot.
# MAGIC df_hourly = (spark.table(BRONZE_TABLE)
# MAGIC     .groupBy("date", "hour", "weekday")
# MAGIC     .agg(spark_sum(coalesce(col("VELO_IN"), lit(0)) + coalesce(col("VELO_OUT"), lit(0))).alias("bikes_hour")))
# MAGIC
# MAGIC df_pivot = (df_hourly
# MAGIC     .groupBy("hour")
# MAGIC     .pivot("weekday", [1, 2, 3, 4, 5, 6, 7])
# MAGIC     .agg(avg("bikes_hour")))
# MAGIC
# MAGIC # Rename pivot columns from "1","2",... to "mon","tue",... for clarity.
# MAGIC weekday_names = {1: "mon", 2: "tue", 3: "wed", 4: "thu", 5: "fri", 6: "sat", 7: "sun"}
# MAGIC for num, name in weekday_names.items():
# MAGIC     df_pivot = df_pivot.withColumnRenamed(str(num), name)
# MAGIC
# MAGIC display(df_pivot.orderBy("hour"))
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC The morning and evening commuter peaks should be visible on weekdays
# MAGIC (mon–fri) but flatter on weekends.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Unpivot — back to long form with `stack()`
# MAGIC
# MAGIC Sometimes you receive a wide spreadsheet-style table and want to unpivot it
# MAGIC for further analysis. Spark has no direct `unpivot()` in Python until very
# MAGIC recent versions, but the SQL `stack()` function works on every runtime.
# MAGIC
# MAGIC `stack(n, k1, v1, k2, v2, ...)` produces `n` rows from one input row, each
# MAGIC with one `(key, value)` pair.

# COMMAND ----------

# TODO: use selectExpr with stack(7, 'mon', mon, 'tue', tue, ...) AS (weekday_name, bikes_avg)
#       to turn the pivot back into a long-form (hour, weekday_name, bikes_avg) table.

# YOUR CODE HERE
raise NotImplementedError("Step 8: unpivot with stack()")

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary><b>💡 Hint – useful building blocks</b></summary>
# MAGIC
# MAGIC You will need: `HOUR`, `.orderBy()`, `.selectExpr()`
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🔑 Solution</b></summary>
# MAGIC
# MAGIC Try it yourself first – then compare.
# MAGIC
# MAGIC ```python
# MAGIC df_long = df_pivot.selectExpr(
# MAGIC     "hour",
# MAGIC     "stack(7, 'mon', mon, 'tue', tue, 'wed', wed, 'thu', thu, 'fri', fri, 'sat', sat, 'sun', sun) AS (weekday_name, bikes_avg)"
# MAGIC )
# MAGIC
# MAGIC display(df_long.orderBy("hour", "weekday_name").limit(20))
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC The unpivoted long-form table is what you'd typically feed into a
# MAGIC visualisation library or another window function.
