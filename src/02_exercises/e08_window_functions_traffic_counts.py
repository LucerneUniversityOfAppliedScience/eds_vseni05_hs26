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
# MAGIC The bundle must be deployed and the setup job `01_workshop_setup`
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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>TO_DATE</code>, <code>HOUR</code>, <code>.withColumn()</code>, <code>.withColumnRenamed()</code>, <code>.saveAsTable()</code>, <code>.printSchema()</code>, <code>.count()</code>, <code>.schema()</code>, <code>.option()</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType
# MAGIC from pyspark.sql.functions import col, to_date, hour, dayofweek, when, expr
# MAGIC 
# MAGIC traffic_schema = StructType([
# MAGIC     StructField(&quot;FK_STANDORT&quot;, IntegerType(),   True),
# MAGIC     StructField(&quot;DATUM&quot;,       TimestampType(), True),
# MAGIC     StructField(&quot;VELO_IN&quot;,     IntegerType(),   True),
# MAGIC     StructField(&quot;VELO_OUT&quot;,    IntegerType(),   True),
# MAGIC     StructField(&quot;FUSS_IN&quot;,     IntegerType(),   True),
# MAGIC     StructField(&quot;FUSS_OUT&quot;,    IntegerType(),   True),
# MAGIC     StructField(&quot;OST&quot;,         DoubleType(),    True),
# MAGIC     StructField(&quot;NORD&quot;,        DoubleType(),    True),
# MAGIC ])
# MAGIC 
# MAGIC df_raw = (spark.read
# MAGIC     .option(&quot;header&quot;, &quot;true&quot;)
# MAGIC     .option(&quot;timestampFormat&quot;, &quot;yyyy-MM-dd&#x27;T&#x27;HH:mm&quot;)
# MAGIC     .schema(traffic_schema)
# MAGIC     .csv(CSV_PATH))
# MAGIC 
# MAGIC df_bronze = (df_raw
# MAGIC     .withColumnRenamed(&quot;FK_STANDORT&quot;, &quot;station_id&quot;)
# MAGIC     .withColumnRenamed(&quot;DATUM&quot;,       &quot;ts&quot;)
# MAGIC     .withColumn(&quot;date&quot;,    to_date(col(&quot;ts&quot;)))
# MAGIC     .withColumn(&quot;hour&quot;,    hour(col(&quot;ts&quot;)))
# MAGIC     # Spark&#x27;s dayofweek is 1=Sunday..7=Saturday. Convert to ISO 1=Monday..7=Sunday.
# MAGIC     .withColumn(&quot;weekday&quot;, ((dayofweek(col(&quot;ts&quot;)) + 5) % 7) + 1)
# MAGIC )
# MAGIC 
# MAGIC df_bronze.write.mode(&quot;overwrite&quot;).saveAsTable(BRONZE_TABLE)
# MAGIC print(f&quot;Bronze rows: {spark.table(BRONZE_TABLE).count():,}&quot;)
# MAGIC df_bronze.printSchema()</code></pre></details>

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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>COALESCE</code>, <code>SUM</code>, <code>.groupBy()</code>, <code>.agg()</code>, <code>.orderBy()</code>, <code>.saveAsTable()</code>, <code>.count()</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>from pyspark.sql.functions import sum as spark_sum, coalesce, lit
# MAGIC 
# MAGIC df_daily = (spark.table(BRONZE_TABLE)
# MAGIC     .groupBy(&quot;station_id&quot;, &quot;date&quot;)
# MAGIC     .agg(
# MAGIC         spark_sum(coalesce(col(&quot;VELO_IN&quot;),  lit(0)) + coalesce(col(&quot;VELO_OUT&quot;),  lit(0))).alias(&quot;bikes_total&quot;),
# MAGIC         spark_sum(coalesce(col(&quot;FUSS_IN&quot;),  lit(0)) + coalesce(col(&quot;FUSS_OUT&quot;),  lit(0))).alias(&quot;peds_total&quot;),
# MAGIC     )
# MAGIC )
# MAGIC 
# MAGIC df_daily.write.mode(&quot;overwrite&quot;).saveAsTable(GOLD_DAILY)
# MAGIC print(f&quot;Daily rows: {spark.table(GOLD_DAILY).count():,}&quot;)
# MAGIC display(spark.table(GOLD_DAILY).orderBy(&quot;station_id&quot;, &quot;date&quot;).limit(10))</code></pre></details>

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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>.withColumn()</code>, <code>.filter()</code>, <code>.orderBy()</code>, <code>.over()</code>, <code>.partitionBy()</code>, <code>.rowsBetween()</code>, <code>Window.unboundedPreceding()</code>, <code>Window.currentRow()</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>from pyspark.sql.window import Window
# MAGIC 
# MAGIC w_running = (Window
# MAGIC     .partitionBy(&quot;station_id&quot;)
# MAGIC     .orderBy(&quot;date&quot;)
# MAGIC     .rowsBetween(Window.unboundedPreceding, Window.currentRow))
# MAGIC 
# MAGIC df_running = (spark.table(GOLD_DAILY)
# MAGIC     .withColumn(&quot;bikes_ytd&quot;, spark_sum(&quot;bikes_total&quot;).over(w_running)))
# MAGIC 
# MAGIC # Pick one busy station for a readable preview.
# MAGIC display(df_running.filter(&quot;station_id = 4241&quot;).orderBy(&quot;date&quot;).limit(15))</code></pre></details>

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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>DENSE_RANK</code>, <code>.withColumn()</code>, <code>.filter()</code>, <code>.orderBy()</code>, <code>.over()</code>, <code>.partitionBy()</code>, <code>Window.partitionBy()</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>from pyspark.sql.functions import dense_rank
# MAGIC 
# MAGIC w_rank = Window.partitionBy(&quot;station_id&quot;).orderBy(col(&quot;bikes_total&quot;).desc())
# MAGIC 
# MAGIC df_top5 = (spark.table(GOLD_DAILY)
# MAGIC     .withColumn(&quot;rank_busiest&quot;, dense_rank().over(w_rank))
# MAGIC     .filter(&quot;rank_busiest &lt;= 5&quot;))
# MAGIC 
# MAGIC display(df_top5.filter(&quot;station_id = 4241&quot;).orderBy(&quot;rank_busiest&quot;))</code></pre></details>

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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>LAG</code>, <code>.withColumn()</code>, <code>.filter()</code>, <code>.orderBy()</code>, <code>.over()</code>, <code>.partitionBy()</code>, <code>Window.partitionBy()</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>from pyspark.sql.functions import lag
# MAGIC 
# MAGIC w_neighbours = Window.partitionBy(&quot;station_id&quot;).orderBy(&quot;date&quot;)
# MAGIC 
# MAGIC df_dod = (spark.table(GOLD_DAILY)
# MAGIC     .withColumn(&quot;bikes_prev_day&quot;, lag(&quot;bikes_total&quot;, 1).over(w_neighbours))
# MAGIC     .withColumn(
# MAGIC         &quot;pct_change&quot;,
# MAGIC         when(col(&quot;bikes_prev_day&quot;).isNull() | (col(&quot;bikes_prev_day&quot;) == 0), None)
# MAGIC         .otherwise((col(&quot;bikes_total&quot;) - col(&quot;bikes_prev_day&quot;)) / col(&quot;bikes_prev_day&quot;))
# MAGIC     )
# MAGIC     .withColumn(&quot;is_spike&quot;, col(&quot;pct_change&quot;) &gt; 0.5))
# MAGIC 
# MAGIC display(df_dod.filter(&quot;station_id = 4241 AND is_spike = true&quot;).orderBy(&quot;date&quot;).limit(20))</code></pre></details>

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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>COALESCE</code>, <code>HOUR</code>, <code>SUM</code>, <code>AVG</code>, <code>.groupBy()</code>, <code>.agg()</code>, <code>.withColumnRenamed()</code>, <code>.orderBy()</code>, <code>.pivot()</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>from pyspark.sql.functions import avg, sum as spark_sum
# MAGIC 
# MAGIC # Aggregate first to one row per (date, hour, weekday) per station, then pivot.
# MAGIC df_hourly = (spark.table(BRONZE_TABLE)
# MAGIC     .groupBy(&quot;date&quot;, &quot;hour&quot;, &quot;weekday&quot;)
# MAGIC     .agg(spark_sum(coalesce(col(&quot;VELO_IN&quot;), lit(0)) + coalesce(col(&quot;VELO_OUT&quot;), lit(0))).alias(&quot;bikes_hour&quot;)))
# MAGIC 
# MAGIC df_pivot = (df_hourly
# MAGIC     .groupBy(&quot;hour&quot;)
# MAGIC     .pivot(&quot;weekday&quot;, [1, 2, 3, 4, 5, 6, 7])
# MAGIC     .agg(avg(&quot;bikes_hour&quot;)))
# MAGIC 
# MAGIC # Rename pivot columns from &quot;1&quot;,&quot;2&quot;,... to &quot;mon&quot;,&quot;tue&quot;,... for clarity.
# MAGIC weekday_names = {1: &quot;mon&quot;, 2: &quot;tue&quot;, 3: &quot;wed&quot;, 4: &quot;thu&quot;, 5: &quot;fri&quot;, 6: &quot;sat&quot;, 7: &quot;sun&quot;}
# MAGIC for num, name in weekday_names.items():
# MAGIC     df_pivot = df_pivot.withColumnRenamed(str(num), name)
# MAGIC 
# MAGIC display(df_pivot.orderBy(&quot;hour&quot;))</code></pre></details>

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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>HOUR</code>, <code>.orderBy()</code>, <code>.selectExpr()</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>df_long = df_pivot.selectExpr(
# MAGIC     &quot;hour&quot;,
# MAGIC     &quot;stack(7, &#x27;mon&#x27;, mon, &#x27;tue&#x27;, tue, &#x27;wed&#x27;, wed, &#x27;thu&#x27;, thu, &#x27;fri&#x27;, fri, &#x27;sat&#x27;, sat, &#x27;sun&#x27;, sun) AS (weekday_name, bikes_avg)&quot;
# MAGIC )
# MAGIC 
# MAGIC display(df_long.orderBy(&quot;hour&quot;, &quot;weekday_name&quot;).limit(20))</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC The unpivoted long-form table is what you'd typically feed into a
# MAGIC visualisation library or another window function.
