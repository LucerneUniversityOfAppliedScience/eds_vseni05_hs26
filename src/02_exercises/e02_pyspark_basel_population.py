# Databricks notebook source

# MAGIC %md
# MAGIC # Basel Population Analysis – Exercise
# MAGIC
# MAGIC In this exercise you will analyse the residential population of the Canton of Basel-Stadt
# MAGIC using PySpark and SQL.
# MAGIC
# MAGIC **Dataset:** Wohnbevölkerung nach Geschlecht und Staatsangehörigkeit (opendata.swiss)
# MAGIC — already available in the Unity Catalog Volume.
# MAGIC
# MAGIC **Columns:**
# MAGIC
# MAGIC | Column | Description |
# MAGIC |--------|-------------|
# MAGIC | `datum` | Reference date (string, e.g. "31. Dezember 2023") |
# MAGIC | `gemeinde` | Municipality: Basel, Riehen, or Bettingen |
# MAGIC | `geschlecht` | Gender: M (male) or W (female) |
# MAGIC | `staatsangehoerigkeit` | Nationality (German name) |
# MAGIC | `anzahl` | Number of residents |
# MAGIC | `jahr` | Year (string, e.g. "2023") |
# MAGIC | `wohnviertel_name` | Neighbourhood name |
# MAGIC | `wohnviertel_id` | Neighbourhood ID |

# COMMAND ----------

CATALOG      = "workspace"
SCHEMA       = "exercises"
PARQUET_PATH = (
    f"/Volumes/{CATALOG}/mobility_raw/landing/sample_data/parquet/"
    "Kanton_BS_Wohnbevoelkerung_nach_Geschlecht_Staatsangehoerigkeit_und_Wohnviertel.parquet"
)
TABLE_POP    = f"{CATALOG}.{SCHEMA}.bs_population"
TABLE_AGG    = f"{CATALOG}.{SCHEMA}.bs_population_by_country"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup: drop existing tables
# MAGIC
# MAGIC Drop any existing tables before re-running so a stale schema from a previous
# MAGIC run does not block the overwrite (Delta refuses schema changes on `overwrite`
# MAGIC when Table ACLs are enabled).

# COMMAND ----------

for table in [TABLE_POP, TABLE_AGG]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped (if existed): {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 1 – Read the Parquet File
# MAGIC
# MAGIC Read the file from the Unity Catalog Volume into a Spark DataFrame.
# MAGIC Use `inferSchema` so Spark detects column types automatically.

# COMMAND ----------

# TODO: read the parquet file from PARQUET_PATH into df_pop
# Hint: spark.read.format("parquet").option(...).load(...)  or  spark.read.parquet(...)

raise NotImplementedError("Task 1: read the parquet file")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>.count()</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>df_pop = spark.read.parquet(PARQUET_PATH)
# MAGIC 
# MAGIC print(f&quot;Rows: {df_pop.count():,}&quot;)</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 – Inspect the Data
# MAGIC
# MAGIC Show the first 10 rows and print the schema.
# MAGIC Does everything look correct?

# COMMAND ----------

# TODO: show the first 10 rows and the schema of df_pop
# Hint: df_pop.show(10) and df_pop.printSchema()

raise NotImplementedError("Task 2: inspect the data")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>.printSchema()</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>df_pop.limit(10).display()</code></pre><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>df_pop.printSchema()</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 – Explicit Schema
# MAGIC
# MAGIC Read the file again, this time with an **explicit schema**.
# MAGIC Use `LongType` for `anzahl` (the Parquet file stores it as INT64) and `StringType` for the rest.
# MAGIC
# MAGIC Hint: instead of `.option("inferSchema", "true")` use `.schema(my_schema)`.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType

# TODO: define bs_schema with the correct types for all 8 columns
# (use LongType for anzahl, StringType for everything else)
bs_schema = None

raise NotImplementedError("Task 3: define bs_schema and re-read the file")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>.printSchema()</code>, <code>.schema()</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>from pyspark.sql.types import StructType, StructField, StringType, LongType
# MAGIC 
# MAGIC bs_schema = StructType([
# MAGIC     StructField(&quot;datum&quot;,                StringType(), True),
# MAGIC     StructField(&quot;gemeinde&quot;,             StringType(), True),
# MAGIC     StructField(&quot;geschlecht&quot;,           StringType(), True),
# MAGIC     StructField(&quot;staatsangehoerigkeit&quot;, StringType(), True),
# MAGIC     StructField(&quot;anzahl&quot;,               LongType(),   True),
# MAGIC     StructField(&quot;jahr&quot;,                 StringType(), True),
# MAGIC     StructField(&quot;wohnviertel_name&quot;,     StringType(), True),
# MAGIC     StructField(&quot;wohnviertel_id&quot;,       StringType(), True),
# MAGIC ])
# MAGIC 
# MAGIC df_pop = spark.read.schema(bs_schema).parquet(PARQUET_PATH)
# MAGIC df_pop.printSchema()</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 – Save as Delta Table
# MAGIC
# MAGIC Save `df_pop` as a managed Delta table in Unity Catalog.
# MAGIC Use `TABLE_POP` as the table name and overwrite if it already exists.
# MAGIC
# MAGIC Afterwards, find the table in the left sidebar under **Catalog → workspace → exercises**.

# COMMAND ----------

# TODO: write df_pop as a Delta table to TABLE_POP (mode: overwrite)
# Hint: df_pop.write.mode("overwrite").saveAsTable(TABLE_POP)

raise NotImplementedError("Task 4: save as Delta table")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>.saveAsTable()</code>, <code>.count()</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>(
# MAGIC     df_pop
# MAGIC     .write
# MAGIC     .mode(&quot;overwrite&quot;)
# MAGIC     .saveAsTable(TABLE_POP)
# MAGIC )
# MAGIC 
# MAGIC print(f&quot;Saved: {TABLE_POP}  ({spark.table(TABLE_POP).count():,} rows)&quot;)</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 – Test: Iran in 1980
# MAGIC
# MAGIC Write a SQL query that returns all rows where `staatsangehoerigkeit` is `'Iran'`
# MAGIC and `jahr` is `'1980'`. How many rows are there?

# COMMAND ----------

# TODO: write a spark.sql() or %sql query to check for Iran / 1980
# Hint: SELECT * FROM {TABLE_POP} WHERE staatsangehoerigkeit = 'Iran' AND jahr = '1980'

raise NotImplementedError("Task 5: query for Iran, 1980")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>.count()</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>df_iran_1980 = spark.sql(f&quot;&quot;&quot;
# MAGIC     SELECT *
# MAGIC     FROM {TABLE_POP}
# MAGIC     WHERE staatsangehoerigkeit = &#x27;Iran&#x27;
# MAGIC       AND jahr = &#x27;1980&#x27;
# MAGIC &quot;&quot;&quot;)
# MAGIC 
# MAGIC print(f&quot;Rows found: {df_iran_1980.count()}&quot;)
# MAGIC df_iran_1980.display()</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 – Population by Country and Year
# MAGIC
# MAGIC Write a SQL query that sums `anzahl` **per `staatsangehoerigkeit` and `jahr`**,
# MAGIC excludes `'Schweiz'`, and sorts the result by `anzahl` descending.

# COMMAND ----------

# TODO: write the aggregation query and store the result in df_by_country
# Hint: SELECT staatsangehoerigkeit, jahr, sum(anzahl) AS anzahl
#         FROM ... WHERE staatsangehoerigkeit <> 'Schweiz'
#         GROUP BY ... ORDER BY anzahl DESC

raise NotImplementedError("Task 6: aggregate by country and year")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>GROUP BY</code>, <code>ORDER BY</code>, <code>SUM</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>df_by_country = spark.sql(f&quot;&quot;&quot;
# MAGIC     SELECT
# MAGIC         staatsangehoerigkeit,
# MAGIC         jahr,
# MAGIC         SUM(anzahl) AS anzahl
# MAGIC     FROM {TABLE_POP}
# MAGIC     WHERE staatsangehoerigkeit != &#x27;Schweiz&#x27;
# MAGIC     GROUP BY staatsangehoerigkeit, jahr
# MAGIC     ORDER BY anzahl DESC
# MAGIC &quot;&quot;&quot;)
# MAGIC 
# MAGIC df_by_country.limit(10).display()</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 – Save the Aggregation as a Delta Table
# MAGIC
# MAGIC Save the result from Task 6 as `TABLE_AGG`.

# COMMAND ----------

# TODO: write df_by_country to TABLE_AGG
# Hint: same pattern as Task 4

raise NotImplementedError("Task 7: save aggregation table")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>.saveAsTable()</code>, <code>.count()</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>(
# MAGIC     df_by_country
# MAGIC     .write
# MAGIC     .mode(&quot;overwrite&quot;)
# MAGIC     .saveAsTable(TABLE_AGG)
# MAGIC )
# MAGIC 
# MAGIC print(f&quot;Saved: {TABLE_AGG}  ({spark.table(TABLE_AGG).count():,} rows)&quot;)</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 – Visualize: Population by Year and Country
# MAGIC
# MAGIC Load `TABLE_AGG` and display it. Then use the **built-in Databricks visualization**
# MAGIC (click the **+** button above the table output) to create a bar chart:
# MAGIC - X axis: `jahr`
# MAGIC - Y axis: `anzahl`
# MAGIC - Group by: `staatsangehoerigkeit`
# MAGIC
# MAGIC Tip: filter to the **top 10 countries** (by total `anzahl`) to keep the chart readable.

# COMMAND ----------

# TODO: load TABLE_AGG, filter to top 10 countries, and display the result
# Hint: first pick the top 10 countries by total anzahl (GROUP BY + ORDER BY + LIMIT 10),
# then join/filter TABLE_AGG to only those countries

raise NotImplementedError("Task 8: load and display aggregation for visualization")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>GROUP BY</code>, <code>ORDER BY</code>, <code>LIMIT</code>, <code>IN</code>, <code>SUM</code>, <code>.join()</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code># Determine top 10 nationalities by total anzahl
# MAGIC top10 = spark.sql(f&quot;&quot;&quot;
# MAGIC     SELECT staatsangehoerigkeit
# MAGIC     FROM {TABLE_AGG}
# MAGIC     GROUP BY staatsangehoerigkeit
# MAGIC     ORDER BY SUM(anzahl) DESC
# MAGIC     LIMIT 10
# MAGIC &quot;&quot;&quot;).toPandas()[&quot;staatsangehoerigkeit&quot;].tolist()
# MAGIC 
# MAGIC # Load the aggregation table filtered to top 10
# MAGIC df_top10 = spark.sql(f&quot;&quot;&quot;
# MAGIC     SELECT staatsangehoerigkeit, jahr, anzahl
# MAGIC     FROM {TABLE_AGG}
# MAGIC     WHERE staatsangehoerigkeit IN ({&quot;, &quot;.join(f&quot;&#x27;{c}&#x27;&quot; for c in top10)})
# MAGIC     ORDER BY jahr, anzahl DESC
# MAGIC &quot;&quot;&quot;)
# MAGIC 
# MAGIC df_top10.display()</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 9 – Filter: Women in Basel, 2010
# MAGIC
# MAGIC Use `spark.sql()` to load only rows where:
# MAGIC - `gemeinde` = `'Basel'`
# MAGIC - `geschlecht` = `'W'`
# MAGIC - `jahr` = `'2010'`
# MAGIC
# MAGIC Store the result in `df_women_2010`.

# COMMAND ----------

# TODO: use spark.sql() with a WHERE clause to filter the table
# Hint: df_women_2010 = spark.sql(f"SELECT * FROM {TABLE_POP} WHERE gemeinde='Basel' AND geschlecht='W' AND jahr='2010'")

raise NotImplementedError("Task 9: filter for women in Basel, 2010")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>.count()</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>df_women_2010 = spark.sql(f&quot;&quot;&quot;
# MAGIC     SELECT *
# MAGIC     FROM {TABLE_POP}
# MAGIC     WHERE gemeinde    = &#x27;Basel&#x27;
# MAGIC       AND geschlecht  = &#x27;W&#x27;
# MAGIC       AND jahr        = &#x27;2010&#x27;
# MAGIC &quot;&quot;&quot;)
# MAGIC 
# MAGIC print(f&quot;Rows: {df_women_2010.count():,}&quot;)
# MAGIC df_women_2010.limit(5).display()</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 10 – Visualize: Where Do the Women Come From?
# MAGIC
# MAGIC Display `df_women_2010` and use the **Databricks built-in visualization** to create
# MAGIC a bar chart showing the number of women per `staatsangehoerigkeit`.
# MAGIC Sort by `anzahl` descending so the largest groups appear first.

# COMMAND ----------

# TODO: display df_women_2010 (sorted by anzahl desc)
# Hint: display(df_women_2010.orderBy(col("anzahl").desc()))

raise NotImplementedError("Task 10: display women data for visualization")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>GROUP BY</code>, <code>ORDER BY</code>, <code>SUM</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>df_women_2010_by_country = spark.sql(f&quot;&quot;&quot;
# MAGIC     SELECT staatsangehoerigkeit, SUM(anzahl) AS anzahl
# MAGIC     FROM {TABLE_POP}
# MAGIC     WHERE gemeinde   = &#x27;Basel&#x27;
# MAGIC       AND geschlecht = &#x27;W&#x27;
# MAGIC       AND jahr       = &#x27;2010&#x27;
# MAGIC     GROUP BY staatsangehoerigkeit
# MAGIC     ORDER BY anzahl DESC
# MAGIC &quot;&quot;&quot;)
# MAGIC 
# MAGIC df_women_2010_by_country.display()</code></pre></details>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS workspace.exercises.bs_population;
# MAGIC DROP TABLE IF EXISTS workspace.exercises.bs_population_by_country
