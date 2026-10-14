# Databricks notebook source
# MAGIC %md
# MAGIC # 01 – Databricks basics: workspace, Unity Catalog & your first data
# MAGIC
# MAGIC **Goal (≈ 30 min):** Find your way around the Databricks workspace, understand where data lives,
# MAGIC and read your first mobility data with Python and SQL.
# MAGIC
# MAGIC **Dataset:** automatic bike and pedestrian counts of the City of Zurich – at dozens of counting stations,
# MAGIC bikes and pedestrians are counted every 15 minutes, since 2009.
# MAGIC
# MAGIC ### How a notebook works
# MAGIC - A notebook consists of **cells**. Run a cell with `Shift + Enter` (or ▶︎ at the top left of the cell).
# MAGIC - At the top right choose **Connect → Serverless** (Free Edition only offers serverless compute).
# MAGIC - Cells can contain Python, SQL or Markdown (`%sql`, `%md`).
# MAGIC - The Spark session `spark` is available automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1 – A quick tour of the workspace
# MAGIC
# MAGIC Have a look at these areas in the left sidebar (2–3 minutes):
# MAGIC
# MAGIC | Area | What for? |
# MAGIC |---|---|
# MAGIC | **Workspace** | folders, notebooks, Git folders – this repo lives here |
# MAGIC | **Catalog** | Unity Catalog: all tables, volumes (files) and their permissions |
# MAGIC | **Jobs & Pipelines** | automated workflows – e.g. the setup job that loaded the data |
# MAGIC | **SQL Editor** | SQL queries without a notebook |
# MAGIC | **Dashboards** | AI/BI dashboards |
# MAGIC | **Genie** | ask questions about your data in natural language |
# MAGIC
# MAGIC ### Unity Catalog – the filing system for data
# MAGIC
# MAGIC Every table has a **three-part name**: `catalog.schema.table`
# MAGIC
# MAGIC ```
# MAGIC workspace                      ← catalog (Free Edition: "workspace")
# MAGIC ├── mobility_raw               ← schema
# MAGIC │   └── landing                ← volume: the raw files live here
# MAGIC ├── mobility_bronze            ← project: raw data as tables
# MAGIC ├── mobility_silver            ← project: cleaned data
# MAGIC ├── mobility_gold              ← project: key figures for the dashboard
# MAGIC ├── exercises                  ← tables of the basic exercises
# MAGIC └── adventureworks             ← sample database for the SQL exercises
# MAGIC ```
# MAGIC
# MAGIC These schemas and the volume were **not created by hand** but by the
# MAGIC **Declarative Automation Bundle** (`databricks bundle deploy`) – see `resources/unity_catalog.yml`.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2 – Configuration
# MAGIC
# MAGIC We define paths and names once at the top – this keeps the notebook easy to adapt.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

LANDING = f"/Volumes/{CATALOG}/mobility_raw/landing"
BIKE_PATH = f"{LANDING}/mobility/bike_pedestrian_counts/"

print(LANDING)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3 – Which schemas and files are there?

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

# Files in a volume can be listed like a normal file system
display(dbutils.fs.ls(LANDING))

# COMMAND ----------

display(dbutils.fs.ls(f"{LANDING}/mobility"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4 – Read an existing table
# MAGIC
# MAGIC Most of the time you do not read files but **tables that already exist** in Unity Catalog.
# MAGIC Every table is addressed with its **three-part name** `catalog.schema.table`.
# MAGIC
# MAGIC Two good examples are already in your workspace:
# MAGIC - `samples.nyctaxi.trips` – sample data that exists in **every** Databricks workspace
# MAGIC - `workspace.adventureworks.production_product` – loaded by the setup job (products of a bike manufacturer)
# MAGIC
# MAGIC 👀 Find them in the left sidebar: **Catalog** → `samples` → `nyctaxi` → `trips` and **Catalog** → `workspace` →
# MAGIC `adventureworks` → `production_product`. The tabs **Columns** and **Sample Data** show you what is inside –
# MAGIC without writing any code.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 – The simplest query: `SELECT * FROM ...`
# MAGIC `*` means "all columns". **Always add a `LIMIT`** when you just want to have a look – tables can have millions of rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM samples.nyctaxi.trips
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 – Which columns are there?
# MAGIC `DESCRIBE TABLE` lists the columns with their data types (and comments, if any).

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE samples.nyctaxi.trips

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 – Choose columns, filter, sort
# MAGIC
# MAGIC | Clause | Meaning |
# MAGIC |---|---|
# MAGIC | `SELECT col1, col2` | only these columns (`AS` gives a column a new name) |
# MAGIC | `WHERE condition` | only rows that fulfil the condition |
# MAGIC | `ORDER BY col DESC` | sort (descending with `DESC`) |
# MAGIC | `LIMIT n` | only the first n rows |

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tpep_pickup_datetime        AS pickup_time,
# MAGIC        trip_distance * 1.609       AS distance_km,
# MAGIC        fare_amount
# MAGIC FROM samples.nyctaxi.trips
# MAGIC WHERE trip_distance > 10
# MAGIC ORDER BY fare_amount DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 – Aggregate: count, sum, average per group
# MAGIC `GROUP BY` combines all rows with the same value; aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) compute one
# MAGIC value per group.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Color,
# MAGIC        COUNT(*)                 AS products,
# MAGIC        ROUND(AVG(ListPrice), 2) AS avg_list_price
# MAGIC FROM workspace.adventureworks.production_product
# MAGIC WHERE Color IS NOT NULL
# MAGIC GROUP BY Color
# MAGIC ORDER BY products DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 – Shorter names with `USE`
# MAGIC With `USE CATALOG` and `USE SCHEMA` you set defaults – then the table name alone is enough.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG workspace;
# MAGIC USE SCHEMA adventureworks;
# MAGIC
# MAGIC SELECT Name, ProductNumber, ListPrice
# MAGIC FROM production_product          -- = workspace.adventureworks.production_product
# MAGIC WHERE ListPrice > 3000
# MAGIC ORDER BY ListPrice DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6 – The same in Python
# MAGIC In Python you read a table with `spark.table(...)` or run SQL with `spark.sql(...)`. Both return a
# MAGIC **DataFrame** that you can show with `display(...)` or process further.

# COMMAND ----------

from pyspark.sql import functions as F

df_trips = spark.table("samples.nyctaxi.trips")

display(
    df_trips
    .select("tpep_pickup_datetime", "trip_distance", "fare_amount")
    .filter(F.col("trip_distance") > 10)
    .orderBy(F.desc("fare_amount"))
    .limit(20)
)

# COMMAND ----------

df_colors = spark.sql("""
    SELECT Color, COUNT(*) AS products
    FROM workspace.adventureworks.production_product
    WHERE Color IS NOT NULL
    GROUP BY Color
""")
display(df_colors)

# COMMAND ----------

# MAGIC %md
# MAGIC ✏️ **Try it:**
# MAGIC 1. How many taxi trips are in `samples.nyctaxi.trips`? (`SELECT COUNT(*) FROM ...`)
# MAGIC 2. What is the average fare (`fare_amount`) of trips longer than 5 miles?
# MAGIC 3. Which are the 5 most expensive products (`ListPrice`) in `workspace.adventureworks.production_product`?
# MAGIC
# MAGIC 💡 There is also the **SQL Editor** (left sidebar): write SQL there without a notebook – handy for quick questions.
# MAGIC More SQL practice: exercise **e01** (`src/02_exercises`).

# COMMAND ----------

# your code here


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5 – Read Parquet with Spark (Python)
# MAGIC
# MAGIC Parquet is the standard file format in data lakes: columnar, compressed and it contains its own schema.

# COMMAND ----------

df_counts = spark.read.parquet(BIKE_PATH)

print(f"Rows: {df_counts.count():,}")
df_counts.printSchema()

# COMMAND ----------

display(df_counts.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC | Column | Meaning |
# MAGIC |---|---|
# MAGIC | `FK_STANDORT` | ID of the counting station |
# MAGIC | `DATUM` | start of the 15-minute interval (local time) |
# MAGIC | `VELO_IN` / `VELO_OUT` | number of bikes in direction "in" / "out" |
# MAGIC | `FUSS_IN` / `FUSS_OUT` | number of pedestrians in direction "in" / "out" |
# MAGIC | `OST` / `NORD` | coordinates (Swiss LV95) |
# MAGIC
# MAGIC Empty values (NULL) mean: this station does not count this mode.
# MAGIC
# MAGIC 💡 **Built-in visualizations:** click **+ → Visualization** above any table output and create a chart directly
# MAGIC in the notebook – no extra libraries needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6 – Filter and aggregate with PySpark

# COMMAND ----------

df_2025 = (
    df_counts
    .filter(F.col("DATUM").startswith("2025"))
    .groupBy("FK_STANDORT")
    .agg(
        F.count("*").alias("intervals"),
        F.sum(F.coalesce("VELO_IN", F.lit(0)) + F.coalesce("VELO_OUT", F.lit(0))).alias("bikes"),
    )
    .orderBy(F.desc("bikes"))
)

display(df_2025)

# COMMAND ----------

# MAGIC %md
# MAGIC ✏️ **Try it:** turn the result above into a **bar chart** (+ → Visualization → Bar).
# MAGIC Which station counts the most bikes? (Station names come in the project, step 3.)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7 – The same with SQL
# MAGIC
# MAGIC With `read_files()` you can query files directly in SQL. Note the `%sql` at the start of the cell.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT to_date(DATUM)                                     AS day,
# MAGIC        SUM(coalesce(VELO_IN, 0) + coalesce(VELO_OUT, 0))  AS bikes,
# MAGIC        SUM(coalesce(FUSS_IN, 0) + coalesce(FUSS_OUT, 0))  AS pedestrians
# MAGIC FROM read_files('/Volumes/workspace/mobility_raw/landing/mobility/bike_pedestrian_counts/', format => 'parquet')
# MAGIC WHERE DATUM >= '2025-01-01' AND DATUM < '2026-01-01'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ✏️ **Try it:** create a **line chart** from the result (x: `day`, y: `bikes`).
# MAGIC - What do you notice over the year?
# MAGIC - Can you see a weekly rhythm? Holidays? Outliers?

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8 – Combine Python and SQL
# MAGIC
# MAGIC A Spark DataFrame can be registered as a temporary view and then queried with SQL – and vice versa.

# COMMAND ----------

df_counts.createOrReplaceTempView("counts_raw")

df_hourly = spark.sql("""
    SELECT hour(to_timestamp(DATUM, "yyyy-MM-dd'T'HH:mm"))  AS hour,
           ROUND(AVG(coalesce(VELO_IN, 0) + coalesce(VELO_OUT, 0)), 2) AS avg_bikes_per_15min
    FROM counts_raw
    WHERE DATUM LIKE '2025%' AND VELO_IN IS NOT NULL
    GROUP BY 1
    ORDER BY 1
""")

display(df_hourly)

# COMMAND ----------

# MAGIC %md
# MAGIC ✏️ **Try it:** chart this as a line. Do you recognise the morning and evening rush hours?

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9 – Save a result as a Delta table
# MAGIC
# MAGIC `saveAsTable()` writes the data as a managed **Delta table** and registers it in Unity Catalog.
# MAGIC Afterwards you find it in the sidebar under **Catalog → workspace → exercises**.

# COMMAND ----------

df_2025.write.mode("overwrite").saveAsTable(f"{CATALOG}.exercises.bikes_per_station_2025")

display(spark.table(f"{CATALOG}.exercises.bikes_per_station_2025"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 10 – Small results? pandas works too
# MAGIC
# MAGIC Spark is built for large data. For small results you can switch to **pandas** at any time:

# COMMAND ----------

pdf = df_hourly.toPandas()
pdf.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Summary
# MAGIC
# MAGIC - Data lives in **Unity Catalog**: files in **volumes**, tables in **schemas** (`catalog.schema.table`)
# MAGIC - `spark.read...` reads files into a **DataFrame**, `display()` shows it
# MAGIC - With `%sql` and `read_files()` the same works in **SQL**
# MAGIC - `saveAsTable()` stores results as **Delta tables**
# MAGIC - Visualizations can be created from any output
# MAGIC
# MAGIC 👉 Continue with **02_delta_time_travel**, then the **exercises** (`src/02_exercises`) or the **project** (`src/03_project`).
