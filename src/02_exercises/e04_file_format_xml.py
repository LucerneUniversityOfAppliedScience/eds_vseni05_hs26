# Databricks notebook source

# MAGIC %md
# MAGIC # XML to Medallion: Exercise
# MAGIC
# MAGIC In this exercise you will load an **XML** file into a Bronze table,
# MAGIC then flatten it into a Silver table.
# MAGIC
# MAGIC ## Learning Goals
# MAGIC - Read XML files with Spark
# MAGIC - Apply the Bronze/Silver medallion pattern

# COMMAND ----------

XML_PATH = "/Volumes/workspace/mobility_raw/landing/sample_data/xml/countries.xml"

print(f"XML file: {XML_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Read XML into Bronze
# MAGIC
# MAGIC Use `spark.read.format("xml")` to read the file. You need to specify
# MAGIC the `rowTag` option — this tells Spark which XML element represents one row.
# MAGIC
# MAGIC Look at the XML structure: `<countries>` → `<country>` → fields...
# MAGIC
# MAGIC 1. Read the XML file with the correct `rowTag`
# MAGIC 2. Inspect the schema with `.printSchema()`
# MAGIC 3. How many countries are in the file?
# MAGIC 4. Save as Bronze table `workspace.exercises.bronze_countries_raw`

# COMMAND ----------

# Hint: spark.read.format("xml").option("rowTag", "country").load(XML_PATH)
# YOUR CODE HERE
raise NotImplementedError("Step 1: read XML and write to Bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary><b>💡 Hint – useful building blocks</b></summary>
# MAGIC
# MAGIC You will need: `LIMIT`, `.saveAsTable()`, `.printSchema()`, `.count()`, `.option()`
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🔑 Solution</b></summary>
# MAGIC
# MAGIC Try it yourself first – then compare.
# MAGIC
# MAGIC ```python
# MAGIC df_countries_raw = (spark.read
# MAGIC     .format("xml")
# MAGIC     .option("rowTag", "country")
# MAGIC     .load(XML_PATH))
# MAGIC
# MAGIC df_countries_raw.printSchema()
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC print(f"Number of countries: {df_countries_raw.count()}")
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC display(df_countries_raw.limit(5))
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC df_countries_raw.write.mode("overwrite").saveAsTable("workspace.exercises.bronze_countries_raw")
# MAGIC print("Bronze table written: workspace.exercises.bronze_countries_raw")
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM workspace.exercises.bronze_countries_raw LIMIT 5
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.exercises.bronze_countries_raw LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Flatten to Silver
# MAGIC
# MAGIC The Bronze table has nested columns (`timezones`, `translations`).
# MAGIC Create a Silver table `workspace.exercises.silver_countries` with these flat columns:
# MAGIC
# MAGIC | Silver column | Source |
# MAGIC |---|---|
# MAGIC | `id` | `id` |
# MAGIC | `name` | `name` |
# MAGIC | `iso3` | `iso3` |
# MAGIC | `iso2` | `iso2` |
# MAGIC | `capital` | `capital` |
# MAGIC | `region` | `region` |
# MAGIC | `subregion` | `subregion` |
# MAGIC | `latitude` | `latitude` (cast to double) |
# MAGIC | `longitude` | `longitude` (cast to double) |
# MAGIC | `currency` | `currency` |
# MAGIC | `currency_name` | `currency_name` |
# MAGIC | `phone_code` | `phone_code` |
# MAGIC | `tld` | `tld` |

# COMMAND ----------

# Hint: read the bronze table, then df.select(col("id"), col("name"), ...,
#       try_cast(col("latitude"), "double").alias("latitude"), ...)
# YOUR CODE HERE
raise NotImplementedError("Step 2: flatten to Silver")

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary><b>💡 Hint – useful building blocks</b></summary>
# MAGIC
# MAGIC You will need: `GROUP BY`, `ORDER BY`, `LIMIT`, `COUNT`, `.select()`, `.saveAsTable()`
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🔑 Solution</b></summary>
# MAGIC
# MAGIC Try it yourself first – then compare.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC df_countries_silver = spark.table("workspace.exercises.bronze_countries_raw").select(
# MAGIC     col("id").cast("int").alias("id"),
# MAGIC     col("name"),
# MAGIC     col("iso3"),
# MAGIC     col("iso2"),
# MAGIC     col("capital"),
# MAGIC     col("region"),
# MAGIC     col("subregion"),
# MAGIC     col("latitude").cast("double").alias("latitude"),
# MAGIC     col("longitude").cast("double").alias("longitude"),
# MAGIC     col("currency"),
# MAGIC     col("currency_name"),
# MAGIC     col("phone_code"),
# MAGIC     col("tld"),
# MAGIC )
# MAGIC
# MAGIC df_countries_silver.write.mode("overwrite").saveAsTable("workspace.exercises.silver_countries")
# MAGIC print("Silver table written: workspace.exercises.silver_countries")
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM workspace.exercises.silver_countries LIMIT 10
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC -- Quick check: countries per region
# MAGIC SELECT region, count(*) AS num_countries
# MAGIC FROM workspace.exercises.silver_countries
# MAGIC GROUP BY region
# MAGIC ORDER BY num_countries DESC
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.exercises.silver_countries LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS workspace.exercises.bronze_countries_raw;
# MAGIC -- DROP TABLE IF EXISTS workspace.exercises.silver_countries;
