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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>LIMIT</code>, <code>.saveAsTable()</code>, <code>.printSchema()</code>, <code>.count()</code>, <code>.option()</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>df_countries_raw = (spark.read
# MAGIC     .format(&quot;xml&quot;)
# MAGIC     .option(&quot;rowTag&quot;, &quot;country&quot;)
# MAGIC     .load(XML_PATH))
# MAGIC 
# MAGIC df_countries_raw.printSchema()</code></pre><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>print(f&quot;Number of countries: {df_countries_raw.count()}&quot;)</code></pre><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>display(df_countries_raw.limit(5))</code></pre><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>df_countries_raw.write.mode(&quot;overwrite&quot;).saveAsTable(&quot;workspace.exercises.bronze_countries_raw&quot;)
# MAGIC print(&quot;Bronze table written: workspace.exercises.bronze_countries_raw&quot;)</code></pre><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT * FROM workspace.exercises.bronze_countries_raw LIMIT 5</code></pre></details>

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

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>GROUP BY</code>, <code>ORDER BY</code>, <code>LIMIT</code>, <code>COUNT</code>, <code>.select()</code>, <code>.saveAsTable()</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>from pyspark.sql.functions import col
# MAGIC 
# MAGIC df_countries_silver = spark.table(&quot;workspace.exercises.bronze_countries_raw&quot;).select(
# MAGIC     col(&quot;id&quot;).cast(&quot;int&quot;).alias(&quot;id&quot;),
# MAGIC     col(&quot;name&quot;),
# MAGIC     col(&quot;iso3&quot;),
# MAGIC     col(&quot;iso2&quot;),
# MAGIC     col(&quot;capital&quot;),
# MAGIC     col(&quot;region&quot;),
# MAGIC     col(&quot;subregion&quot;),
# MAGIC     col(&quot;latitude&quot;).cast(&quot;double&quot;).alias(&quot;latitude&quot;),
# MAGIC     col(&quot;longitude&quot;).cast(&quot;double&quot;).alias(&quot;longitude&quot;),
# MAGIC     col(&quot;currency&quot;),
# MAGIC     col(&quot;currency_name&quot;),
# MAGIC     col(&quot;phone_code&quot;),
# MAGIC     col(&quot;tld&quot;),
# MAGIC )
# MAGIC 
# MAGIC df_countries_silver.write.mode(&quot;overwrite&quot;).saveAsTable(&quot;workspace.exercises.silver_countries&quot;)
# MAGIC print(&quot;Silver table written: workspace.exercises.silver_countries&quot;)</code></pre><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT * FROM workspace.exercises.silver_countries LIMIT 10</code></pre><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>-- Quick check: countries per region
# MAGIC SELECT region, count(*) AS num_countries
# MAGIC FROM workspace.exercises.silver_countries
# MAGIC GROUP BY region
# MAGIC ORDER BY num_countries DESC</code></pre></details>

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
