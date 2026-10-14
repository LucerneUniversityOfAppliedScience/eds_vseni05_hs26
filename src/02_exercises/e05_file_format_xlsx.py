# Databricks notebook source

# MAGIC %md
# MAGIC # XLSX to Medallion: Exercise
# MAGIC
# MAGIC In this exercise you will load an **Excel file** with 2 sheets into Bronze tables,
# MAGIC then clean them up into Silver tables.
# MAGIC
# MAGIC Databricks Serverless does not have a native Excel reader — we read the file with
# MAGIC **pandas** and then convert it to a **PySpark** DataFrame.
# MAGIC
# MAGIC **Naming convention used below:**
# MAGIC - `pdf` = pandas DataFrame
# MAGIC - `df`  = PySpark DataFrame
# MAGIC
# MAGIC ## Learning Goals
# MAGIC - Read Excel sheets with pandas and bridge to Spark
# MAGIC - Apply the Bronze/Silver medallion pattern

# COMMAND ----------

# MAGIC %pip install openpyxl
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd

XLSX_PATH = "/Volumes/workspace/mobility_raw/landing/sample_data/xlsx/FinancialsSampleData.xlsx"
print(f"XLSX file: {XLSX_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Read both sheets into Bronze
# MAGIC
# MAGIC The file `FinancialsSampleData.xlsx` has **2 sheets**:
# MAGIC - **Financials1**: Monthly budget data (Account, Business Unit, Year, Jan–Dec)
# MAGIC - **Financials2**: Sales transactions (Segment, Country, Product, Sales, Profit, ...)
# MAGIC
# MAGIC For each sheet:
# MAGIC 1. `pdf = pd.read_excel(XLSX_PATH, sheet_name="Financials1")`
# MAGIC 2. `df = spark.createDataFrame(pdf)`
# MAGIC 3. Write `df` as a Bronze Delta table.
# MAGIC
# MAGIC Target tables:
# MAGIC - `workspace.exercises.bronze_financials_budget`
# MAGIC - `workspace.exercises.bronze_financials_sales`

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 1: read XLSX sheets with pandas, create Spark DataFrames, write to Bronze")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>LIMIT</code>, <code>.saveAsTable()</code>, <code>.read_excel()</code>, <code>.createDataFrame()</code>, <code>.printSchema()</code>, <code>.count()</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>pdf_budget = pd.read_excel(XLSX_PATH, sheet_name=&quot;Financials1&quot;)
# MAGIC df_budget = spark.createDataFrame(pdf_budget)
# MAGIC 
# MAGIC print(f&quot;Financials1: {df_budget.count()} rows&quot;)
# MAGIC df_budget.printSchema()</code></pre><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>pdf_sales = pd.read_excel(XLSX_PATH, sheet_name=&quot;Financials2&quot;)
# MAGIC df_sales = spark.createDataFrame(pdf_sales)
# MAGIC 
# MAGIC print(f&quot;Financials2: {df_sales.count()} rows&quot;)
# MAGIC df_sales.printSchema()</code></pre><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>def clean_cols(df):
# MAGIC     return df.toDF(*[re.sub(r&quot;[ ,;{}()\n\t=]&quot;, &quot;_&quot;, c) for c in df.columns])
# MAGIC 
# MAGIC df_budget = clean_cols(df_budget)
# MAGIC df_sales = clean_cols(df_sales)
# MAGIC 
# MAGIC df_budget.write.mode(&quot;overwrite&quot;).saveAsTable(&quot;workspace.exercises.bronze_financials_budget&quot;)
# MAGIC df_sales.write.mode(&quot;overwrite&quot;).saveAsTable(&quot;workspace.exercises.bronze_financials_sales&quot;)
# MAGIC print(&quot;Bronze tables written&quot;)</code></pre><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT * FROM workspace.exercises.bronze_financials_budget LIMIT 5</code></pre><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT * FROM workspace.exercises.bronze_financials_sales LIMIT 5</code></pre></details>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.exercises.bronze_financials_budget LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.exercises.bronze_financials_sales LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Clean up to Silver
# MAGIC
# MAGIC Inspect the Bronze tables:
# MAGIC 1. Check the data types with `DESCRIBE`
# MAGIC 2. Are there any issues that need fixing?
# MAGIC
# MAGIC **Hints to look for:**
# MAGIC - Are numeric columns actually numeric or stored as strings?
# MAGIC - Are there column names with typos or extra spaces?
# MAGIC
# MAGIC **Hint on casting:** Serverless Spark has ANSI mode on. For numeric casts use
# MAGIC `try_cast(col(...), "double")` instead of `.cast("double")` so that malformed or
# MAGIC empty values become `NULL` instead of raising an error.
# MAGIC
# MAGIC Create Silver tables:
# MAGIC - `workspace.exercises.silver_financials_budget`
# MAGIC - `workspace.exercises.silver_financials_sales`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE workspace.exercises.bronze_financials_budget

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE workspace.exercises.bronze_financials_sales

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 2: clean up to Silver")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>WITH</code>, <code>GROUP BY</code>, <code>ORDER BY</code>, <code>LIMIT</code>, <code>TRY_CAST</code>, <code>CAST</code>, <code>YEAR</code>, <code>MONTH</code>, <code>TRIM</code>, <code>ROUND</code>, <code>SUM</code>, <code>DESCRIBE</code>, <code>.withColumn()</code>, <code>.withColumnRenamed()</code></p></details>
# MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>DESCRIBE workspace.exercises.bronze_financials_budget</code></pre><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>DESCRIBE workspace.exercises.bronze_financials_sales</code></pre><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>from pyspark.sql.functions import expr
# MAGIC 
# MAGIC # Silver: Budget table — fix column name typo, ensure numeric types for months
# MAGIC df_budget_silver = (spark.table(&quot;workspace.exercises.bronze_financials_budget&quot;)
# MAGIC     .withColumnRenamed(&quot;Businees_Unit&quot;, &quot;business_unit&quot;)
# MAGIC     .withColumnRenamed(&quot;Account&quot;, &quot;account&quot;)
# MAGIC     .withColumnRenamed(&quot;Currency&quot;, &quot;currency&quot;)
# MAGIC     .withColumnRenamed(&quot;Year&quot;, &quot;year&quot;)
# MAGIC     .withColumnRenamed(&quot;Scenario&quot;, &quot;scenario&quot;)
# MAGIC )
# MAGIC 
# MAGIC # Rename month columns to lowercase first, then cast — avoid withColumn+drop with
# MAGIC # case-only differences (Spark resolves names case-insensitively by default and
# MAGIC # would treat Jan and jan as the same column).
# MAGIC for month in [&quot;Jan&quot;, &quot;Feb&quot;, &quot;Mar&quot;, &quot;Apr&quot;, &quot;May&quot;, &quot;Jun&quot;, &quot;Jul&quot;, &quot;Aug&quot;, &quot;Sep&quot;, &quot;Oct&quot;, &quot;Nov&quot;, &quot;Dec&quot;]:
# MAGIC     df_budget_silver = df_budget_silver.withColumnRenamed(month, month.lower())
# MAGIC 
# MAGIC for month in [&quot;jan&quot;, &quot;feb&quot;, &quot;mar&quot;, &quot;apr&quot;, &quot;may&quot;, &quot;jun&quot;, &quot;jul&quot;, &quot;aug&quot;, &quot;sep&quot;, &quot;oct&quot;, &quot;nov&quot;, &quot;dec&quot;]:
# MAGIC     df_budget_silver = df_budget_silver.withColumn(month, expr(f&quot;try_cast({month} as double)&quot;))
# MAGIC 
# MAGIC df_budget_silver.write.mode(&quot;overwrite&quot;).saveAsTable(&quot;workspace.exercises.silver_financials_budget&quot;)
# MAGIC print(&quot;Silver table written: workspace.exercises.silver_financials_budget&quot;)</code></pre><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT * FROM workspace.exercises.silver_financials_budget LIMIT 10</code></pre><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code># Silver: Sales table — trim column names and cast numeric types
# MAGIC df_sales_silver = (spark.table(&quot;workspace.exercises.bronze_financials_sales&quot;)
# MAGIC     .withColumnRenamed(&quot;Segment&quot;, &quot;segment&quot;)
# MAGIC     .withColumnRenamed(&quot;Country&quot;, &quot;country&quot;)
# MAGIC     .withColumnRenamed(&quot;Product&quot;, &quot;product&quot;)
# MAGIC     .withColumnRenamed(&quot;Discount_Band&quot;, &quot;discount_band&quot;)
# MAGIC     .withColumnRenamed(&quot;Units_Sold&quot;, &quot;units_sold&quot;)
# MAGIC     .withColumnRenamed(&quot;Manufacturing_Price&quot;, &quot;manufacturing_price&quot;)
# MAGIC     .withColumnRenamed(&quot;Sale_Price&quot;, &quot;sale_price&quot;)
# MAGIC     .withColumnRenamed(&quot;Gross_Sales&quot;, &quot;gross_sales&quot;)
# MAGIC     .withColumnRenamed(&quot;Discounts&quot;, &quot;discounts&quot;)
# MAGIC     .withColumnRenamed(&quot;_Sales&quot;, &quot;sales&quot;)
# MAGIC     .withColumnRenamed(&quot;COGS&quot;, &quot;cogs&quot;)
# MAGIC     .withColumnRenamed(&quot;Profit&quot;, &quot;profit&quot;)
# MAGIC     .withColumnRenamed(&quot;Date&quot;, &quot;date&quot;)
# MAGIC     .withColumnRenamed(&quot;Month_Number&quot;, &quot;month_number&quot;)
# MAGIC     .withColumnRenamed(&quot;Month_Name&quot;, &quot;month_name&quot;)
# MAGIC     .withColumnRenamed(&quot;Year&quot;, &quot;year&quot;)
# MAGIC     .withColumn(&quot;units_sold&quot;, expr(&quot;try_cast(units_sold as double)&quot;))
# MAGIC     .withColumn(&quot;manufacturing_price&quot;, expr(&quot;try_cast(manufacturing_price as double)&quot;))
# MAGIC     .withColumn(&quot;sale_price&quot;, expr(&quot;try_cast(sale_price as double)&quot;))
# MAGIC     .withColumn(&quot;gross_sales&quot;, expr(&quot;try_cast(gross_sales as double)&quot;))
# MAGIC     .withColumn(&quot;discounts&quot;, expr(&quot;try_cast(discounts as double)&quot;))
# MAGIC     .withColumn(&quot;sales&quot;, expr(&quot;try_cast(sales as double)&quot;))
# MAGIC     .withColumn(&quot;cogs&quot;, expr(&quot;try_cast(cogs as double)&quot;))
# MAGIC     .withColumn(&quot;profit&quot;, expr(&quot;try_cast(profit as double)&quot;))
# MAGIC     .withColumn(&quot;month_number&quot;, expr(&quot;try_cast(month_number as int)&quot;))
# MAGIC     .withColumn(&quot;year&quot;, expr(&quot;try_cast(year as int)&quot;))
# MAGIC )
# MAGIC 
# MAGIC df_sales_silver.write.mode(&quot;overwrite&quot;).saveAsTable(&quot;workspace.exercises.silver_financials_sales&quot;)
# MAGIC print(&quot;Silver table written: workspace.exercises.silver_financials_sales&quot;)</code></pre><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT * FROM workspace.exercises.silver_financials_sales LIMIT 10</code></pre><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>-- Quick check: profit by segment
# MAGIC SELECT segment, round(sum(profit), 2) AS total_profit
# MAGIC FROM workspace.exercises.silver_financials_sales
# MAGIC GROUP BY segment
# MAGIC ORDER BY total_profit DESC</code></pre></details>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.exercises.silver_financials_budget LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.exercises.silver_financials_sales LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS workspace.exercises.bronze_financials_budget;
# MAGIC -- DROP TABLE IF EXISTS workspace.exercises.bronze_financials_sales;
# MAGIC -- DROP TABLE IF EXISTS workspace.exercises.silver_financials_budget;
# MAGIC -- DROP TABLE IF EXISTS workspace.exercises.silver_financials_sales;
