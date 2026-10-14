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

# MAGIC %md
# MAGIC <details>
# MAGIC <summary><b>💡 Hint – useful building blocks</b></summary>
# MAGIC
# MAGIC You will need: `LIMIT`, `.saveAsTable()`, `.read_excel()`, `.createDataFrame()`, `.printSchema()`, `.count()`
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🔑 Solution</b></summary>
# MAGIC
# MAGIC Try it yourself first – then compare.
# MAGIC
# MAGIC ```python
# MAGIC pdf_budget = pd.read_excel(XLSX_PATH, sheet_name="Financials1")
# MAGIC df_budget = spark.createDataFrame(pdf_budget)
# MAGIC
# MAGIC print(f"Financials1: {df_budget.count()} rows")
# MAGIC df_budget.printSchema()
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC pdf_sales = pd.read_excel(XLSX_PATH, sheet_name="Financials2")
# MAGIC df_sales = spark.createDataFrame(pdf_sales)
# MAGIC
# MAGIC print(f"Financials2: {df_sales.count()} rows")
# MAGIC df_sales.printSchema()
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC def clean_cols(df):
# MAGIC     return df.toDF(*[re.sub(r"[ ,;{}()\n\t=]", "_", c) for c in df.columns])
# MAGIC
# MAGIC df_budget = clean_cols(df_budget)
# MAGIC df_sales = clean_cols(df_sales)
# MAGIC
# MAGIC df_budget.write.mode("overwrite").saveAsTable("workspace.exercises.bronze_financials_budget")
# MAGIC df_sales.write.mode("overwrite").saveAsTable("workspace.exercises.bronze_financials_sales")
# MAGIC print("Bronze tables written")
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM workspace.exercises.bronze_financials_budget LIMIT 5
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM workspace.exercises.bronze_financials_sales LIMIT 5
# MAGIC ```
# MAGIC
# MAGIC </details>

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

# MAGIC %md
# MAGIC <details>
# MAGIC <summary><b>💡 Hint – useful building blocks</b></summary>
# MAGIC
# MAGIC You will need: `WITH`, `GROUP BY`, `ORDER BY`, `LIMIT`, `TRY_CAST`, `CAST`, `YEAR`, `MONTH`, `TRIM`, `ROUND`, `SUM`, `DESCRIBE`, `.withColumn()`, `.withColumnRenamed()`
# MAGIC
# MAGIC </details>
# MAGIC <details>
# MAGIC <summary><b>🔑 Solution</b></summary>
# MAGIC
# MAGIC Try it yourself first – then compare.
# MAGIC
# MAGIC ```sql
# MAGIC DESCRIBE workspace.exercises.bronze_financials_budget
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC DESCRIBE workspace.exercises.bronze_financials_sales
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import expr
# MAGIC
# MAGIC # Silver: Budget table — fix column name typo, ensure numeric types for months
# MAGIC df_budget_silver = (spark.table("workspace.exercises.bronze_financials_budget")
# MAGIC     .withColumnRenamed("Businees_Unit", "business_unit")
# MAGIC     .withColumnRenamed("Account", "account")
# MAGIC     .withColumnRenamed("Currency", "currency")
# MAGIC     .withColumnRenamed("Year", "year")
# MAGIC     .withColumnRenamed("Scenario", "scenario")
# MAGIC )
# MAGIC
# MAGIC # Rename month columns to lowercase first, then cast — avoid withColumn+drop with
# MAGIC # case-only differences (Spark resolves names case-insensitively by default and
# MAGIC # would treat Jan and jan as the same column).
# MAGIC for month in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]:
# MAGIC     df_budget_silver = df_budget_silver.withColumnRenamed(month, month.lower())
# MAGIC
# MAGIC for month in ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]:
# MAGIC     df_budget_silver = df_budget_silver.withColumn(month, expr(f"try_cast({month} as double)"))
# MAGIC
# MAGIC df_budget_silver.write.mode("overwrite").saveAsTable("workspace.exercises.silver_financials_budget")
# MAGIC print("Silver table written: workspace.exercises.silver_financials_budget")
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM workspace.exercises.silver_financials_budget LIMIT 10
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC # Silver: Sales table — trim column names and cast numeric types
# MAGIC df_sales_silver = (spark.table("workspace.exercises.bronze_financials_sales")
# MAGIC     .withColumnRenamed("Segment", "segment")
# MAGIC     .withColumnRenamed("Country", "country")
# MAGIC     .withColumnRenamed("Product", "product")
# MAGIC     .withColumnRenamed("Discount_Band", "discount_band")
# MAGIC     .withColumnRenamed("Units_Sold", "units_sold")
# MAGIC     .withColumnRenamed("Manufacturing_Price", "manufacturing_price")
# MAGIC     .withColumnRenamed("Sale_Price", "sale_price")
# MAGIC     .withColumnRenamed("Gross_Sales", "gross_sales")
# MAGIC     .withColumnRenamed("Discounts", "discounts")
# MAGIC     .withColumnRenamed("_Sales", "sales")
# MAGIC     .withColumnRenamed("COGS", "cogs")
# MAGIC     .withColumnRenamed("Profit", "profit")
# MAGIC     .withColumnRenamed("Date", "date")
# MAGIC     .withColumnRenamed("Month_Number", "month_number")
# MAGIC     .withColumnRenamed("Month_Name", "month_name")
# MAGIC     .withColumnRenamed("Year", "year")
# MAGIC     .withColumn("units_sold", expr("try_cast(units_sold as double)"))
# MAGIC     .withColumn("manufacturing_price", expr("try_cast(manufacturing_price as double)"))
# MAGIC     .withColumn("sale_price", expr("try_cast(sale_price as double)"))
# MAGIC     .withColumn("gross_sales", expr("try_cast(gross_sales as double)"))
# MAGIC     .withColumn("discounts", expr("try_cast(discounts as double)"))
# MAGIC     .withColumn("sales", expr("try_cast(sales as double)"))
# MAGIC     .withColumn("cogs", expr("try_cast(cogs as double)"))
# MAGIC     .withColumn("profit", expr("try_cast(profit as double)"))
# MAGIC     .withColumn("month_number", expr("try_cast(month_number as int)"))
# MAGIC     .withColumn("year", expr("try_cast(year as int)"))
# MAGIC )
# MAGIC
# MAGIC df_sales_silver.write.mode("overwrite").saveAsTable("workspace.exercises.silver_financials_sales")
# MAGIC print("Silver table written: workspace.exercises.silver_financials_sales")
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM workspace.exercises.silver_financials_sales LIMIT 10
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC -- Quick check: profit by segment
# MAGIC SELECT segment, round(sum(profit), 2) AS total_profit
# MAGIC FROM workspace.exercises.silver_financials_sales
# MAGIC GROUP BY segment
# MAGIC ORDER BY total_profit DESC
# MAGIC ```
# MAGIC
# MAGIC </details>

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
