-- Databricks notebook source
-- MAGIC %md
-- MAGIC # E01 – SQL Basics with AdventureWorks
-- MAGIC
-- MAGIC **Goal:** Practice SQL basics directly in Databricks – from `SELECT` to CTEs.
-- MAGIC
-- MAGIC **Data:** AdventureWorks (a fictional bicycle manufacturer by Microsoft). The tables were loaded by the setup job
-- MAGIC into the schema `workspace.adventureworks`, e.g. `adventureworks.production_product`,
-- MAGIC `adventureworks.sales_salesorderheader`, `adventureworks.person_person`.
-- MAGIC
-- MAGIC **How to work:**
-- MAGIC - The notebook is written in **SQL**. Attach it to **Serverless** (or the *Serverless Starter Warehouse*).
-- MAGIC - Below each exercise there is an empty cell `-- your solution` – write your query there and run it.
-- MAGIC - The difficulty is shown in the title: *Easy*, *Medium*, *Hard*.
-- MAGIC - Below each exercise you find a **hint** and the **solution** (click to expand) – try it yourself first!
-- MAGIC
-- MAGIC > Source: SQL introduction from CAS DEADS7, adapted for Databricks Free Edition.

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Topic 1: Introduction to SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 1.1 (Easy)
-- MAGIC **Business context:** The product management team needs a complete overview of all available product information in the catalog.
-- MAGIC
-- MAGIC Show all columns from the table `adventureworks.production_product`.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT *
-- MAGIC FROM adventureworks.production_product;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 1.2 (Easy)
-- MAGIC **Business context:** For a quick price overview, the sales team needs a compact list of the most important product information.
-- MAGIC
-- MAGIC Show the first 10 products from the table `adventureworks.production_product` with the columns `ProductID`, `Name` and `ListPrice`.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>LIMIT</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT ProductID, Name, ListPrice
-- MAGIC FROM adventureworks.production_product
-- MAGIC LIMIT 10;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 1.3 (Medium)
-- MAGIC **Business context:** As a new data analyst, you need to understand the structure of the order data in order to carry out future analyses.
-- MAGIC
-- MAGIC Explore the structure of the `adventureworks.sales_salesorderheader` table. Show the first 5 records and use `DESCRIBE` or `SHOW COLUMNS` to display the columns.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>LIMIT</code>, <code>DESCRIBE</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>-- Show column structure
-- MAGIC DESCRIBE adventureworks.sales_salesorderheader;</code></pre><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>-- First 5 records
-- MAGIC SELECT *
-- MAGIC FROM adventureworks.sales_salesorderheader
-- MAGIC LIMIT 5;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Topic 2: SELECT Statement

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 2.1 (Easy)
-- MAGIC **Business context:** The HR department needs a list of names of all persons to create employee badges.
-- MAGIC
-- MAGIC Show the columns `FirstName`, `MiddleName` and `LastName` from the table `adventureworks.person_person`.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT FirstName, MiddleName, LastName
-- MAGIC FROM adventureworks.person_person;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 2.2 (Easy)
-- MAGIC Write a query on `adventureworks.production_product` that shows the column `ProductID` as "Product ID" and `Name` as "ProductName" (use aliases).

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     ProductID AS `Product ID`,
-- MAGIC     Name AS ProductName
-- MAGIC FROM adventureworks.production_product;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 2.4 (Medium)
-- MAGIC Create a concatenated column "FullName" from `FirstName`, `MiddleName` and `LastName` of the table `adventureworks.person_person`. Handle `NULL` values in `MiddleName` appropriately.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>COALESCE</code>, <code>CONCAT</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     BusinessEntityID,
-- MAGIC     CONCAT(
-- MAGIC         FirstName,
-- MAGIC         &#x27; &#x27;,
-- MAGIC         COALESCE(MiddleName || &#x27; &#x27;, &#x27;&#x27;),
-- MAGIC         LastName
-- MAGIC     ) AS FullName
-- MAGIC FROM adventureworks.person_person;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 2.5 (Hard)
-- MAGIC **Business context:** The finance team needs a detailed analysis of the discount structure to assess the impact of price reductions on revenue.
-- MAGIC
-- MAGIC From `adventureworks.sales_salesorderdetail`, show the columns `SalesOrderID`, `ProductID`, `OrderQty`, `UnitPrice` and calculate:
-- MAGIC - The total price without discount: `OrderQty * UnitPrice` as "Total_Gross"
-- MAGIC
-- MAGIC - The total price with discount: `OrderQty * UnitPrice * (1 - UnitPriceDiscount)` as "Total_Net"
-- MAGIC - The savings from the discount: `OrderQty * UnitPrice * UnitPriceDiscount` as "Discount_Savings"

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     SalesOrderID,
-- MAGIC     ProductID,
-- MAGIC     OrderQty,
-- MAGIC     UnitPrice,
-- MAGIC     OrderQty * UnitPrice AS Total_Gross,
-- MAGIC     OrderQty * UnitPrice * (1 - UnitPriceDiscount) AS Total_Net,
-- MAGIC     OrderQty * UnitPrice * UnitPriceDiscount AS Discount_Savings
-- MAGIC FROM adventureworks.sales_salesorderdetail;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Topic 3: Data Types and Conversion

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 3.1 (Easy)
-- MAGIC **Business context:** The finance department needs prices without decimal places for simplified budget calculations.
-- MAGIC
-- MAGIC Convert the column `ListPrice` from `adventureworks.production_product` to an integer value using `CAST()` or `CONVERT()`.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>CAST</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     ProductID,
-- MAGIC     Name,
-- MAGIC     ListPrice,
-- MAGIC     CAST(ListPrice AS INT) AS ListPrice_Int
-- MAGIC FROM adventureworks.production_product;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 3.2 (Easy)
-- MAGIC **Business context:** For the export to Excel, the reporting team needs order data in the European date format.
-- MAGIC
-- MAGIC From `adventureworks.sales_salesorderheader`, show the column `OrderDate` as a formatted date in the format "DD.MM.YYYY" using `DATE_FORMAT()`.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>DATE_FORMAT</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     SalesOrderID,
-- MAGIC     OrderDate,
-- MAGIC     DATE_FORMAT(OrderDate, &#x27;dd.MM.yyyy&#x27;) AS OrderDate_Formatted
-- MAGIC FROM adventureworks.sales_salesorderheader;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 3.3 (Medium)
-- MAGIC **Business context:** The finance department needs revenue figures in a readable form with thousands separators for presentations.
-- MAGIC
-- MAGIC Format the column `TotalDue` from `adventureworks.sales_salesorderheader` with two decimal places and thousands separators using `FORMAT_NUMBER()`.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>FORMAT_NUMBER</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     SalesOrderID,
-- MAGIC     TotalDue,
-- MAGIC     FORMAT_NUMBER(TotalDue, 2) AS TotalDue_Formatted
-- MAGIC FROM adventureworks.sales_salesorderheader;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 3.4 (Hard)
-- MAGIC **Business context:** Product management wants to analyze the age of all products in the range in order to make lifecycle decisions.
-- MAGIC
-- MAGIC From the table `adventureworks.production_product`: convert the column `SellStartDate` (string) to a date using `TO_DATE()` and calculate the number of days from the sell start until today using `DATEDIFF()`.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>TO_DATE</code>, <code>DATEDIFF</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     ProductID,
-- MAGIC     Name,
-- MAGIC     SellStartDate,
-- MAGIC     TO_DATE(SellStartDate) AS SellStartDate_Date,
-- MAGIC     DATEDIFF(CURRENT_DATE(), TO_DATE(SellStartDate)) AS Days_Since_Start
-- MAGIC FROM adventureworks.production_product;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Topic 4: Handling NULL Values

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 4.1 (Easy)
-- MAGIC **Business context:** The catalog team wants to identify all products that have not yet been assigned to a subcategory and need to be reviewed.
-- MAGIC
-- MAGIC Show all products from `adventureworks.production_product` where `ProductSubcategoryID` is NULL.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>IS NULL</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT ProductID, Name, ProductSubcategoryID
-- MAGIC FROM adventureworks.production_product
-- MAGIC WHERE ProductSubcategoryID IS NULL;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 4.2 (Easy)
-- MAGIC **Business context:** For customer communication, all names should be displayed in full, even if there is no middle name.
-- MAGIC
-- MAGIC In the table `adventureworks.person_person`, replace all NULL values in the column `MiddleName` with 'N/A' using `IFNULL()` or `COALESCE()`.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>COALESCE</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     BusinessEntityID,
-- MAGIC     FirstName,
-- MAGIC     COALESCE(MiddleName, &#x27;N/A&#x27;) AS MiddleName,
-- MAGIC     LastName
-- MAGIC FROM adventureworks.person_person;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 4.3 (Medium)
-- MAGIC **Business context:** The e-commerce shop always wants to display at least one product attribute, even if not all data is available.
-- MAGIC
-- MAGIC Use `COALESCE()` to show the first non-NULL column of `Color`, `Size` and `Weight` from `adventureworks.production_product`. If all are NULL, show 'Not specified'.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>COALESCE</code>, <code>CAST</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     ProductID,
-- MAGIC     Name,
-- MAGIC     COALESCE(Color, Size, CAST(Weight AS STRING), &#x27;Not specified&#x27;) AS First_Attribute
-- MAGIC FROM adventureworks.production_product;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 4.5 (Hard)
-- MAGIC **Business context:** The finance team wants to identify products with a real profit margin and filter out cost-price products (no markup).
-- MAGIC
-- MAGIC Use `NULLIF()` to compare the column `ListPrice` with `StandardCost` in `adventureworks.production_product`. If both are identical, return NULL, otherwise the difference `ListPrice - StandardCost` as "ProfitMargin".

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>CASE WHEN</code>, <code>NULLIF</code>, <code>IS NULL</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     ProductID,
-- MAGIC     Name,
-- MAGIC     ListPrice,
-- MAGIC     StandardCost,
-- MAGIC     CASE
-- MAGIC         WHEN NULLIF(ListPrice, StandardCost) IS NULL THEN NULL
-- MAGIC         ELSE ListPrice - StandardCost
-- MAGIC     END AS ProfitMargin
-- MAGIC FROM adventureworks.production_product;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Topic 5: Sorting and Filtering

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 5.1 (Easy)
-- MAGIC **Business context:** For the printed product catalog, the marketing team needs an alphabetically sorted product list.
-- MAGIC
-- MAGIC Show all products from `adventureworks.production_product`, sorted by `Name` in ascending order.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>ORDER BY</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT ProductID, Name, ListPrice
-- MAGIC FROM adventureworks.production_product
-- MAGIC ORDER BY Name ASC;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 5.3 (Medium)
-- MAGIC **Business context:** For sorting on the website, products with a color should appear first and colorless products at the end of the list.
-- MAGIC
-- MAGIC Sort `adventureworks.production_product` by `Color` in ascending order. NULL values should appear last (use `NULLS LAST`).

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>ORDER BY</code>, <code>NULLS LAST</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT ProductID, Name, Color
-- MAGIC FROM adventureworks.production_product
-- MAGIC ORDER BY Color ASC NULLS LAST;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 5.4 (Medium)
-- MAGIC **Business context:** Sales management needs a ranking of the largest orders, sorted chronologically when values are equal.
-- MAGIC
-- MAGIC From `adventureworks.sales_salesorderheader`, show `SalesOrderID`, `OrderDate` and `TotalDue`, sorted by `TotalDue` in descending order and, for equal values, by `OrderDate` in ascending order.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>ORDER BY</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT SalesOrderID, OrderDate, TotalDue
-- MAGIC FROM adventureworks.sales_salesorderheader
-- MAGIC ORDER BY TotalDue DESC, OrderDate ASC;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 5.5 (Hard)
-- MAGIC **Business context:** The printed product catalog should show uncategorized products first (for review), then be sorted by category, price and name.
-- MAGIC
-- MAGIC Sort `adventureworks.production_product` primarily by `ProductSubcategoryID` (NULL values first using `NULLS FIRST`), secondarily by `ListPrice` in descending order and thirdly by `Name` in ascending order.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>ORDER BY</code>, <code>NULLS FIRST</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT ProductID, Name, ProductSubcategoryID, ListPrice
-- MAGIC FROM adventureworks.production_product
-- MAGIC ORDER BY ProductSubcategoryID ASC NULLS FIRST,
-- MAGIC          ListPrice DESC,
-- MAGIC          Name ASC;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Topic 6: Handling Duplicates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 6.1 (Easy)
-- MAGIC **Business context:** The e-commerce team wants to determine all available product colors for the filter options on the website.
-- MAGIC
-- MAGIC Show all distinct values of the column `Color` from `adventureworks.production_product` using `DISTINCT`.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>ORDER BY</code>, <code>DISTINCT</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT DISTINCT Color
-- MAGIC FROM adventureworks.production_product
-- MAGIC ORDER BY Color;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 6.2 (Medium)
-- MAGIC **Business context:** Quality assurance suspects duplicates in the product data and wants to check color/size combinations that were created more than once.
-- MAGIC
-- MAGIC Find all combinations of `Color` and `Size` from `adventureworks.production_product` that occur more than once. Show only the duplicates with their count.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>GROUP BY</code>, <code>HAVING</code>, <code>ORDER BY</code>, <code>COUNT</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT Color, Size, COUNT(*) AS DuplicateCount
-- MAGIC FROM adventureworks.production_product
-- MAGIC GROUP BY Color, Size
-- MAGIC HAVING COUNT(*) &gt; 1
-- MAGIC ORDER BY DuplicateCount DESC;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 6.4 (Hard)
-- MAGIC **Business context:** Product management wants to identify the premium product (highest price) in each product category for a luxury campaign.
-- MAGIC
-- MAGIC Use `ROW_NUMBER()` over `PARTITION BY` to identify only the most expensive product (highest `ListPrice`) for each `ProductSubcategoryID` from `adventureworks.production_product`. Show `ProductSubcategoryID`, `ProductID`, `Name` and `ListPrice`.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>WITH</code>, <code>ORDER BY</code>, <code>IS NOT NULL</code>, <code>ROW_NUMBER</code>, <code>OVER</code>, <code>PARTITION BY</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>WITH RankedProducts AS (
-- MAGIC     SELECT
-- MAGIC         ProductSubcategoryID,
-- MAGIC         ProductID,
-- MAGIC         Name,
-- MAGIC         ListPrice,
-- MAGIC         ROW_NUMBER() OVER (PARTITION BY ProductSubcategoryID ORDER BY ListPrice DESC) AS rn
-- MAGIC     FROM adventureworks.production_product
-- MAGIC     WHERE ProductSubcategoryID IS NOT NULL
-- MAGIC )
-- MAGIC SELECT ProductSubcategoryID, ProductID, Name, ListPrice
-- MAGIC FROM RankedProducts
-- MAGIC WHERE rn = 1
-- MAGIC ORDER BY ProductSubcategoryID;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Topic 7: WHERE Clause and Filtering

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 7.1 (Easy)
-- MAGIC **Business context:** The premium segment team wants to identify all high-priced products for an exclusive marketing campaign.
-- MAGIC
-- MAGIC Show all products from `adventureworks.production_product` where `ListPrice` is greater than 500.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>ORDER BY</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT ProductID, Name, ListPrice
-- MAGIC FROM adventureworks.production_product
-- MAGIC WHERE ListPrice &gt; 500
-- MAGIC ORDER BY ListPrice DESC;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 7.2 (Easy)
-- MAGIC **Business context:** Customer service wants to create a list of names for a personalized mailing campaign to all customers whose last name starts with 'M'.
-- MAGIC
-- MAGIC Filter `adventureworks.person_person` for all persons whose `LastName` starts with 'M' (use `LIKE`).

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>ORDER BY</code>, <code>LIKE</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT BusinessEntityID, FirstName, LastName
-- MAGIC FROM adventureworks.person_person
-- MAGIC WHERE LastName LIKE &#x27;M%&#x27;
-- MAGIC ORDER BY LastName, FirstName;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 7.3 (Medium)
-- MAGIC **Business context:** Marketing is planning a color-themed campaign and needs all products in the brand colors red, blue and black.
-- MAGIC
-- MAGIC Show all products from `adventureworks.production_product` where `Color` is either 'Red', 'Blue' or 'Black' (use `IN`).

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>ORDER BY</code>, <code>IN</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT ProductID, Name, Color
-- MAGIC FROM adventureworks.production_product
-- MAGIC WHERE Color IN (&#x27;Red&#x27;, &#x27;Blue&#x27;, &#x27;Black&#x27;)
-- MAGIC ORDER BY Color, Name;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 7.4 (Medium)
-- MAGIC **Business context:** The logistics department wants to identify products with two-digit number endings that follow a special packaging standard.
-- MAGIC
-- MAGIC Filter `adventureworks.production_product` for products whose `ProductNumber` ends with exactly two digits. Use `RLIKE` with a regular expression.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>ORDER BY</code>, <code>RLIKE</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT ProductID, Name, ProductNumber
-- MAGIC FROM adventureworks.production_product
-- MAGIC WHERE ProductNumber RLIKE &#x27;[0-9]{2}$&#x27;
-- MAGIC ORDER BY ProductNumber;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 7.5 (Hard)
-- MAGIC **Business context:** Controlling wants to analyze completed orders from 2011 in the mid-price segment in order to identify typical buying patterns.
-- MAGIC
-- MAGIC Filter `adventureworks.sales_salesorderheader` for all orders:
-- MAGIC - with `OrderDate` between '2011-01-01' and '2011-12-31'
-- MAGIC - and `TotalDue` between 1000 and 5000
-- MAGIC - and `Status` equal to 5
-- MAGIC
-- MAGIC Use `BETWEEN` and combine the conditions with `AND`.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>ORDER BY</code>, <code>BETWEEN</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT SalesOrderID, OrderDate, TotalDue, Status
-- MAGIC FROM adventureworks.sales_salesorderheader
-- MAGIC WHERE OrderDate BETWEEN &#x27;2011-01-01&#x27; AND &#x27;2011-12-31&#x27;
-- MAGIC   AND TotalDue BETWEEN 1000 AND 5000
-- MAGIC   AND Status = 5
-- MAGIC ORDER BY OrderDate;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Topic 8: JOINs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 8.1 (Easy)
-- MAGIC **Business context:** Product management needs an overview of all products with their associated categories for the reorganization of the online shop.
-- MAGIC
-- MAGIC Create an `INNER JOIN` between `adventureworks.production_product` and `adventureworks.production_productsubcategory` on `ProductSubcategoryID`. Show `ProductID`, `Product.Name`, `ProductSubcategory.Name`.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>INNER JOIN</code>, <code>ORDER BY</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     p.ProductID,
-- MAGIC     p.Name AS ProductName,
-- MAGIC     ps.Name AS SubcategoryName
-- MAGIC FROM adventureworks.production_product p
-- MAGIC INNER JOIN adventureworks.production_productsubcategory ps
-- MAGIC     ON p.ProductSubcategoryID = ps.ProductSubcategoryID
-- MAGIC ORDER BY ps.Name, p.Name;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 8.2 (Medium)
-- MAGIC **Business context:** The catalog department wants to see all products, including those without an assigned subcategory, in order to set data maintenance priorities.
-- MAGIC
-- MAGIC Create a `LEFT JOIN` from `adventureworks.production_product` to `adventureworks.production_productsubcategory`. Also show products without a subcategory (NULL values).

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>LEFT JOIN</code>, <code>ORDER BY</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     p.ProductID,
-- MAGIC     p.Name AS ProductName,
-- MAGIC     ps.Name AS SubcategoryName
-- MAGIC FROM adventureworks.production_product p
-- MAGIC LEFT JOIN adventureworks.production_productsubcategory ps
-- MAGIC     ON p.ProductSubcategoryID = ps.ProductSubcategoryID
-- MAGIC ORDER BY ps.Name, p.Name;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 8.3 (Medium)
-- MAGIC **Business context:** The CRM team wants to analyze the order frequency per customer in order to identify regular customers and offer loyalty programs.
-- MAGIC
-- MAGIC Join `adventureworks.sales_salesorderheader` with `adventureworks.sales_customer` on `CustomerID`. Show `SalesOrderID`, `OrderDate`, `CustomerID` and count the number of orders per customer.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>INNER JOIN</code>, <code>GROUP BY</code>, <code>ORDER BY</code>, <code>COUNT</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     c.CustomerID,
-- MAGIC     COUNT(soh.SalesOrderID) AS Order_Count
-- MAGIC FROM adventureworks.sales_customer c
-- MAGIC INNER JOIN adventureworks.sales_salesorderheader soh
-- MAGIC     ON c.CustomerID = soh.CustomerID
-- MAGIC GROUP BY c.CustomerID
-- MAGIC ORDER BY Order_Count DESC;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 8.5 (Hard)
-- MAGIC **Business context:** The mountain bike product team wants to analyze all sales in their category in order to identify bestsellers and optimize inventory levels.
-- MAGIC
-- MAGIC Join 3 tables:
-- MAGIC - `adventureworks.sales_salesorderdetail`
-- MAGIC - `adventureworks.production_product`
-- MAGIC - `adventureworks.production_productsubcategory`
-- MAGIC
-- MAGIC Show for each order: `SalesOrderID`, `ProductID`, `Product.Name`, `ProductSubcategory.Name`, `OrderQty`, `LineTotal`. Filter by subcategory = 'Mountain Bikes'.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>INNER JOIN</code>, <code>ORDER BY</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     sod.SalesOrderID,
-- MAGIC     sod.ProductID,
-- MAGIC     p.Name AS ProductName,
-- MAGIC     ps.Name AS SubcategoryName,
-- MAGIC     sod.OrderQty,
-- MAGIC     sod.LineTotal
-- MAGIC FROM adventureworks.sales_salesorderdetail sod
-- MAGIC INNER JOIN adventureworks.production_product p
-- MAGIC     ON sod.ProductID = p.ProductID
-- MAGIC INNER JOIN adventureworks.production_productsubcategory ps
-- MAGIC     ON p.ProductSubcategoryID = ps.ProductSubcategoryID
-- MAGIC WHERE ps.Name = &#x27;Mountain Bikes&#x27;
-- MAGIC ORDER BY sod.LineTotal DESC;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Topic 9: Functions and Aggregates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 9.1 (Easy)
-- MAGIC **Business context:** The pricing team needs the average product price as a benchmark for pricing new products.
-- MAGIC
-- MAGIC Calculate the average price (`AVG`) of all products from `adventureworks.production_product`.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>AVG</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT AVG(ListPrice) AS Avg_Price
-- MAGIC FROM adventureworks.production_product;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 9.2 (Easy)
-- MAGIC **Business context:** For the data export to external partners, product names must be available in standardized upper/lower case.
-- MAGIC
-- MAGIC Use `UPPER()` and `LOWER()` to show the `Name` from `adventureworks.production_product` in uppercase and lowercase.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>LIMIT</code>, <code>UPPER</code>, <code>LOWER</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     ProductID,
-- MAGIC     Name,
-- MAGIC     UPPER(Name) AS Name_Upper,
-- MAGIC     LOWER(Name) AS Name_Lower
-- MAGIC FROM adventureworks.production_product
-- MAGIC LIMIT 10;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 9.3 (Medium)
-- MAGIC **Business context:** The finance team needs the annual revenue figures for the annual report and the board presentation.
-- MAGIC
-- MAGIC From `adventureworks.sales_salesorderheader`, calculate the sum of `TotalDue` for each year (`YEAR(OrderDate)`). Use `GROUP BY`.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>GROUP BY</code>, <code>ORDER BY</code>, <code>YEAR</code>, <code>SUM</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     YEAR(OrderDate) AS OrderYear,
-- MAGIC     SUM(TotalDue) AS Annual_Revenue
-- MAGIC FROM adventureworks.sales_salesorderheader
-- MAGIC GROUP BY YEAR(OrderDate)
-- MAGIC ORDER BY OrderYear;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 9.4 (Medium)
-- MAGIC **Business context:** The CRM team wants to identify VIP customers (with more than 10 orders) in order to offer them exclusive loyalty rewards.
-- MAGIC
-- MAGIC From `adventureworks.sales_salesorderheader`, show all customers (`CustomerID`) with more than 10 orders. Use `GROUP BY` and `HAVING`.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>GROUP BY</code>, <code>HAVING</code>, <code>ORDER BY</code>, <code>COUNT</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     CustomerID,
-- MAGIC     COUNT(*) AS Order_Count
-- MAGIC FROM adventureworks.sales_salesorderheader
-- MAGIC GROUP BY CustomerID
-- MAGIC HAVING COUNT(*) &gt; 10
-- MAGIC ORDER BY Order_Count DESC;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 9.5 (Hard)
-- MAGIC **Business context:** The sales team wants to identify the top sellers and analyze their price ranges for assortment planning.
-- MAGIC
-- MAGIC From `adventureworks.sales_salesorderdetail`, calculate for each product (`ProductID`):
-- MAGIC - The total number of units sold (`SUM(OrderQty)`)
-- MAGIC - The average price (`AVG(UnitPrice)`)
-- MAGIC - The maximum and minimum price (`MAX`, `MIN`)
-- MAGIC
-- MAGIC Show only products with more than 100 units sold. Sort by total quantity in descending order.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>GROUP BY</code>, <code>HAVING</code>, <code>ORDER BY</code>, <code>SUM</code>, <code>AVG</code>, <code>MIN</code>, <code>MAX</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     ProductID,
-- MAGIC     SUM(OrderQty) AS Total_Sold,
-- MAGIC     AVG(UnitPrice) AS Avg_Price,
-- MAGIC     MAX(UnitPrice) AS Max_Price,
-- MAGIC     MIN(UnitPrice) AS Min_Price
-- MAGIC FROM adventureworks.sales_salesorderdetail
-- MAGIC GROUP BY ProductID
-- MAGIC HAVING SUM(OrderQty) &gt; 100
-- MAGIC ORDER BY Total_Sold DESC;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 9.6 (Medium)
-- MAGIC **Business context:** Category management wants to understand the distribution of products across price segments for strategic assortment decisions.
-- MAGIC
-- MAGIC Use `CASE WHEN` to create a new column "PriceCategory" from `adventureworks.production_product`:
-- MAGIC - "Cheap" for `ListPrice < 100`
-- MAGIC - "Medium" for `ListPrice` between 100 and 500
-- MAGIC - "Expensive" for `ListPrice > 500`
-- MAGIC
-- MAGIC Count the number of products per price category.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>GROUP BY</code>, <code>ORDER BY</code>, <code>CASE WHEN</code>, <code>BETWEEN</code>, <code>COUNT</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     CASE
-- MAGIC         WHEN ListPrice &lt; 100 THEN &#x27;Cheap&#x27;
-- MAGIC         WHEN ListPrice BETWEEN 100 AND 500 THEN &#x27;Medium&#x27;
-- MAGIC         ELSE &#x27;Expensive&#x27;
-- MAGIC     END AS PriceCategory,
-- MAGIC     COUNT(*) AS Product_Count
-- MAGIC FROM adventureworks.production_product
-- MAGIC GROUP BY PriceCategory
-- MAGIC ORDER BY PriceCategory;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 9.7 (Hard)
-- MAGIC **Business context:** The sales dashboard should show the largest individual orders per product – understand the different ranking methods for correct visualization.
-- MAGIC
-- MAGIC Use window functions (`ROW_NUMBER`, `RANK`, `DENSE_RANK`) to rank the top 3 orders by `OrderQty` for each product from `adventureworks.sales_salesorderdetail`. Show the differences between the three ranking functions.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>ORDER BY</code>, <code>ROW_NUMBER</code>, <code>DENSE_RANK</code>, <code>RANK</code>, <code>OVER</code>, <code>PARTITION BY</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     ProductID,
-- MAGIC     SalesOrderID,
-- MAGIC     OrderQty,
-- MAGIC     ROW_NUMBER() OVER (PARTITION BY ProductID ORDER BY OrderQty DESC) AS RowNum,
-- MAGIC     RANK() OVER (PARTITION BY ProductID ORDER BY OrderQty DESC) AS Rank,
-- MAGIC     DENSE_RANK() OVER (PARTITION BY ProductID ORDER BY OrderQty DESC) AS DenseRank
-- MAGIC FROM adventureworks.sales_salesorderdetail
-- MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY ProductID ORDER BY OrderQty DESC) &lt;= 3
-- MAGIC ORDER BY ProductID, OrderQty DESC;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Topic 10: Subqueries

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 10.1 (Easy)
-- MAGIC **Business context:** The marketing department wants to identify above-average priced products for a premium campaign.
-- MAGIC
-- MAGIC Find all products from `adventureworks.production_product` whose `ListPrice` is greater than the average price of all products. Use a scalar subquery.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>ORDER BY</code>, <code>AVG</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT ProductID, Name, ListPrice
-- MAGIC FROM adventureworks.production_product
-- MAGIC WHERE ListPrice &gt; (
-- MAGIC     SELECT AVG(ListPrice)
-- MAGIC     FROM adventureworks.production_product
-- MAGIC )
-- MAGIC ORDER BY ListPrice DESC;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 10.2 (Medium)
-- MAGIC **Business context:** Product management wants to know which products have already generated revenue vs. shelf warmers without any sales.
-- MAGIC
-- MAGIC Find all products from `adventureworks.production_product` that have been sold at least once in the table `adventureworks.sales_salesorderdetail`. Use `IN` with a subquery.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>ORDER BY</code>, <code>DISTINCT</code>, <code>IN</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT ProductID, Name
-- MAGIC FROM adventureworks.production_product
-- MAGIC WHERE ProductID IN (
-- MAGIC     SELECT DISTINCT ProductID
-- MAGIC     FROM adventureworks.sales_salesorderdetail
-- MAGIC )
-- MAGIC ORDER BY Name;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 10.5 (Medium)
-- MAGIC **Business context:** Understand the performance differences between IN and EXISTS for more efficient database analyses on large data volumes.
-- MAGIC
-- MAGIC Use `EXISTS` to find all products from `adventureworks.production_product` that have at least one order in `adventureworks.sales_salesorderdetail`. Compare the result with the IN variant from Exercise 10.2.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>ORDER BY</code>, <code>EXISTS</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT ProductID, Name
-- MAGIC FROM adventureworks.production_product p
-- MAGIC WHERE EXISTS (
-- MAGIC     SELECT 1
-- MAGIC     FROM adventureworks.sales_salesorderdetail sod
-- MAGIC     WHERE sod.ProductID = p.ProductID
-- MAGIC )
-- MAGIC ORDER BY Name;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 10.6 (Medium)
-- MAGIC **Business context:** Assortment management wants to identify shelf warmers (unsold products) in order to make phase-out decisions or launch marketing measures.
-- MAGIC
-- MAGIC Use `NOT EXISTS` to find all products from `adventureworks.production_product` that have never been sold (no entries in `adventureworks.sales_salesorderdetail`).

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>ORDER BY</code>, <code>EXISTS</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT ProductID, Name
-- MAGIC FROM adventureworks.production_product p
-- MAGIC WHERE NOT EXISTS (
-- MAGIC     SELECT 1
-- MAGIC     FROM adventureworks.sales_salesorderdetail sod
-- MAGIC     WHERE sod.ProductID = p.ProductID
-- MAGIC )
-- MAGIC ORDER BY Name;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 10.3 (Medium)
-- MAGIC **Business context:** Key account management is looking for high-value customers (lifetime value over 50,000) for personal support by dedicated account managers.
-- MAGIC
-- MAGIC Use a subquery in the `FROM` clause to first calculate the sum of `TotalDue` per customer from `adventureworks.sales_salesorderheader`. Then show only customers with a total over 50,000.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>GROUP BY</code>, <code>ORDER BY</code>, <code>SUM</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT CustomerID, Total_Revenue
-- MAGIC FROM (
-- MAGIC     SELECT CustomerID, SUM(TotalDue) AS Total_Revenue
-- MAGIC     FROM adventureworks.sales_salesorderheader
-- MAGIC     GROUP BY CustomerID
-- MAGIC )
-- MAGIC WHERE Total_Revenue &gt; 50000
-- MAGIC ORDER BY Total_Revenue DESC;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 10.4 (Hard)
-- MAGIC **Business context:** The sales analytics team needs the order frequency per product in order to measure the popularity of individual items.
-- MAGIC
-- MAGIC Create a correlated subquery: for each product from `adventureworks.production_product`, show the number of orders in `adventureworks.sales_salesorderdetail`. The subquery should determine the count individually for each product.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>ORDER BY</code>, <code>LIMIT</code>, <code>COUNT</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     p.ProductID,
-- MAGIC     p.Name,
-- MAGIC     (
-- MAGIC         SELECT COUNT(*)
-- MAGIC         FROM adventureworks.sales_salesorderdetail sod
-- MAGIC         WHERE sod.ProductID = p.ProductID
-- MAGIC     ) AS Order_Count
-- MAGIC FROM adventureworks.production_product p
-- MAGIC ORDER BY Order_Count DESC
-- MAGIC LIMIT 20;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Topic 11: CTEs (Common Table Expressions)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 11.1 (Easy)
-- MAGIC **Business context:** The luxury segment team needs a list of all high-end products (over €1000) for an exclusive catalog insert.
-- MAGIC
-- MAGIC Create a simple CTE that contains all products from `adventureworks.production_product` with `ListPrice > 1000`. Then show all columns from the CTE.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>WITH</code>, <code>ORDER BY</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>WITH ExpensiveProducts AS (
-- MAGIC     SELECT *
-- MAGIC     FROM adventureworks.production_product
-- MAGIC     WHERE ListPrice &gt; 1000
-- MAGIC )
-- MAGIC SELECT ProductID, Name, ListPrice
-- MAGIC FROM ExpensiveProducts
-- MAGIC ORDER BY ListPrice DESC;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 11.2 (Medium)
-- MAGIC **Business context:** Sales management wants to identify the top 10 customers by revenue in order to invite them to an exclusive customer event.
-- MAGIC
-- MAGIC Create a CTE that calculates the sum of `TotalDue` per `CustomerID` from `adventureworks.sales_salesorderheader`. Then use the CTE to determine the top 10 customers by revenue.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>WITH</code>, <code>GROUP BY</code>, <code>ORDER BY</code>, <code>LIMIT</code>, <code>SUM</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>WITH CustomerTotals AS (
-- MAGIC     SELECT
-- MAGIC         CustomerID,
-- MAGIC         SUM(TotalDue) AS Total_Revenue
-- MAGIC     FROM adventureworks.sales_salesorderheader
-- MAGIC     GROUP BY CustomerID
-- MAGIC )
-- MAGIC SELECT CustomerID, Total_Revenue
-- MAGIC FROM CustomerTotals
-- MAGIC ORDER BY Total_Revenue DESC
-- MAGIC LIMIT 10;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 11.3 (Medium)
-- MAGIC **Business context:** The marketing team is planning a red-themed campaign and wants to understand how well red products sell.
-- MAGIC
-- MAGIC Create two CTEs:
-- MAGIC 1. All products from `adventureworks.production_product` with `Color = 'Red'`
-- MAGIC 2. All order details from `adventureworks.sales_salesorderdetail`
-- MAGIC
-- MAGIC Join both CTEs to show red products with their sales figures.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>LEFT JOIN</code>, <code>WITH</code>, <code>GROUP BY</code>, <code>ORDER BY</code>, <code>SUM</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>WITH RedProducts AS (
-- MAGIC     SELECT ProductID, Name, Color
-- MAGIC     FROM adventureworks.production_product
-- MAGIC     WHERE Color = &#x27;Red&#x27;
-- MAGIC ),
-- MAGIC SalesData AS (
-- MAGIC     SELECT ProductID, SUM(OrderQty) AS Total_Sold
-- MAGIC     FROM adventureworks.sales_salesorderdetail
-- MAGIC     GROUP BY ProductID
-- MAGIC )
-- MAGIC SELECT
-- MAGIC     rp.ProductID,
-- MAGIC     rp.Name,
-- MAGIC     rp.Color,
-- MAGIC     sd.Total_Sold
-- MAGIC FROM RedProducts rp
-- MAGIC LEFT JOIN SalesData sd ON rp.ProductID = sd.ProductID
-- MAGIC ORDER BY sd.Total_Sold DESC;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 11.4 (Hard)
-- MAGIC **Business context:** Product management needs a ranking of the top 5 bestsellers by revenue in order to optimize production decisions and inventory planning.
-- MAGIC
-- MAGIC Create a nested CTE structure:
-- MAGIC 1. CTE1: From `adventureworks.sales_salesorderdetail`, calculate the sum of `LineTotal` per `ProductID`
-- MAGIC 2. CTE2: Join CTE1 with `adventureworks.production_product` and show `ProductID`, `Name` and `TotalSales`
-- MAGIC 3. Main query: Show the top 5 products by `TotalSales` in descending order

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>INNER JOIN</code>, <code>WITH</code>, <code>GROUP BY</code>, <code>ORDER BY</code>, <code>LIMIT</code>, <code>SUM</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>WITH ProductSales AS (
-- MAGIC     SELECT
-- MAGIC         ProductID,
-- MAGIC         SUM(LineTotal) AS TotalSales
-- MAGIC     FROM adventureworks.sales_salesorderdetail
-- MAGIC     GROUP BY ProductID
-- MAGIC ),
-- MAGIC ProductDetails AS (
-- MAGIC     SELECT
-- MAGIC         p.ProductID,
-- MAGIC         p.Name,
-- MAGIC         ps.TotalSales
-- MAGIC     FROM adventureworks.production_product p
-- MAGIC     INNER JOIN ProductSales ps ON p.ProductID = ps.ProductID
-- MAGIC )
-- MAGIC SELECT ProductID, Name, TotalSales
-- MAGIC FROM ProductDetails
-- MAGIC ORDER BY TotalSales DESC
-- MAGIC LIMIT 5;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Topic 12: Set Operations (UNION, INTERSECT, EXCEPT)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 12.2 (Medium)
-- MAGIC **Business context:** The sales department wants to know which products from the catalog have actually been sold in order to evaluate the product range.
-- MAGIC
-- MAGIC Use `INTERSECT` to find all `ProductID` values that occur both in `adventureworks.production_product` and in `adventureworks.sales_salesorderdetail`.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>INTERSECT</code>, <code>ORDER BY</code>, <code>DISTINCT</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT ProductID
-- MAGIC FROM adventureworks.production_product
-- MAGIC INTERSECT
-- MAGIC SELECT DISTINCT ProductID
-- MAGIC FROM adventureworks.sales_salesorderdetail
-- MAGIC ORDER BY ProductID;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 12.3 (Medium)
-- MAGIC **Business context:** Product management wants to identify unsold products in order to decide whether they should be removed from the range or promoted more heavily.
-- MAGIC
-- MAGIC Use `EXCEPT` to find all products from `adventureworks.production_product` that have never been sold in `adventureworks.sales_salesorderdetail`.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>EXCEPT</code>, <code>ORDER BY</code>, <code>DISTINCT</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT ProductID
-- MAGIC FROM adventureworks.production_product
-- MAGIC EXCEPT
-- MAGIC SELECT DISTINCT ProductID
-- MAGIC FROM adventureworks.sales_salesorderdetail
-- MAGIC ORDER BY ProductID;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Topic 13: Advanced Window Functions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 13.1 (Medium)
-- MAGIC **Business context:** The CRM team wants to analyze purchase frequency by measuring the time intervals between consecutive orders per customer.
-- MAGIC
-- MAGIC Use `LEAD()` and `LAG()` to show the previous and next order (`OrderDate`) for each customer from `adventureworks.sales_salesorderheader`. Partition by `CustomerID` and sort by `OrderDate`.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>ORDER BY</code>, <code>LAG</code>, <code>LEAD</code>, <code>OVER</code>, <code>PARTITION BY</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     CustomerID,
-- MAGIC     SalesOrderID,
-- MAGIC     OrderDate,
-- MAGIC     LAG(OrderDate) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS Previous_Order,
-- MAGIC     LEAD(OrderDate) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS Next_Order
-- MAGIC FROM adventureworks.sales_salesorderheader
-- MAGIC ORDER BY CustomerID, OrderDate;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 13.2 (Hard)
-- MAGIC **Business context:** The finance team needs a cumulative revenue history per customer in order to visualize the growth of the customer lifetime value.
-- MAGIC
-- MAGIC Use `SUM() OVER()` to calculate a running total for `TotalDue` from `adventureworks.sales_salesorderheader`, partitioned by `CustomerID` and sorted by `OrderDate`.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>ORDER BY</code>, <code>SUM</code>, <code>OVER</code>, <code>PARTITION BY</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>SELECT
-- MAGIC     CustomerID,
-- MAGIC     SalesOrderID,
-- MAGIC     OrderDate,
-- MAGIC     TotalDue,
-- MAGIC     SUM(TotalDue) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS Running_Total
-- MAGIC FROM adventureworks.sales_salesorderheader
-- MAGIC ORDER BY CustomerID, OrderDate;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 13.3 (Hard)
-- MAGIC **Business context:** The pricing strategy department wants to divide the product range into four price segments in order to develop target-group-specific marketing campaigns.
-- MAGIC
-- MAGIC Use `NTILE(4)` to divide all products from `adventureworks.production_product` into four quartiles based on `ListPrice`. Show the minimum and maximum prices for each quartile.

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>WITH</code>, <code>GROUP BY</code>, <code>ORDER BY</code>, <code>COUNT</code>, <code>MIN</code>, <code>MAX</code>, <code>NTILE</code>, <code>OVER</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>WITH ProductQuartiles AS (
-- MAGIC     SELECT
-- MAGIC         ProductID,
-- MAGIC         Name,
-- MAGIC         ListPrice,
-- MAGIC         NTILE(4) OVER (ORDER BY ListPrice) AS PriceQuartile
-- MAGIC     FROM adventureworks.production_product
-- MAGIC     WHERE ListPrice &gt; 0
-- MAGIC )
-- MAGIC SELECT
-- MAGIC     PriceQuartile,
-- MAGIC     MIN(ListPrice) AS Min_Price,
-- MAGIC     MAX(ListPrice) AS Max_Price,
-- MAGIC     COUNT(*) AS Product_Count
-- MAGIC FROM ProductQuartiles
-- MAGIC GROUP BY PriceQuartile
-- MAGIC ORDER BY PriceQuartile;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 13.4 (Hard)
-- MAGIC **Business context:** Category management wants to identify the top 3 bestsellers by revenue for each product category for optimized shelf placement.
-- MAGIC
-- MAGIC Combine window functions: use `DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...)` to determine the top 3 products by total revenue (`SUM(LineTotal)`) for each `ProductSubcategoryID` from `adventureworks.sales_salesorderdetail` (with a JOIN to `product`).

-- COMMAND ----------

-- your solution


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>INNER JOIN</code>, <code>WITH</code>, <code>GROUP BY</code>, <code>ORDER BY</code>, <code>IS NOT NULL</code>, <code>SUM</code>, <code>DENSE_RANK</code>, <code>OVER</code>, <code>PARTITION BY</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>WITH CategorySales AS (
-- MAGIC     SELECT
-- MAGIC         p.ProductSubcategoryID,
-- MAGIC         p.ProductID,
-- MAGIC         p.Name,
-- MAGIC         SUM(sod.LineTotal) AS TotalSales
-- MAGIC     FROM adventureworks.production_product p
-- MAGIC     INNER JOIN adventureworks.sales_salesorderdetail sod
-- MAGIC         ON p.ProductID = sod.ProductID
-- MAGIC     WHERE p.ProductSubcategoryID IS NOT NULL
-- MAGIC     GROUP BY p.ProductSubcategoryID, p.ProductID, p.Name
-- MAGIC ),
-- MAGIC RankedSales AS (
-- MAGIC     SELECT
-- MAGIC         *,
-- MAGIC         DENSE_RANK() OVER (PARTITION BY ProductSubcategoryID ORDER BY TotalSales DESC) AS SalesRank
-- MAGIC     FROM CategorySales
-- MAGIC )
-- MAGIC SELECT
-- MAGIC     ProductSubcategoryID,
-- MAGIC     ProductID,
-- MAGIC     Name,
-- MAGIC     TotalSales,
-- MAGIC     SalesRank
-- MAGIC FROM RankedSales
-- MAGIC WHERE SalesRank &lt;= 3
-- MAGIC ORDER BY ProductSubcategoryID, SalesRank;</code></pre></details>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 13.5 (Very Hard)
-- MAGIC Create an analysis with multiple window functions:
-- MAGIC - For each product in `adventureworks.sales_salesorderdetail`:
-- MAGIC   - Calculate the rank by quantity sold (`RANK()` over `SUM(OrderQty)`)
-- MAGIC   - Calculate the percentage share of total revenue (`PERCENT_RANK()`)
-- MAGIC   - Show the moving average of the last 3 orders (`AVG() OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)`)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Wrap-up
-- MAGIC
-- MAGIC Good luck solving the exercises! 🚀
-- MAGIC
-- MAGIC **Tips:**
-- MAGIC - Test each query individually against the AdventureWorks database
-- MAGIC - Use the SQL documentation if anything is unclear
-- MAGIC - Pay attention to NULL values and use the appropriate functions (`COALESCE`, `IFNULL`)
-- MAGIC - For complex queries: build and test them step by step
-- MAGIC - Keep the business context in mind: which question does your analysis answer?
-- MAGIC
-- MAGIC **Key concepts to practice:**
-- MAGIC - **JOINs** for linking data between tables
-- MAGIC - **Aggregations** with GROUP BY and HAVING for summaries
-- MAGIC - **Window functions** for advanced analyses without grouping
-- MAGIC - **CTEs** for readable, structured complex queries
-- MAGIC - **Set operations** for combining result sets

-- COMMAND ----------

-- your solution

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>💡 Hint – useful building blocks</b></summary><p style='margin:6px 0'>You will need: <code>INNER JOIN</code>, <code>WITH</code>, <code>GROUP BY</code>, <code>ORDER BY</code>, <code>LIMIT</code>, <code>BETWEEN</code>, <code>ROUND</code>, <code>SUM</code>, <code>AVG</code>, <code>RANK</code>, <code>PERCENT_RANK</code>, <code>OVER</code>, <code>ROWS BETWEEN</code></p></details>
-- MAGIC <details style='margin:4px 0;padding:6px 10px;border:1px solid #d0d7de;border-radius:6px;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;font-size:14px'><summary style='cursor:pointer'><b>🔑 Solution</b></summary><p style='margin:6px 0'>Try it yourself first – then compare.</p><pre style='background:#f6f8fa;padding:8px;border-radius:4px;overflow-x:auto;font-size:13px;line-height:1.4'><code>WITH ProductMetrics AS (
-- MAGIC     SELECT
-- MAGIC         ProductID,
-- MAGIC         SUM(OrderQty) AS Total_Sold,
-- MAGIC         SUM(LineTotal) AS Total_Revenue,
-- MAGIC         AVG(OrderQty) AS Avg_OrderQty
-- MAGIC     FROM adventureworks.sales_salesorderdetail
-- MAGIC     GROUP BY ProductID
-- MAGIC ),
-- MAGIC RankedMetrics AS (
-- MAGIC     SELECT
-- MAGIC         ProductID,
-- MAGIC         Total_Sold,
-- MAGIC         Total_Revenue,
-- MAGIC         RANK() OVER (ORDER BY Total_Sold DESC) AS Rank_Quantity_Sold,
-- MAGIC         PERCENT_RANK() OVER (ORDER BY Total_Revenue) AS Percent_Revenue,
-- MAGIC         AVG(Total_Revenue) OVER (
-- MAGIC             ORDER BY Total_Sold
-- MAGIC             ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
-- MAGIC         ) AS Moving_Average
-- MAGIC     FROM ProductMetrics
-- MAGIC )
-- MAGIC SELECT
-- MAGIC     p.ProductID,
-- MAGIC     p.Name,
-- MAGIC     rm.Total_Sold,
-- MAGIC     rm.Total_Revenue,
-- MAGIC     rm.Rank_Quantity_Sold,
-- MAGIC     ROUND(rm.Percent_Revenue, 4) AS Percent_Revenue,
-- MAGIC     ROUND(rm.Moving_Average, 2) AS Moving_Average_Revenue
-- MAGIC FROM RankedMetrics rm
-- MAGIC INNER JOIN adventureworks.production_product p
-- MAGIC     ON rm.ProductID = p.ProductID
-- MAGIC ORDER BY rm.Rank_Quantity_Sold
-- MAGIC LIMIT 20;</code></pre></details>

