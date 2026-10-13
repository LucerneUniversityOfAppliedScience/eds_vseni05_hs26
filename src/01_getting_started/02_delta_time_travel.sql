-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 – Delta Lake: versioning & time travel
-- MAGIC
-- MAGIC **Goal (≈ 15 min):** See why Delta tables are more than "CSV files, but nicer":
-- MAGIC every change is logged, and you can go back to an earlier version at any time.
-- MAGIC
-- MAGIC This notebook is written entirely in **SQL** (default language next to the title).
-- MAGIC It uses the hourly weather data from the volume – no other notebook needs to run first.

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA exercises;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1 – Create a playground table
-- MAGIC Daily weather in Zurich 2025, computed from the hourly CSV file.

-- COMMAND ----------

DROP TABLE IF EXISTS delta_playground;   -- start fresh, so the first version is 0

CREATE TABLE delta_playground AS
SELECT to_date(time)                                AS day,
       ROUND(AVG(CAST(temperature_c AS DOUBLE)), 1)  AS temp_mean_c,
       ROUND(SUM(CAST(precipitation_mm AS DOUBLE)), 1) AS precipitation_mm
FROM read_files('/Volumes/workspace/mobility_raw/landing/mobility/weather/', format => 'csv', header => true)
WHERE time LIKE '2025%'
GROUP BY 1;

SELECT COUNT(*) AS days, ROUND(AVG(temp_mean_c), 2) AS avg_temp FROM delta_playground;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2 – Oops! A faulty update
-- MAGIC Someone wants to correct the temperature of a single day – and forgets the `WHERE` clause … 😱

-- COMMAND ----------

UPDATE delta_playground SET temp_mean_c = 0;

SELECT COUNT(*) AS days, ROUND(AVG(temp_mean_c), 2) AS avg_temp FROM delta_playground;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3 – The history of the table
-- MAGIC Delta logs every operation: who, when, what.

-- COMMAND ----------

DESCRIBE HISTORY delta_playground;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4 – Time travel: what did the table look like before?
-- MAGIC Version **0** is the freshly created table (see `DESCRIBE HISTORY` above).

-- COMMAND ----------

SELECT COUNT(*) AS days, ROUND(AVG(temp_mean_c), 2) AS avg_temp
FROM delta_playground VERSION AS OF 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5 – Restore the correct version

-- COMMAND ----------

RESTORE TABLE delta_playground TO VERSION AS OF 0;

SELECT COUNT(*) AS days, ROUND(AVG(temp_mean_c), 2) AS avg_temp FROM delta_playground;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ✏️ **Try it:**
-- MAGIC 1. Delete all rainy days with `DELETE FROM delta_playground WHERE precipitation_mm >= 1`.
-- MAGIC 2. Look at `DESCRIBE HISTORY` again. Which version number does your `DELETE` have?
-- MAGIC 3. Use time travel to compare the number of rows before and after the delete.
-- MAGIC 4. Open the table in the **Catalog Explorer → History** – you see the same in the UI.

-- COMMAND ----------

-- your code here


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clean up

-- COMMAND ----------

DROP TABLE IF EXISTS delta_playground;
