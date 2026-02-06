-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS learning.bronze;
CREATE SCHEMA if not exists learning.silver;

-- COMMAND ----------

USE learning.bronze;

CREATE OR REPLACE TABLE learning.silver.customer_sales_silver AS
SELECT
    ca.CustomerID,
    c.FirstName,
    c.LastName,
    s.OrderDate,
    s.Name,
    s.OrderQty,
    s.LineTotal
FROM customer_address_bronze ca
JOIN
    customer_bronze c
ON
    ca.CustomerID = c.CustomerID
JOIN
    sales_order_bronze s
ON
    ca.CustomerID = s.CustomerID


-- COMMAND ----------

USE learning.silver;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("""
-- MAGIC                SELECT * FROM customer_sales_silver
-- MAGIC             """)
-- MAGIC #checar duplicados
-- MAGIC duplicate_existis = df.count() > df.dropDuplicates().count()
-- MAGIC
-- MAGIC #Boolean para taskValues
-- MAGIC dbutils.jobs.taskValues.set(key="has_duplicates", value=duplicate_existis)