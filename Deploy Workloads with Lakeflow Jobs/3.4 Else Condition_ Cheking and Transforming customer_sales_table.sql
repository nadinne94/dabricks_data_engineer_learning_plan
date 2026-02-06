-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS learning.silver;
CREATE SCHEMA IF NOT EXISTS learning.gold;

-- COMMAND ----------

USE learning.silver

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ### ----- Data Cleaning -----
-- MAGIC
-- MAGIC # strip whitespace from columns
-- MAGIC df = spark.sql("select * from learning.silver.customer_sales_silver")
-- MAGIC df = df.toDF(*[c.strip() for c in df.columns])
-- MAGIC
-- MAGIC # standardize column names to title case and strip whitespace
-- MAGIC from pyspark.sql.functions import trim, initcap, col
-- MAGIC df = df.withColumn("FirstName", trim(initcap(col("FirstName"))))
-- MAGIC df = df.withColumn("LastName", trim(initcap(col("LastName"))))
-- MAGIC
-- MAGIC # convert data types for IDs, numeric columns, and dates
-- MAGIC from pyspark.sql.functions import to_date, regexp_replace
-- MAGIC
-- MAGIC df = df.withColumn("CustomerID", col("CustomerID").cast("long"))\
-- MAGIC        .withColumn("OrderQty", col("OrderQty").cast("double"))\
-- MAGIC        .withColumn("LineTotal",regexp_replace("LineTotal", ",", ".").cast("decimal(10,2)"))\
-- MAGIC        .withColumn("OrderDate", to_date(col("OrderDate")))
-- MAGIC
-- MAGIC # drop rows with missing values after conversions
-- MAGIC df = df.dropna(subset=["CustomerID", "OrderQty", "LineTotal", "OrderDate"])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ### ----------- Feature Engineering -----------
-- MAGIC from pyspark.sql.functions import year, month, date_format, round as spark_round, col
-- MAGIC
-- MAGIC # extract year and month from date
-- MAGIC df = df.withColumn("ModifiedYear", year(col("OrderDate")))\
-- MAGIC        .withColumn("ModifiedMonth", month(col("OrderDate")))
-- MAGIC
-- MAGIC #calculate total price
-- MAGIC from pyspark.sql.functions import when
-- MAGIC
-- MAGIC df = df.withColumn("UnitPrice",spark_round(when(col("OrderQty") != 0, 
-- MAGIC                      col("LineTotal") / col("OrderQty")).otherwise(None),2))

-- COMMAND ----------

USE learning.gold

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").saveAsTable("learning.gold.customer_sales_gold")