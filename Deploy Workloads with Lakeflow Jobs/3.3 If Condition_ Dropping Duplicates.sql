-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS learning.silver;
USE learning.silver;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC customers_sales_df = spark.sql('''
-- MAGIC                                  select distinct * from customer_sales_silver''')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC customers_sales_df.write.mode('overwrite').saveAsTable('customer_sales_silver')