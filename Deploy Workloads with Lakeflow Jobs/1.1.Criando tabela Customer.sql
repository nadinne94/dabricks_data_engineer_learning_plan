-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS learning.bronze;
USE learning.bronze;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC customer_address_df = spark.read.table('learning.ingestion_simulation.customer_raw')
-- MAGIC customer_address_df.write.mode('overwrite').saveAsTable('customer_bronze')