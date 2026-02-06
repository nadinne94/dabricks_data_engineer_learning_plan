-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS learning.bronze;
USE learning.bronze;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC customer_address_df = spark.read.table('learning.ingestion_simulation.address_raw')
-- MAGIC customer_address_df.write.mode('overwrite').saveAsTable('address_bronze')