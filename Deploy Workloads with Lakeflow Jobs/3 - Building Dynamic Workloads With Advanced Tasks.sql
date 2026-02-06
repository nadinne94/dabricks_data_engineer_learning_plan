-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Criando Tabelas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.1 Joining Custumers and Orders Table
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.2 Joing CustomerAdress e Customer
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Aplicando condições (if/else)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.3 If Codition: Dropping Duplicates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.4 Else Condition_ Cheking and Transforming customer_sales_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Aplicando iteração (for each)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.5 For each: Customer Address State

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Resultados

-- COMMAND ----------

SELECT *
FROM learning.silver.customer_address_california_silver

-- COMMAND ----------

SELECT *
FROM learning.silver.customer_address_nevada_silver

-- COMMAND ----------

SELECT *
FROM learning.silver.customer_address_new_mexico_silver

-- COMMAND ----------

SELECT *
FROM learning.gold.customer_sales_gold