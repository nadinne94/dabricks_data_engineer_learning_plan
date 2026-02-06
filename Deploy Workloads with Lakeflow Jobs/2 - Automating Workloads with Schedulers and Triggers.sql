-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Criar pasta de acionamento do gatilho

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS learning.bronze;
USE learning.bronze;
CREATE VOLUME IF NOT EXISTS trigger_storage_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Salvar em uma variável a pasta referente ao gatilho

-- COMMAND ----------

-- DBTITLE 1,Caminho do gatilho
-- MAGIC %python
-- MAGIC your_volume_path = (f"/Volumes/learning/bronze/trigger_storage_location")
-- MAGIC print(your_volume_path)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Acionar o gatilho

-- COMMAND ----------

-- DBTITLE 1,Inserir arquivo pra acionar o gatilho
-- MAGIC %python
-- MAGIC dbutils.fs.cp("/Volumes/learning/ingestion_simulation/raw/CustomerAddress.csv", "/Volumes/learning/bronze/trigger_storage_location/CustomerAddress.csv")
-- MAGIC