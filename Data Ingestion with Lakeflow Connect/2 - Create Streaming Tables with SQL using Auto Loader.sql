-- Databricks notebook source
-- MAGIC %md
-- MAGIC #2 - Create Streaming Tables with SQL using Auto Loader

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Objetivos de Aprendizado

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Create sttreaming tables in Databricks SQL for incremental data ingestion.
-- MAGIC - Refresh streaming tables using the REFRESH statement
-- MAGIC
-- MAGIC > The CREATE STREAMING TABLES SQL is the recommended alternaive to the legacy COPY INTO SQL command for incremental ingestion from cloud object storage. Databricks recommends using streaming tables to ingest data using Databricks SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Create Streaming Tables for Incremental Processing

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Recurso utilizado: SQL Waherouse (Serveless)

-- COMMAND ----------

-- DBTITLE 1,Setup do ambiente
USE CATALOG learning;
USE SCHEMA bronze;
SELECT current_catalog(), current_schema();

-- COMMAND ----------

-- DBTITLE 1,Visualizar arquivo CSV
SELECT *
FROM read_files(
  '/Volumes/learning/sales/raw/ingest_simulation',
  format => 'csv',
  header => true
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Using Databricks SQL
-- MAGIC [Streaming tables in Databricks SQL](https://docs.databricks.com/aws/en/ldp/dbsql/streaming)

-- COMMAND ----------

-- DBTITLE 1,Create a streaming table
CREATE OR REFRESH STREAMING TABLE sql_autoloader_address
SCHEDULE EVERY 1 WEEK
AS
SELECT *
FROM STREAM read_files(
  '/Volumes/learning/sales/raw/ingest_simulation',
  format => 'csv',
  sep => ',',
  header => true
);

-- COMMAND ----------

-- DBTITLE 1,Visualizar streaming table
SELECT *
FROM sql_autoloader_address

-- COMMAND ----------

-- DBTITLE 1,Detalhamento da tabela
DESCRIBE TABLE EXTENDED sql_autoloader_address

-- COMMAND ----------

-- DBTITLE 1,Ver histórico
DESCRIBE HISTORY sql_autoloader_address

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Adicionar novo arquivo na pasta e atualizar na tabela

-- COMMAND ----------

-- DBTITLE 1,Refresh table
REFRESH STREAMING TABLE sql_autoloader_address

-- COMMAND ----------

-- DBTITLE 1,Visualizar a tabela atualizada
SELECT *
FROM sql_autoloader_address

-- COMMAND ----------

DESCRIBE HISTORY sql_autoloader_address

-- COMMAND ----------

-- DBTITLE 1,Deletar tabela
DROP TABLE sql_autoloader_address

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Recursos adicionais
-- MAGIC [Use streaming tables in Databricks SQL](https://docs.databricks.com/aws/en/ldp/dbsql/streaming)
-- MAGIC
-- MAGIC [Create streaming tables](https://docs.databricks.com/aws/en/ldp/dbsql/streaming#create-streaming-tables)
-- MAGIC
-- MAGIC [Refresh a streaming table](https://docs.databricks.com/aws/en/ldp/dbsql/streaming#refresh-a-streaming-table)
-- MAGIC
-- MAGIC [Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/)