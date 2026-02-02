-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Adding Metadata Columns During Ingestion

-- COMMAND ----------

-- DBTITLE 1,Definindo schema
USE learning.bronze

-- COMMAND ----------

-- DBTITLE 1,Verificando catalogo e schema
SELECT current_catalog(), current_database();

-- COMMAND ----------

-- DBTITLE 1,Listando os arquivos
LIST '/Volumes/learning/ingestion_simulation/raw'

-- COMMAND ----------

SELECT *
FROM read_files(
  "/Volumes/learning/ingestion_simulation/raw/Address.csv",
  format => 'csv',
  sep => ';',
  header => true)
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Ingestion Requirements
-- MAGIC - Alter some column
-- MAGIC - Include the input file name
-- MAGIC - Include the last modification timestamp of the imput file
-- MAGIC - Add the file ingestion time to the Bronze table.
-- MAGIC Note: The _metadata column is available across all supported input file formats

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Convert using Unixtime
-- MAGIC [Unixtime](https://learn.microsoft.com/pt-br/azure/databricks/sql/language-manual/functions/from_unixtime)

-- COMMAND ----------

/*
SELECT
  *,
  cast(from_unixtime(nome_da_coluna / 1000000) AS DATE) AS nome_da_coluna_date
FROM read_files(
  "/Volumes/learning/ingestion_simulation/raw/Address.csv",
  format => 'csv',
  sep => ';'
 )*/

-- COMMAND ----------

-- DBTITLE 1,Convert Unix to timestamp
SELECT
    *,
    DATEDIFF(DAY, '1899-12-30', ModifiedDate) AS ModifiedDate_serial
FROM read_files(
  "/Volumes/learning/ingestion_simulation/raw/SalesOrderHeader.csv",
  format => 'csv',
  sep => ';')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Adding Column Metadata on Ingestion
-- MAGIC - file_modification_time
-- MAGIC - ingestion_time
-- MAGIC
-- MAGIC [File Matadata](https://docs.databricks.com/aws/en/ingestion/file-metadata-column)

-- COMMAND ----------

-- DBTITLE 1,Add matadata columns
SELECT
  *,
  DATEDIFF(DAY, '1899-12-30', ModifiedDate) AS ModifiedDate_serial,
  _metadata.file_modification_time AS file_modification_time,           --last data source file modification time
  _metadata.file_name AS source_file,                                   --ingest data source file name
  current_timestamp() AS ingestion_time                                 --ingestion timestamp
FROM read_files(
  "/Volumes/learning/ingestion_simulation/raw/SalesOrderHeader.csv",
  format => 'csv',
  sep => ';')
LIMIT 10;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Criando tabela final schema bronze

-- COMMAND ----------

-- DBTITLE 1,Criar tabela
DROP TABLE IF EXISTS salesorderheader_bronze;

CREATE TABLE salesorderheader_bronze AS
SELECT
    *,
    DATEDIFF(DAY, '1899-12-30', ModifiedDate) AS ModifiedDate_serial,
    _metadata.file_modification_time AS file_modification_time,           --last data source file modification time
    _metadata.file_name AS source_file,                                   --ingest data source file name
    current_timestamp() AS ingestion_time
FROM read_files(
  "/Volumes/learning/ingestion_simulation/raw/SalesOrderHeader.csv",
  format => 'csv',
  sep => ';');

--View the final bronze table
SELECT * 
FROM salesorderheader_bronze
LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,Explore the final bronze table
SELECT
  source_file,
  count(*) as total
FROM salesorderheader_bronze
GROUP BY source_file
ORDER BY source_file;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Com python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import (input_file_name, col, datediff, lit, current_timestamp)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #1. Read the csv files in cloud storage into a Spark DataFrame
-- MAGIC df = (spark
-- MAGIC       .read
-- MAGIC       .format("csv")
-- MAGIC       .option("sep", ";")
-- MAGIC       .option("header", "true")
-- MAGIC       .load("/Volumes/learning/ingestion_simulation/raw/SalesOrderHeader.csv")
-- MAGIC     )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #2. Add a metadata column
-- MAGIC df_with_metadata = (
-- MAGIC     df.withColumn("ModifiedDate_serial",datediff(col("ModifiedDate"), lit("1899-12-30")))
-- MAGIC       .withColumn("file_modification_time",col("_metadata.file_modification_time"))
-- MAGIC       #.withColumn("source_file", input_file_name())
-- MAGIC       .withColumn("source_file",col("_metadata.file_path"))
-- MAGIC       .withColumn("ingestion_time", current_timestamp())
-- MAGIC     )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC No Unity Catalog:
-- MAGIC
-- MAGIC - Use _metadata.file_path
-- MAGIC - Não use input_file_name()
-- MAGIC
-- MAGIC | Ambiente      | Função                |
-- MAGIC | ------------- | --------------------- |
-- MAGIC | Sem UC        | `input_file_name()`   |
-- MAGIC | Unity Catalog | `_metadata.file_path` |
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (df_with_metadata
-- MAGIC     .write
-- MAGIC     .format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .saveAsTable(f"learning.bronze.salesorderheader_metadata")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC salesorderheader_metadata = spark.table("salesorderheader_metadata")
-- MAGIC display(salesorderheader_metadata)

-- COMMAND ----------

DROP TABLE IF EXISTS salesorderheader_bronze;
DROP TABLE IF EXISTS salesorderheader_metadata;