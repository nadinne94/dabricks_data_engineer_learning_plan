-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Handiling CSV Ingestion with the Rescued Data Column

-- COMMAND ----------

-- DBTITLE 1,Setup do ambiente
USE learning.bronze;
SELECT current_catalog(), current_schema();

-- COMMAND ----------

-- DBTITLE 1,Inspecionar arquivos
LIST '/Volumes/learning/ingestion_simulation/raw'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Visualizar e criar tabelas com SQL a partir de arquivos CSV

-- COMMAND ----------

-- DBTITLE 1,Consultar arquivos csv
SELECT *
FROM csv.`/Volumes/learning/ingestion_simulation/raw/Address.csv`
LIMIT 5;

-- COMMAND ----------

-- DBTITLE 1,Ler csv read_files
SELECT *
FROM read_files(
        "/Volumes/learning/ingestion_simulation/raw/Address.csv",
        format => "csv",
        sep => ";",
        header => "true"
) LIMIT 5

-- COMMAND ----------

-- DBTITLE 1,Criar tabela com read_files
DROP TABLE IF EXISTS address_bronze;

CREATE TABLE address_bronze AS
SELECT *,
       _metadata.file_modification_time AS file_modification_time,
       _metadata.file_name AS source_file,
       current_timestamp() AS ingestion_time
FROM read_files(
        "/Volumes/learning/ingestion_simulation/raw/Address.csv",
        format => "csv",
        sep => ";",
        header => "true"
);

SELECT *
FROM address_bronze
LIMIT 5


-- COMMAND ----------

-- DBTITLE 1,View the inferred schema
DESCRIBE TABLE EXTENDED address_bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Visualizar e criar tabelas com Python a partir de arquivos CSV

-- COMMAND ----------

-- DBTITLE 1,Consultar arquivos
-- MAGIC %python
-- MAGIC
-- MAGIC df = (spark
-- MAGIC       .read
-- MAGIC       .option("header", True)
-- MAGIC       .option("sep", ";")
-- MAGIC       .option("rescuedDataColumn", "_rescued_data")                 #add rescued data
-- MAGIC       .csv("/Volumes/learning/ingestion_simulation/raw/Address.csv")
-- MAGIC     )
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

-- DBTITLE 1,Criar e visualizar tabela
-- MAGIC %python
-- MAGIC
-- MAGIC spark.sql("DROP TABLE IF EXISTS learning.bronze.address_with_python")
-- MAGIC (df.write
-- MAGIC   .format("delta")
-- MAGIC   .mode("overwrite")
-- MAGIC   .saveAsTable(f"learning.bronze.address_with_python"))
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Problemas comuns com arquivos csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Definir o schema durante a ingestão
-- MAGIC - melhora o processamento
-- MAGIC - mais rápido
-- MAGIC - recomendado

-- COMMAND ----------

-- DBTITLE 1,Definir o schema inteiro
SELECT *
FROM read_files(
        "/Volumes/learning/ingestion_simulation/raw/Address.csv",
        format => "csv",
        sep => ";",
        header => "true",
        schema => '''AddressID INT, AddressLine1 STRING, AddressLine2 STRING, City STRING, StateProvince STRING, CountryRegion STRING, PostalCode BIGINT, rowguid STRING, ModifiedDate DATE''',
        rescueddatacolumn => "rescued_data"
);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Rescued Data Column

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Rescued Data Columns armazena os dados da linha que não estão de acordo como o tipo de data da coluna, ao invés deste dado ser deletado. A coluna armazena qualquer dado que não esteja de acordo com o schema:
-- MAGIC - A coluna não consta no schema
-- MAGIC - Erros de digitação
-- MAGIC - Case mismatches

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exemplo: Ausencia do nome de uma coluna

-- COMMAND ----------

/*
SELECT
  cast(_rescued_data:_cn AS TYPE) AS nome_da_coluna,                -- cn: coluna a ser trabalhada(c0, c1, c2 etc)
  *
FROM
  read_files(
    "/Volumes/learning/ingestion_simulation/raw/Address.csv",
    format => "csv",
    sep => ";",
    header => "true"
  ) */

-- COMMAND ----------

DROP SCHEMA IF EXISTS learning.bronze CASCADE;
CREATE SCHEMA learning.bronze;