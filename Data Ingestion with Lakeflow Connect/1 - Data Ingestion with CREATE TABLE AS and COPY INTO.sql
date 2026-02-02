-- Databricks notebook source
-- MAGIC %md
-- MAGIC #1 - Data Ingestion with CREATE TABLE AS and COPY INTO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Objetivos de Apredizado

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Use the CTAS statement witj read_files() to ingest Parquet files into a Delta table
-- MAGIC - Use COPY INTO to incrementally load Parquet files from cloud object storage into a Delta table.
-- MAGIC
-- MAGIC **Required - CLASSIC COMPUTE**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Origem do Dados
-- MAGIC Utilizado o Banco de Dado [AdventureWorks](https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorksLT2025.bak)
-- MAGIC - Formato .bak
-- MAGIC   - Backup binário do SQL Server
-- MAGIC   - Não é dado analítico
-- MAGIC   - Databricks não lê
-- MAGIC
-- MAGIC - Transformação para CSV
-- MAGIC   - Restaurar o arquivo .bak no SQL Server Management Studio (SSMS)
-- MAGIC   - Exportar dados (SQL Server Import and Export Wizard)
-- MAGIC
-- MAGIC Para acompanhar a Demo do curso:
-- MAGIC - criar um schema e volume para armazenar os dados brutos
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Check de catálogo e schema
SELECT current_catalog(), current_schema();

-- COMMAND ----------

-- DBTITLE 1,Schema e volume para armazenar os dados
--criar schema e volume
CREATE SCHEMA IF NOT EXISTS learning.sales;
CREATE VOLUME IF NOT EXISTS learning.sales.raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Upload dos arquivos CSV
-- MAGIC
-- MAGIC `Catalog → sales → raw → Upload files`

-- COMMAND ----------

-- DBTITLE 1,Listar os arquivos no formato bruto
LIST '/Volumes/learning/sales/raw/CustomerAddress.csv';


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Setup do ambiente
-- MAGIC - utilizar o catálago padrão (apropriado para exercícios práticos)
-- MAGIC - criar schema
-- MAGIC - criar volume

-- COMMAND ----------

-- DBTITLE 1,Criar schema para tabelas
--criar schema
CREATE SCHEMA IF NOT EXISTS learning.bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Ingestão de dados em lote com CREATE TABLE AS _(CTAS)_ e read_files()
-- MAGIC
-- MAGIC CTAS com a função [read_files()](https://docs.databricks.com/aws/pt/sql/language-manual/functions/read_files)

-- COMMAND ----------

-- DBTITLE 1,Ler os dados.csv com read_files
--Leitura da tabela dentro do schema "sales"
SELECT *
FROM read_files(
  '/Volumes/learning/sales/raw/CustomerAddress.csv',
  format => 'csv',
  header => true
)
LIMIT 10;


-- COMMAND ----------

-- DBTITLE 1,Criar tabela com read_files
-- criar tabela dentro do schema "bronze"
USE learning.bronze;

-- criar delta table
CREATE TABLE IF NOT EXISTS customer_address_ctas_rf
SELECT *
FROM read_files(
  '/Volumes/learning/sales/raw/CustomerAddress.csv',
  format => 'csv',
  header => true
);

-- visualizar tabela
SELECT *
FROM customer_address_ctas_rf
LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,Descrição da tabela
-- visualizar nome das colunas, tipo de dados e metadados adicionais da tabela
DESCRIBE TABLE EXTENDED customer_address_ctas_rf;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Managed Tables vs External Tables _(dar uma olhada)_
-- MAGIC - A tabela foi criada dentro do catalago `learning`
-- MAGIC - A linha _Type_ indica que a tabela é Managed
-- MAGIC - A linha _Location_ mostra a localização do armazenamento gerenciado na nuvem
-- MAGIC - A linha _Provider_ especifica que a tabela é delta table

-- COMMAND ----------

DROP TABLE IF EXISTS learning.bronze.customer_address_ctas_rf

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Ingestão de dados com python

-- COMMAND ----------

-- DBTITLE 1,Criar tabela com python
-- MAGIC %python
-- MAGIC # 1. Ler o arquivo CSV do volume para Spark DataFrame
-- MAGIC df = (spark
-- MAGIC       .read.
-- MAGIC       format("csv")
-- MAGIC       .option("header", "true")        #primeira linha como nome das colunas
-- MAGIC       .option("inferSchema", "true")   # infere os tipos (opcional, mas recomendado)
-- MAGIC       .load("/Volumes/learning/sales/raw/CustomerAddress.csv")
-- MAGIC )
-- MAGIC
-- MAGIC # 2. Escrever o dataframe em uma tabela delta (sobrescreve a tabela se ela já existir)
-- MAGIC (df
-- MAGIC  .write
-- MAGIC .mode("overwrite")
-- MAGIC .saveAsTable(f"learning.bronze.custumer_address_with_python")
-- MAGIC )
-- MAGIC
-- MAGIC # 3. Visualizar a tabela
-- MAGIC display(spark.table(f"learning.bronze.custumer_address_with_python"))

-- COMMAND ----------

DROP TABLE IF EXISTS learning.bronze.custumer_address_with_python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Ingestão incremental de dados com COPY INTO
-- MAGIC [COPY INTO](https://learn.microsoft.com/pt-br/azure/databricks/ingestion/cloud-object-storage/copy-into/)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###COPY INTO schema mismatch

-- COMMAND ----------

-- DBTITLE 1,COPY INTO com evolução de schema
DROP TABLE IF EXISTS learning.bronze.customer_address_ci;

--Criar tabela vazia
CREATE TABLE learning.bronze.customer_address_ci (
  CustomerID string,
  AddressID string,
  AddressType string,
  rowguid string
);

/* FORMA ERRADA (QUANTIDADE DE COLUNAS NA TABELA CRIADA É DIFERENTE DA QUANTIDADE DE COLUNAS NO ARQUIVO)
--Copy Into para ingerir dados
COPY INTO learning.bronze.customer_address_ci
  FROM '/Volumes/learning/sales/raw/CustomerAddress.csv'
  FILEFORMAT = csv; */

--Forma correta
COPY INTO learning.bronze.customer_address_ci
  FROM '/Volumes/learning/sales/raw/CustomerAddress.csv'
  FILEFORMAT = csv
  FORMAT_OPTIONS ('header' = 'true')
  COPY_OPTIONS ('mergeSchema' = 'true'); --merge the schema of each file


-- COMMAND ----------

-- DBTITLE 1,Visualizar dados na tabela criada
SELECT *
FROM learning.bronze.customer_address_ci
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Lidando com evolução de esquema preventivamente

-- COMMAND ----------

-- DBTITLE 1,COPY INTO with schema evolution
DROP TABLE IF EXISTS learning.bronze.customer_address_ci_no_schema;

--tabela criada sem especificar schema
CREATE TABLE learning.bronze.customer_address_ci_no_schema;

--copiar os dados para delta table
COPY INTO learning.bronze.customer_address_ci_no_schema
  FROM '/Volumes/learning/sales/raw/CustomerAddress.csv'
  FILEFORMAT = csv
  FORMAT_OPTIONS ('header' = 'true')
  COPY_OPTIONS ('mergeSchema' = 'true'); --merge the schema of each file



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Idempotência (Ingestão incremental)
-- MAGIC > Idempotência no Databricks é a propriedade de um pipeline ou operação de dados que produz o mesmo resultado final, independentemente de quantas vezes seja executada com a mesma entrada. Essencial para a resiliência, ela evita duplicidade de dados e inconsistências, garantindo que reprocessamentos (retry) não criem efeitos colaterais. 
-- MAGIC
-- MAGIC **COPY INTO** rastreia os arquivos que foram inseridos anteriormente. Se o comando rodar de novo, nenhum dado adicional é inserido porque os arquivos no diretório de origem não mudaram

-- COMMAND ----------

COPY INTO customer_address_ci_no_schema
  FROM '/Volumes/learning/sales/raw/CustomerAddress.csv'
  FILEFORMAT = csv
  FORMAT_OPTIONS ('header' = 'true')
  COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------

DROP TABLE IF EXISTS learning.bronze.customer_address_ci_no_schema;
DROP TABLE IF EXISTS learning.bronze.customer_address_ci;