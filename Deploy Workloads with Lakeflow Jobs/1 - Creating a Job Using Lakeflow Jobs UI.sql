-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Ajustes para acompanhar a aula
-- MAGIC Na aula _DEMO Creating a Job Using Lakeflow Jobs UI_ existem 3 tabelas(raw) como fonte. Então vou criar três tabelas no schema `learning.ingestion_simulation` (onde armazeno os dados para utilizar nas aulas)

-- COMMAND ----------

-- DBTITLE 1,Setup do Ambiente
USE CATALOG learning;
USE learning.ingestion_simulation;

SELECT current_catalog(), current_schema();

-- COMMAND ----------

-- DBTITLE 1,Criando tabela CustomerAddress
DROP TABLE IF EXISTS learning.ingestion_simulation.customer_address_raw;

CREATE TABLE learning.ingestion_simulation.customer_address_raw AS
SELECT *
FROM read_files(
  '/Volumes/learning/ingestion_simulation/raw/CustomerAddress.csv',
  format => 'csv',
  sep => ';',
  header => "true"
);

-- COMMAND ----------

-- DBTITLE 1,Criando tabela Customer
DROP TABLE IF EXISTS learning.ingestion_simulation.customer_raw;

CREATE TABLE learning.ingestion_simulation.customer_raw AS
SELECT *
FROM read_files(
  '/Volumes/learning/ingestion_simulation/raw/Customer.csv',
  format => 'csv',
  sep => ';',
  header => "true"
);

-- COMMAND ----------

DROP TABLE IF EXISTS learning.ingestion_simulation.address_raw;

CREATE TABLE learning.ingestion_simulation.address_raw AS
SELECT *
FROM read_files(
  '/Volumes/learning/ingestion_simulation/raw/Address.csv',
  format => 'csv',
  sep => ';',
  header => "true"
);

-- COMMAND ----------

DROP TABLE IF EXISTS learning.ingestion_simulation.sales_detail_raw;

CREATE TABLE learning.ingestion_simulation.sales_detail_raw AS
SELECT *
FROM read_files(
  '/Volumes/learning/ingestion_simulation/raw/SalesOrderDetail.csv',
  format => 'csv',
  sep => ';',
  header => "true"
);

-- COMMAND ----------

DROP TABLE IF EXISTS learning.ingestion_simulation.sales_header_raw;

CREATE TABLE learning.ingestion_simulation.sales_header_raw AS
SELECT *
FROM read_files(
  '/Volumes/learning/ingestion_simulation/raw/SalesOrderHeader.csv',
  format => 'csv',
  sep => ';',
  header => "true"
);

-- COMMAND ----------

DROP TABLE IF EXISTS learning.ingestion_simulation.product_raw;

CREATE TABLE learning.ingestion_simulation.product_raw AS
SELECT *
FROM read_files(
  '/Volumes/learning/ingestion_simulation/raw/Product.csv',
  format => 'csv',
  sep => ';',
  header => "true"
);

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_order_raw AS
SELECT
  sh.SalesOrderID,
  sh.OrderDate,
  sh.CustomerID,
  p.Name,
  sd.OrderQty,
  sd.UnitPrice,
  sd.LineTotal
FROM sales_header_raw sh
JOIN
  sales_detail_raw sd
ON
  sh.SalesOrderID = sd.SalesOrderID
JOIN 
  product_raw p
ON
  sd.ProductID = p.ProductID

-- COMMAND ----------

select * from sales_order_raw
limit 10

-- COMMAND ----------

-- DBTITLE 1,Cell 11
-- MAGIC %python
-- MAGIC '''df_sales = spark.table("learning.ingestion_simulation.sales_order_raw")'''

-- COMMAND ----------

-- MAGIC %python
-- MAGIC '''from pyspark.sql.functions import regexp_replace
-- MAGIC df_sales = df_sales.withColumn(
-- MAGIC     "LineTotal",
-- MAGIC     regexp_replace("LineTotal", ",", ".").cast("decimal(10,2)")
-- MAGIC )
-- MAGIC df_sales.write.mode("overwrite").saveAsTable("learning.ingestion_simulation.sales_order")'''

-- COMMAND ----------

--select * from learning.ingestion_simulation.sales_order
--limit 10

-- COMMAND ----------

use learning.ingestion_simulation;
select StateProvince, count(*) AS Total
from address_raw
group by StateProvince
order by count(*) desc


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Salvar as tabelas no schema bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create the Job
-- MAGIC - A notebook task
-- MAGIC - A SQL File task

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Limpar e Reiniciar o schema bronze

-- COMMAND ----------

DROP SCHEMA IF EXISTS learning.bronze CASCADE;
CREATE SCHEMA learning.bronze;
DROP SCHEMA IF EXISTS learning.silver CASCADE;
CREATE SCHEMA learning.silver;
DROP SCHEMA IF EXISTS learning.gold CASCADE;
CREATE SCHEMA learning.gold;