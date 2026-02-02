-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##CRIANDO AS TABELAS PARA USAR MARGE INTO

-- COMMAND ----------

SELECT *
FROM read_files(
  "/Volumes/learning/ingestion_simulation/raw/Address.csv",
  format => "csv",  
  sep => ";",
  header => "true"
)

-- COMMAND ----------

DROP TABLE IF EXISTS learning.bronze.address_marge_target;

CREATE TABLE learning.bronze.address_marge_target AS
SELECT *
FROM read_files(
  "/Volumes/learning/ingestion_simulation/raw/Address.csv",
  format => "csv", 
  sep => ";",
  header => "true"
) LIMIT 10;

SELECT *

FROM learning.bronze.address_marge_target

-- COMMAND ----------

DROP TABLE IF EXISTS learning.bronze.address_marge_source;

CREATE TABLE learning.bronze.address_marge_source AS
SELECT *
FROM read_files(
  "/Volumes/learning/ingestion_simulation/raw/Address.csv",
  format => "csv", 
  sep => ";",
  header => "true"
) LIMIT 10;


SELECT *
FROM learning.bronze.address_marge_target

-- COMMAND ----------

CREATE TABLE learning.bronze.address_new_client AS
SELECT *
FROM read_files(
  "/Volumes/learning/ingestion_simulation/raw/Address.csv",
  format => "csv", 
  sep => ";",
  header => "true"
)
ORDER BY AddressID DESC 
LIMIT 10;

SELECT *
FROM learning.bronze.address_new_client

-- COMMAND ----------

-- DBTITLE 1,Cell 4
-- Update AddressLine2 values for the first 10 rows
UPDATE learning.bronze.address_marge_source
SET AddressLine2 = CASE 
  WHEN AddressID = (SELECT AddressID FROM learning.bronze.address_marge_source ORDER BY AddressID LIMIT 1 OFFSET 0) THEN '6388 Lake City Way'
  WHEN AddressID = (SELECT AddressID FROM learning.bronze.address_marge_source ORDER BY AddressID LIMIT 1 OFFSET 1) THEN NULL
  WHEN AddressID = (SELECT AddressID FROM learning.bronze.address_marge_source ORDER BY AddressID LIMIT 1 OFFSET 2) THEN '26910 Indela Road'
  WHEN AddressID = (SELECT AddressID FROM learning.bronze.address_marge_source ORDER BY AddressID LIMIT 1 OFFSET 3) THEN NULL
  WHEN AddressID = (SELECT AddressID FROM learning.bronze.address_marge_source ORDER BY AddressID LIMIT 1 OFFSET 4) THEN '22580 Free Street'
  WHEN AddressID = (SELECT AddressID FROM learning.bronze.address_marge_source ORDER BY AddressID LIMIT 1 OFFSET 5) THEN NULL
  WHEN AddressID = (SELECT AddressID FROM learning.bronze.address_marge_source ORDER BY AddressID LIMIT 1 OFFSET 6) THEN NULL
  WHEN AddressID = (SELECT AddressID FROM learning.bronze.address_marge_source ORDER BY AddressID LIMIT 1 OFFSET 7) THEN NULL
  WHEN AddressID = (SELECT AddressID FROM learning.bronze.address_marge_source ORDER BY AddressID LIMIT 1 OFFSET 8) THEN '9228 Via Del Sol'
  WHEN AddressID = (SELECT AddressID FROM learning.bronze.address_marge_source ORDER BY AddressID LIMIT 1 OFFSET 9) THEN '10099 Rue de la Paix'
  ELSE AddressLine2
END;

SELECT *
FROM learning.bronze.address_marge_source

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## MARGE INTO
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Tabela base
SELECT *
FROM learning.bronze.address_marge_target

-- COMMAND ----------

-- DBTITLE 1,Tabela com atualizações
SELECT *
FROM learning.bronze.address_marge_source

-- COMMAND ----------

-- DBTITLE 1,Marge into
MERGE INTO learning.bronze.address_marge_target AS tgt
USING learning.bronze.address_marge_source AS src
ON tgt.AddressID == src.AddressID
WHEN MATCHED
  AND tgt.AddressLine2 IS DISTINCT FROM src.AddressLine2
THEN UPDATE SET
  tgt.AddressLine2 = src.AddressLine2
WHEN MATCHED
  AND tgt.AddressLine2 IS NULL
THEN DELETE;


-- COMMAND ----------

SELECT *
FROM learning.bronze.address_marge_target

-- COMMAND ----------

-- DBTITLE 1,Tabela com novos dados
-- AddressID não batem
MERGE INTO learning.bronze.address_marge_target AS tgt
USING learning.bronze.address_new_client AS src
ON tgt.AddressID == src.AddressID
WHEN NOT MATCHED
  THEN INSERT (AddressID, AddressLine1, AddressLine2, City, StateProvince, CountryRegion, PostalCode, rowguid, ModifiedDate)
  VALUES (src.AddressID, src.AddressLine1, src.AddressLine2, src.City, src.StateProvince, src.CountryRegion, src.PostalCode, src.rowguid, src.ModifiedDate);

-- COMMAND ----------

SELECT *
FROM learning.bronze.address_marge_target

-- COMMAND ----------

DESCRIBE HISTORY learning.bronze.address_marge_target