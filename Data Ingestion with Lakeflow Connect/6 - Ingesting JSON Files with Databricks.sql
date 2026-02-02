-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Ingesting JSON Files with Databricks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Visão geral de CTAS com rea_files() para ingestão de arquivos JSON

-- COMMAND ----------

-- DBTITLE 1,Inspecionar json_files
LIST '/Volumes/learning/json_data/raw/yelp_academic_dataset_business.json'

-- COMMAND ----------

-- DBTITLE 1,Visualizar arquivos
SELECT *
FROM json.`/Volumes/learning/json_data/raw/yelp_academic_dataset_business.json`
LIMIT 5;

-- COMMAND ----------

-- DBTITLE 1,Visualizar arquivos em forma de tabela
SELECT *
FROM read_files(
  "/Volumes/learning/json_data/raw/yelp_academic_dataset_business.json",
  format => 'json')
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##CTAS e read_files()

-- COMMAND ----------

-- DBTITLE 1,Criar tabela bronze
DROP TABLE IF EXISTS learning.bronze.business_bronze_raw;

CREATE TABLE learning.bronze.business_bronze_raw
SELECT *
FROM read_files(
  "/Volumes/learning/json_data/raw/yelp_academic_dataset_business.json",
  format => 'json');

SELECT *
FROM learning.bronze.business_bronze_raw
LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,Decodificar string como binario

/*SELECT
  (unbase64(nomecoluna) AS STRING) AS decoded_nome_coluna,
  _c0,
  _c1,
  ...,
  _cN
FROM
  catalago.schema.table*/

-- COMMAND ----------

-- DBTITLE 1,Criar tabela decodificada
/*
CREATE OR REPLACE TABLE catalago.schema.table_decode
SELECT
  (unbase64(nomecoluna) AS STRING) AS decoded_nome_coluna,
  _c0,
  _c1,
  ...,
  _cN
FROM
  catalago.schema.table
LIMIT 10 */

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Formato Json string dentro da tabela
-- MAGIC [Query JSON strings](https://docs.databricks.com/aws/en/semi-structured/json)
-- MAGIC
-- MAGIC [schema_of_json function](https://docs.databricks.com/aws/en/sql/language-manual/functions/schema_of_json)
-- MAGIC
-- MAGIC [to_json function](https://docs.databricks.com/gcp/en/sql/language-manual/functions/to_json)
-- MAGIC
-- MAGIC [from_json function](https://docs.databricks.com/gcp/en/sql/language-manual/functions/from_json)

-- COMMAND ----------

-- DBTITLE 1,schema_of_json
SELECT schema_of_json('{"AcceptsInsurance": null, "AgesAllowed": null, "Alcohol": null, "Ambience": null, "BYOB": null, "BYOBCorkage": null, "BestNights": null, "BikeParking": null, "BusinessAcceptsBitcoin": null, "BusinessAcceptsCreditCards": null, "BusinessParking": null, "ByAppointmentOnly": "True", "Caters": null, "CoatCheck": null, "Corkage": null, "DietaryRestrictions": null, "DogsAllowed": null, "DriveThru": null, "GoodForDancing": null, "GoodForKids": null, "GoodForMeal": null, "HairSpecializesIn": null, "HappyHour": null, "HasTV": null, "Music": null, "NoiseLevel": null, "Open24Hours": null, "OutdoorSeating": null, "RestaurantsAttire": null, "RestaurantsCounterService": null, "RestaurantsDelivery": null, "RestaurantsGoodForGroups": null, "RestaurantsPriceRange2": null, "RestaurantsReservations": null, "RestaurantsTableService": null, "RestaurantsTakeOut": null, "Smoking": null, "WheelchairAccessible": null, "WiFi": null, "AllowsPets": null}') AS schema

-- COMMAND ----------

-- DBTITLE 1,De STRING para STRUCT
/*
CREATE OR REPLACE TABLE learning.bronze.business_bronze_struct AS
SELECT
  * EXCEPT attribute,
  from_json(
    STRUCT<AcceptsInsurance: STRING, AgesAllowed: STRING, Alcohol: STRING, AllowsPets: STRING, Ambience: STRING, BYOB: STRING, BYOBCorkage: STRING, BestNights: STRING, BikeParking: STRING, BusinessAcceptsBitcoin: STRING, BusinessAcceptsCreditCards: STRING, BusinessParking: STRING, ByAppointmentOnly: STRING, Caters: STRING, CoatCheck: STRING, Corkage: STRING, DietaryRestrictions: STRING, DogsAllowed: STRING, DriveThru: STRING, GoodForDancing: STRING, GoodForKids: STRING, GoodForMeal: STRING, HairSpecializesIn: STRING, HappyHour: STRING, HasTV: STRING, Music: STRING, NoiseLevel: STRING, Open24Hours: STRING, OutdoorSeating: STRING, RestaurantsAttire: STRING, RestaurantsCounterService: STRING, RestaurantsDelivery: STRING, RestaurantsGoodForGroups: STRING, RestaurantsPriceRange2: STRING, RestaurantsReservations: STRING, RestaurantsTableService: STRING, RestaurantsTakeOut: STRING, Smoking: STRING, WheelchairAccessible: STRING, WiFi: STRING>) AS attribute
FROM* table/

-- COMMAND ----------

-- DBTITLE 1,Acessar valores coluna STRUCT
SELECT
  attributes.AcceptsInsurance,
  attributes.AgesAllowed,
  attributes.Alcohol,
  attributes.Ambience,
  attributes.BYOB,
  attributes.BYOBCorkage,
  attributes.BestNights,
  attributes.BikeParking,
  attributes.BusinessAcceptsBitcoin,
  attributes.BusinessAcceptsCreditCards,
  attributes.BusinessParking,
  attributes.ByAppointmentOnly,
  attributes.Caters,
  attributes.CoatCheck,
  attributes.Corkage,
  attributes.DietaryRestrictions,
  attributes.DogsAllowed,
  attributes.DriveThru,
  attributes.GoodForDancing,
  attributes.GoodForKids,
  attributes.GoodForMeal,
  attributes.HairSpecializesIn,
  attributes.HappyHour,
  attributes.HasTV,
  attributes.Music,
  attributes.NoiseLevel,
  attributes.Open24Hours,
  attributes.OutdoorSeating,
  attributes.RestaurantsAttire,
  attributes.RestaurantsCounterService,
  attributes.RestaurantsDelivery
FROM learning.bronze.business_bronze_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Extrair dados do array dentro do STRUCT
-- MAGIC - para retonar as linhas com **NULL** `explode_outer()`
-- MAGIC
-- MAGIC [explode_outer](https://docs.databricks.com/gcp/en/sql/language-manual/functions/explode_outerurl) 

-- COMMAND ----------

-- DBTITLE 1,Extrair dados do array dentro do STRUCT
-- extrair dados do array dentro do STRUCT sem considerar nulos
-- se a coluna Ambience fosse um ARRAY por exemplo
/*CREATE OR REPLACE TABLE learning.bronze.business_explode_array AS
SELECT
  attributes,
  array_size(attributes.Ambience) AS number_of_ambience,
  explode(attributes.Ambience) AS ambience_array,
  attributes.Ambience
FROM learning.bronze.business_bronze_raw;

SELECT *
FROM learning.bronze.business_explode_array*/

-- COMMAND ----------

DROP SCHEMA IF EXISTS learning.bronze CASCADE;
CREATE SCHEMA learning.bronze;
