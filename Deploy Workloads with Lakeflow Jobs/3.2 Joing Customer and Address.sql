-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS learning.bronze;
CREATE SCHEMA IF NOT EXISTS learning.silver;

-- COMMAND ----------

USE learning.bronze;

CREATE OR REPLACE TABLE learning.silver.customer_adress_silver AS
SELECT
    ca.CustomerID,
    s.OrderDate,
    s.OrderQty,
    c.FirstName,
    c.LastName,
    c.Phone,
    c.EmailAddress,
    a.AddressLine1,
    a.City,
    a.StateProvince,
    a.CountryRegion
FROM customer_address_bronze ca
JOIN
    learning.bronze.sales_order_bronze s
ON
    ca.CustomerID = s.CustomerID
JOIN
    customer_bronze c
ON
    ca.CustomerID = c.CustomerID
JOIN
    address_bronze a
ON
    ca.AddressID = a.AddressID
