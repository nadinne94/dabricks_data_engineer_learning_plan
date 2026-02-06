# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# Reading the data from each state
df_california = spark.read.table('learning.silver.customer_address_california_silver')
df_nevada = spark.read.table('learning.silver.customer_address_nevada_silver')
df_new_mexico = spark.read.table('learning.silver.customer_address_new_mexico_silver')

# COMMAND ----------

# --- COMMON CLEANING AND STANDARDIZATION ---
def clean_common(df):
  df = df.toDF(*[c.strip().lower() for c in df.columns])

  '''## Intentionally used wrong column name so that task will fail
  df = df.withColumn("Name", F.initcap(F.trim(F.col("Name"))))'''

  #Correct Code
  df = df.withColumn("firstname", F.initcap(F.trim(F.col("firstname"))))

  return df

# COMMAND ----------

# --- CALIFORNIA TRANSFORMATION ---
def prep_ca(df_california):
  df = clean_common(df_california)
  
  #fill missing cities with unkown
  df = df.withColumn("city", F.when(F.col("city").isNull(), F.lit("Unknown")).otherwise(F.col("City")))
  
  #extract order month 
  df = df.withColumn("ordermonth", F.date_format(F.col("orderdate"), "MM"))
  
  #Custom: Add column for Southern CA city marker
  so_cal_cities = ["Anaheim", "Bakersfield","Los Angeles", "San Diego" , "Long Beach"]
  df = df.withColumn(
      "is_southern_ca", 
      F.when(F.col("city").isin(so_cal_cities), F.lit("Yes")).otherwise(F.lit("No"))
  )  
  return df

# COMMAND ----------

# --- NEVADA TRANSFORMATION ---
def prep_nv(df_nevada):
  df = clean_common(df_nevada)
  
  #fill missing cities with Unspecified
  df = df.withColumn("city", F.when(F.col("city").isNull(), F.lit("Unspecified")).otherwise(F.col("city")))

  #Assinging Nevada region group
  nv_east_cities = ["Las Vegas", "Reno", "Carson City"]
  nv_west_cities = ["Pahrump", "Tonopah", "Mesquite"]
  df = df.withColumn(
      "nv_region", 
      F.when(F.col("city").isin([c for c in nv_east_cities]), F.lit("east")).otherwise(F.when(F.col("city").isin([c for c in nv_west_cities]), F.lit("west")).otherwise(F.lit("central")))
  )

  #extract order week 
  df = df.withColumn("orderdayofweek", F.date_format(F.col("orderdate"), "E"))
  return df

# COMMAND ----------

# --- New Mexico TRANSFORMATION ---
def prep_nm(df_new_mexico):
  df = clean_common(df_new_mexico)

  #fill missingg cities with 'other'
  df = df.withColumn("city", F.when(F.col("city").isNull(), F.lit("other")).otherwise(F.col("city")))
  
  #segment orders by size
  df = df.withColumn("order_size_label", F.when(F.col("is_large_order") == True, F.lit("large")).otherwise(F.lit("small")))

  #extract order day of week
  df = df.withColumn("orderdayOfweek", F.date_format(F.col("orderdate"), "E"))
  
  return df

# COMMAND ----------

# --- Apply transformations ---
df_california_clean = prep_ca(df_california)
df_nevada_clean = prep_nv(df_nevada)
df_new_mexico_clean = prep_nm(df_new_mexico)

# --- write to tables ---
df_california_clean.write.mode("overwrite").saveAsTable("learning.gold.customer_address_california_gold")
df_nevada_clean.write.mode("overwrite").saveAsTable("learning.gold.customer_address_nevada_gold")
df_new_mexico_clean.write.mode("overwrite").saveAsTable("learning.gold.customer_address_new_mexico_gold")