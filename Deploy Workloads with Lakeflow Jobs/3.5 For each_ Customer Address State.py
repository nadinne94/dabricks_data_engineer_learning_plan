# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS learning.silver;
# MAGIC USE learning.silver
# MAGIC

# COMMAND ----------

#getting state code from widget
state = dbutils.widgets.get("state")
print(f"Running for state: {state}")

# COMMAND ----------

# DBTITLE 1,Reading and transforming data from master customer_address_table
#Reading and transformagin data from master customer_address_table
##Adding is_large_order column

df = spark.sql(f"""
                SELECT *,
                    CASE WHEN OrderQty > 2 THEN 'Y' ELSE 'N' END AS is_large_order
                FROM learning.silver.customer_adress_silver
                WHERE StateProvince = '{state}'
            """)

# Save to Delta table, one per state specified in the loop
table_name = f"customer_address_{state.replace(' ', '_').lower()}_silver"
df.write.mode("overwrite").saveAsTable(table_name)
