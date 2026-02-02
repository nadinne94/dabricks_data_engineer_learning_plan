# Databricks notebook source
# MAGIC %md
# MAGIC #Auto Loader with Python

# COMMAND ----------

# MAGIC %md
# MAGIC [What is Auto Loader?](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
# MAGIC
# MAGIC [Using Auto Loader with Unity Catalog](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/unity-catalog)
# MAGIC
# MAGIC [Auto Loader options](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options)

# COMMAND ----------

import os
import shutil
import random

def copy_files(copy_from, copy_to, n=1):
    os.makedirs(copy_to, exist_ok=True)

    files = [
        f for f in os.listdir(copy_from)
        if os.path.isfile(os.path.join(copy_from, f))
    ]

    files_to_copy = random.sample(files, min(n, len(files)))

    for f in files_to_copy:
        shutil.copy(
            os.path.join(copy_from, f),
            os.path.join(copy_to, f)
        )

    print(f"Copied {len(files_to_copy)} file(s) to {copy_to}")


# COMMAND ----------

spark.sql(f'CREATE TABLE IF NOT EXISTS learning.bronze.python_auto_loader')

# COMMAND ----------

spark.sql(f'CREATE VOLUME IF NOT EXISTS learning.bronze.auto_loader_source')

# COMMAND ----------

# DBTITLE 1,criar volume
## criar volume para armazenar the Auto Loader checkpoint files
spark.sql(f'CREATE VOLUME IF NOT EXISTS learning.bronze.auto_loader_files')

# COMMAND ----------

# DBTITLE 1,Auto Loader with python
##set checkpoint location
checkpoint_file_location = f'/Volumes/learning/bronze/auto_loader_files'

##incrementally (or stream) data using auto loader
(spark
    .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("sep", ",")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", f"{checkpoint_file_location}")
        .load(f"/Volumes/learning/bronze/auto_loader_source")
    .writeStream
        .option("checkpointLocation", f"{checkpoint_file_location}")
        .option("mergeSchema", "true")
        .trigger(once=True)
        .toTable(f"learning.bronze.python_auto_loader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM learning.bronze.python_auto_loader

# COMMAND ----------

copy_files(copy_from = '/Volumes/learning/sales/raw/ingest_simulation/', copy_to = "/Volumes/learning/bronze/auto_loader_source", n=2)

# COMMAND ----------

spark.sql(f'LIST "/Volumes/learning/bronze/auto_loader_source"').display()

# COMMAND ----------

##set checkpoint location
checkpoint_file_location = f'/Volumes/learning/bronze/auto_loader_files'

##incrementally (or stream) data using auto loader
(spark
    .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("sep", ",")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", f"{checkpoint_file_location}")
        .load(f"/Volumes/learning/bronze/auto_loader_source")
    .writeStream
        .option("checkpointLocation", f"{checkpoint_file_location}")
        .option("mergeSchema", "true")
        .trigger(once=True)
        .toTable(f"learning.bronze.python_auto_loader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM learning.bronze.python_auto_loader

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY learning.bronze.python_auto_loader

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS learning.bronze.python_auto_loader;
# MAGIC DROP VOLUME IF EXISTS learning.bronze.auto_loader_files;
# MAGIC DROP VOLUME IF EXISTS learning.bronze.auto_loader_source;