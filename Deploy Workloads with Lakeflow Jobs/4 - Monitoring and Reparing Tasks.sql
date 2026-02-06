-- Databricks notebook source
SELECT * 
FROM learning.gold.customer_address_california_gold

-- COMMAND ----------

select *
from learning.gold.customer_address_nevada_gold

-- COMMAND ----------

select *
from learning.gold.customer_address_new_mexico_gold

-- COMMAND ----------

SHOW SCHEMAS IN system

-- COMMAND ----------

SHOW TABLES IN system.lakeflow

-- COMMAND ----------

-- DBTITLE 1,Job and Task Run Timeline Query
SELECT 
    jobs.workspace_id,
    jobs.name as job_name,
    jobs.job_id,
    timeline.period_start_time,
    timeline.period_end_time,
    timeline.task_key,
    timeline.result_state
FROM system.lakeflow.jobs as jobs
INNER JOIN
  system.lakeflow.job_task_run_timeline as timeline
ON
  jobs.job_id = timeline.job_id
ORDER BY timeline.period_start_time

-- COMMAND ----------

-- MAGIC %md
-- MAGIC https://docs.databricks.com/aws/en/admin/system-tables/
-- MAGIC https://docs.databricks.com/gcp/en/admin/system-tables/compute