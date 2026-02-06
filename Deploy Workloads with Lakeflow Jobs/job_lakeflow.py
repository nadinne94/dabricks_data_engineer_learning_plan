# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --upgrade databricks-sdk==0.70.0
%restart_python

from databricks.sdk.service.jobs import JobSettings as Job


lakeflow_job = Job.from_dict(
    {
        "name": "lakeflow_job",
        "trigger": {
            "pause_status": "UNPAUSED",
            "file_arrival": {
                "url": "/Volumes/learning/bronze/trigger_storage_location/",
            },
        },
        "tasks": [
            {
                "task_key": "ingestion_address",
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/nadinne.cavalcante94@gmail.com/Deploy Workloads with Lakeflow Jobs/1.3.Criando tabela Address",
                    "source": "WORKSPACE",
                },
                "environment_key": "Default",
            },
            {
                "task_key": "ingestion_customer",
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/nadinne.cavalcante94@gmail.com/Deploy Workloads with Lakeflow Jobs/1.1.Criando tabela Customer",
                    "source": "WORKSPACE",
                },
                "environment_key": "Default",
            },
            {
                "task_key": "ingestion_customer_address",
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/nadinne.cavalcante94@gmail.com/Deploy Workloads with Lakeflow Jobs/2.1 Criar tabela CustomerAddress",
                    "base_parameters": {
                        "catalog": "learning",
                        "schema": "bronze",
                    },
                    "source": "WORKSPACE",
                },
                "environment_key": "Default",
            },
            {
                "task_key": "customer_address_report",
                "depends_on": [
                    {
                        "task_key": "ingestion_address",
                    },
                    {
                        "task_key": "ingestion_customer",
                    },
                    {
                        "task_key": "ingestion_customer_address",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/nadinne.cavalcante94@gmail.com/Deploy Workloads with Lakeflow Jobs/3.2 Joing Customer and Address",
                    "source": "WORKSPACE",
                },
                "environment_key": "Default",
            },
            {
                "task_key": "customer_address_state_wise_iterator",
                "depends_on": [
                    {
                        "task_key": "customer_address_report",
                    },
                ],
                "for_each_task": {
                    "inputs": "[\"California\",\"Nevada\",\"New Mexico\"]",
                    "task": {
                        "task_key": "customer_address_state_wise_report",
                        "notebook_task": {
                            "notebook_path": "/Workspace/Users/nadinne.cavalcante94@gmail.com/Deploy Workloads with Lakeflow Jobs/3.5 For each: Customer Address State",
                            "base_parameters": {
                                "state": "{{input}}",
                            },
                            "source": "WORKSPACE",
                        },
                    },
                },
            },
            {
                "task_key": "ingestion_sales_order",
                "sql_task": {
                    "query": {
                        "query_id": "f0ac0a39-860e-4028-9cc3-f4560aa24bcd",
                    },
                    "warehouse_id": "8effccaa6411ee54",
                },
            },
            {
                "task_key": "customer_sales_summary",
                "depends_on": [
                    {
                        "task_key": "ingestion_customer",
                    },
                    {
                        "task_key": "ingestion_customer_address",
                    },
                    {
                        "task_key": "ingestion_sales_order",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/nadinne.cavalcante94@gmail.com/Deploy Workloads with Lakeflow Jobs/3.1 Joining Custumers and Sales_Orders Table",
                    "source": "WORKSPACE",
                },
                "environment_key": "Default",
            },
            {
                "task_key": "checking_for_duplicates",
                "depends_on": [
                    {
                        "task_key": "customer_sales_summary",
                    },
                ],
                "condition_task": {
                    "op": "EQUAL_TO",
                    "left": "{{tasks.customer_sales_summary.values.has_duplicates}}",
                    "right": "true",
                },
            },
            {
                "task_key": "dropping_duplicates",
                "depends_on": [
                    {
                        "task_key": "checking_for_duplicates",
                        "outcome": "true",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/nadinne.cavalcante94@gmail.com/Deploy Workloads with Lakeflow Jobs/3.3 If Condition_ Dropping Duplicates",
                    "source": "WORKSPACE",
                },
                "environment_key": "Default",
            },
            {
                "task_key": "transforming_customer_address_state",
                "depends_on": [
                    {
                        "task_key": "customer_address_state_wise_iterator",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/nadinne.cavalcante94@gmail.com/Deploy Workloads with Lakeflow Jobs/4.1. Transforming Customer Address Table",
                    "source": "WORKSPACE",
                },
                "min_retry_interval_millis": 900000,
                "disable_auto_optimization": True,
            },
            {
                "task_key": "transforming_customer_address_table",
                "depends_on": [
                    {
                        "task_key": "checking_for_duplicates",
                        "outcome": "false",
                    },
                    {
                        "task_key": "dropping_duplicates",
                    },
                ],
                "run_if": "NONE_FAILED",
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/nadinne.cavalcante94@gmail.com/Deploy Workloads with Lakeflow Jobs/3.4 Else Condition_ Cheking and Transforming customer_sales_table",
                    "source": "WORKSPACE",
                },
                "environment_key": "Default",
            },
            {
                "task_key": "refrashing_dashboard",
                "depends_on": [
                    {
                        "task_key": "transforming_customer_address_table",
                    },
                    {
                        "task_key": "transforming_customer_address_state",
                    },
                ],
                "dashboard_task": {
                    "subscription": {
                    },
                    "warehouse_id": "8effccaa6411ee54",
                    "dashboard_id": "01f1037e62a6198395f539a95f3c5c05",
                },
            },
        ],
        "queue": {
            "enabled": True,
        },
        "environments": [
            {
                "environment_key": "Default",
                "spec": {
                    "environment_version": "4",
                },
            },
        ],
        "performance_target": "PERFORMANCE_OPTIMIZED",
    }
)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.jobs.reset(new_settings=lakeflow_job, job_id=684476061917425)
# or create a new job using: w.jobs.create(**lakeflow_job.as_shallow_dict())
