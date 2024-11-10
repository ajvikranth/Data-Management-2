import json
from datetime import datetime

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from docker.types import Mount

from google.api_core.retry import Retry
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)


DATASET_NAME = 'historic_job_postings'
TABLE_NAME_JSON = 'json_load'
BUCKET_PATH = "gs://historical-job-postings/"
OBJECT_NAME = "techmap-jobs-export-2020-10_ie.json"

PROJECT_ID = "data-management-2-440220"
REGION = "us-east1"
CLUSTER_NAME = "spark-dm2"
GCS_JOB_FILE = "gs://historical-job-postings/process_historic_data.py"

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "gce_cluster_config": {
        "internal_ip_only": False,
    }
    }

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE},
}


with DAG(dag_id="Historic_Job_Posting_Data_Management",
         start_date=datetime(2024,1,1),
         schedule_interval=None,
         catchup=False) as dag:
    
    create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    project_id=PROJECT_ID,
    cluster_config=CLUSTER_CONFIG,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    retry=Retry(maximum=100.0, initial=10.0, multiplier=1.0),
    )

    pyspark_task = DataprocSubmitJobOperator(
    task_id="pyspark_task", job=PYSPARK_JOB, region=REGION, project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )


    create_cluster >> pyspark_task >> delete_cluster





    
        
    # load_json = BigQueryInsertJobOperator(
    #     task_id='gcs_to_bigquery',
    #     configuration={
    #         "load": {
    #             "sourceUris": [f"{BUCKET_PATH}{OBJECT_NAME}",],
    #             "destinationTable": {
    #                 "projectId": "data-management-2-440220",
    #                 "datasetId": DATASET_NAME,
    #                 "tableId": TABLE_NAME_JSON
    #             },
    #             "schema_object_bucket":"historical-job-postings",
    #             "schema_object": "schema.json",
    #             "source_format": "NEWLINE_DELIMITED_JSON",
    #             "writeDisposition": "WRITE_TRUNCATE",
    #             "autodetect": False,
    #             "deferrable": True,
    #             "create_disposition":"CREATE_IF_NEEDED"
    #         }
    #     },
    #     gcp_conn_id="google_cloud_default",
    # )

    # load_json.template_ext = () 


   