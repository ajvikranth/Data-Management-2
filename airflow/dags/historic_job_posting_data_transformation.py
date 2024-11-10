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

with DAG(dag_id="Historic_Job_Posting_Data_Transformation",
         start_date=datetime(2024,1,1),
         schedule_interval=None,
         catchup=False) as dag:

    dbt = DockerOperator(
        task_id='dbt_transformation',
        image="ghcr.io/dbt-labs/dbt-bigquery:1.8.2",
        container_name='aj-dbt',
        api_version='auto',
        auto_remove=True,
        entrypoint = "/bin/bash",
        mounts = [Mount(
            source="/home/astro/projects/Data-Management-2/airflow/dbt", target="/usr/app/dbt", type="bind"),],
        working_dir = "/usr/app/dbt/job_desc_transform",
        command="-c 'dbt run --select stage_table_historic -t historic --profiles-dir /usr/app/dbt/'", 
        docker_url="tcp://docker-proxy:2375",
        network_mode="host",
        mount_tmp_dir = False
        )