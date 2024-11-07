import os
import json
import time
from requests import Response
from datetime import datetime
from dotenv import load_dotenv

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from docker.types import Mount


load_dotenv()
muse_api_key = os.environ.get('MUSE_API_KEY')


def save_to_gcs(**context):
        # Get the API response from the previous task
        api_response = context['task_instance'].xcom_pull(task_ids="load_api_data")

        # Define the bucket and destination filename
        gcs_bucket = 'daily-job-postings'
        current_date = datetime.now().strftime('%Y-%m-%d')
        gcs_path = f"data_{current_date}.json"

        # Initialize the GCS Hook
        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")

        # Convert the response to JSON and upload to GCS
        gcs_hook.upload(
            bucket_name=gcs_bucket,
            object_name=gcs_path,
            data=json.dumps(api_response),
            mime_type='application/json'
        )

def paginate(response: Response) -> dict:
    content = response.json()

    page_count: int = content['page_count']
    page: int = content['page']

    print(f"page_count {page_count} - page {page}")

    if page < page_count and page <= 1:
        time.sleep(2)
        return dict(data={"location":"ireland",
                          "page": page + 1,
                          "api_key": muse_api_key})

with DAG(dag_id="api_external",
         start_date=datetime(2023,1,1),
         schedule_interval="0 * * * *",
         catchup=False) as dag:
    
    load_api_data = HttpOperator(
    task_id="load_api_data",
    method="GET",
    http_conn_id="muse_api",
    endpoint="/api/public/jobs",
    data={"location":"ireland",
          "page": 1,
          "api_key": muse_api_key},
    pagination_function=paginate,
    log_response=True,
    do_xcom_push=True,
    )
    
    save_to_gcs = PythonOperator(
        task_id="save_to_gcs",
        python_callable=save_to_gcs,
        provide_context=True,
    )

    dbt = DockerOperator(
        task_id='docker_command_sleep',
        image="ghcr.io/dbt-labs/dbt-bigquery:1.8.2",
        container_name='aj-dbt',
        api_version='auto',
        auto_remove=True,
        entrypoint = "/bin/bash",
        mounts = [Mount(
            source="/home/astro/projects/Data-Management-2/airflow/dbt", target="/usr/app/dbt", type="bind"),],
        working_dir = "/usr/app/dbt/job_desc_transform",
        command="-c 'dbt debug --profiles-dir /usr/app/dbt/'", 
        docker_url="tcp://docker-proxy:2375",
        network_mode="host",
        mount_tmp_dir = False
        )

#defining the flow 
    load_api_data >> save_to_gcs >> dbt
