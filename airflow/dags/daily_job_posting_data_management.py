import os
import json
import time
from requests import Response
from datetime import datetime
from dotenv import load_dotenv

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from docker.types import Mount


load_dotenv()
muse_api_key = os.environ.get('MUSE_API_KEY')

DATASET_NAME = 'daily_job_postings'
TABLE_NAME_JSON = 'json_load'
BUCKET_PATH = "gs://daily-job-postings/"
current_date = datetime.now().strftime('%Y-%m-%d')
OBJECT_NAME = f"data_{current_date}.json"

def pre_process_json(api_response):
    data =''
    for page in api_response:
         page = json.loads(page)
         results = page.get('results', [])
         for record in results:
              data += json.dumps(record) + "\n"

    return data


def save_to_gcs(**context):
        # Get the API response from the previous task
        api_response = context['task_instance'].xcom_pull(task_ids="load_api_data")

        # Define the bucket and destination filename
        gcs_bucket = 'daily-job-postings'

        # Initialize the GCS Hook
        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")

        # Convert the response to JSON and upload to GCS
        gcs_hook.upload(
            bucket_name=gcs_bucket,
            object_name=OBJECT_NAME,
            data= pre_process_json(api_response),
            mime_type='application/json'
        )

def paginate(response: Response) -> dict:
    content = response.json()

    page_count: int = content['page_count']
    page: int = content['page']

    print(f"page_count {page_count} - page {page}")

    if page < page_count and page < 99:
        time.sleep(1)
        return dict(data={"location":"ireland",
                          "page": page + 1,
                          "api_key": muse_api_key})

with DAG(dag_id="Daily_Job_Posting_Data_Management",
         start_date=datetime(2024,1,1),
         schedule_interval="@daily",
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
    
    load_json = BigQueryInsertJobOperator(
        task_id='gcs_to_bigquery',
        configuration={
            "load": {
                "sourceUris": [f"gs://daily-job-postings/{OBJECT_NAME}",],
                "destinationTable": {
                    "projectId": "data-management-2-440220",
                    "datasetId": DATASET_NAME,
                    "tableId": TABLE_NAME_JSON
                },
                "source_format": "NEWLINE_DELIMITED_JSON",
                "writeDisposition": "WRITE_TRUNCATE",
                "autodetect": True,
                "deferrable": True,
                "create_disposition":"CREATE_IF_NEEDED"
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    load_json.template_ext = () 

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
        command="-c 'dbt run --select stage_table -t daily --profiles-dir /usr/app/dbt/'", 
        docker_url="tcp://docker-proxy:2375",
        network_mode="host",
        mount_tmp_dir = False
        )

#defining the flow 
    load_api_data >> save_to_gcs >> load_json >> dbt
