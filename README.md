
This project demonstrates an end-to-end cloud-based ELT (Extract, Load, Transform) pipeline for processing job posting datasets. The solution integrates APIs, static datasets, Airflow DAGs, and data warehouse transformations, delivering automated, scalable, and reusable data workflows.

## 🚀 Project Overview
The goal of this project is to automate data ingestion and transformation for job posting analytics using a modern data engineering stack.

Key Features:
  - Automated ELT workflow orchestrated by Apache Airflow
  - Integration with real-time APIs and historic dataset
  - Incremental and batch processing for job posting data
  - Robust transformations including data cleaning, type conversion, and unnesting of nested structures
  - Cloud-based storage and processing for scalability
  - Interactive visualization with Looker

## 📊 Data Sources

| Source Type       | Description                                                  | Example URLs                                 |
| ----------------- | ------------------------------------------------------------ | -------------------------------------------- |
| API               | Real-time job postings data                                  | `https://www.themuse.com/api/public/jobs`    |
| Static Dataset    | Historical job postings data since 2020                      | `https://jobdatafeeds.com/job-data-overview` |
            
## ⚙️ Pipeline Architecture
The project implements two ELT pipelines:

### 1️⃣ Daily Job Posting Pipeline
  - Extracts incremental job postings via API
  - Cleans and transforms data:
      - HTML character removal
      - Datatype conversion
      - Null value imputation
  - Loads data into data warehouse for visualization

***Airflow DAG***: airflow_daily_job_dag.py  
***Key Task***: Incremental materialization for new data  

<p align="center">
  <img src="https://github.com/ajvikranth/Data-Management-2/blob/master/airflow/pipeline1.png" 
       alt="Daily Job Posting Pipeline" 
       style="background-color:white; padding:10px;">
</p> 

### 2️⃣ Historic Job Posting Pipeline
  - Processes bulk historical job posting datasets
  - Formats source into a defined schema and performs:
    - Field renaming and selection
    - Timestamp conversion
    - Array unnesting
    - Left joins with default values
  - Enables backfilling for long-term analytics
***Airflow DAG***: airflow_historic_job_dag.py  
***Key Task***: Full materialization and union of multiple sources  

<p align="center">
  <img src="https://github.com/ajvikranth/Data-Management-2/blob/master/airflow/pipeline2.png" 
       alt="Daily Job Posting Pipeline" 
       style="background-color:white; padding:10px;">
</p> 


## 🏗️ Tech Stack
Orchestration: Apache Airflow  
Data Storage: Cloud Buckets & Data Warehouse (BigQuery / Similar)  
Transformation: SQL-based transformations with incremental & batch models using DBT and PYSPARK  
Visualization: Looker  
Languages & Tools: Python, SQL, JSON  

## 🏁 Project Goals & Outcomes
Goals:
- Automate data ingestion & transformation
- Enable scalable & reusable ELT pipeline
- Deliver clean and analytics-ready datasets
- Facilitate interactive visual analytics via Looker

Outcomes:
- Fully cloud-based ELT pipeline
- Seamless daily & historical job data processing
- Centralized data insights for workforce analytics
