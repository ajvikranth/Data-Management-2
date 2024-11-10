from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

spark = SparkSession.builder \
    .appName("Preprocessing-Historic-Data") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0") \
    .getOrCreate()

input_paths = ["gs://historical-job-postings/processed-data/preprocessed-jobs-export-2022-10.jsonl",
               "gs://historical-job-postings/processed-data/preprocessed-jobs-export-2021-10.jsonl",
               "gs://historical-job-postings/processed-data/preprocessed-jobs-export-2020-10.jsonl"]

year = 2022

for input_path in input_paths:

    df = spark.read.json(input_path)

    preprocessed_df = df.select(
        monotonically_increasing_id().alias("id"),  # Generate a unique ID
        col("source"),
        col("text"),
        col("position"),
        col("salary"),
        col("contact"),
        col("orgAddress"),
        col("orgCompany"),
        col("name"),
        col("orgTags"),
        col("dateCreated").alias("posted_date")
    )

    preprocessed_df.write \
        .format("bigquery") \
        .option("writeMethod", "direct") \
        .mode("overwrite") \
        .save(f"data-management-2-440220.historic_job_postings.json_load{year}")
    year -= 1

spark.stop()

