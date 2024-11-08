{{ config(
    materialized='incremental',
    unique_key='publication_date'
) }}

WITH new_data AS (
    SELECT
      json_load.id AS job_id,
      json_load.publication_date,
      category.name AS category_name,
      json_load.company.name AS company_name,
      levels.name AS level_name,
      locations.name AS location_name,
      json_load.name AS job_name,
      json_load.contents AS job_description
    FROM
      `daily_job_postings.json_load` AS json_load,
      UNNEST(json_load.categories) AS category,
      UNNEST(json_load.levels) AS levels,
      UNNEST(json_load.locations) AS locations
)


SELECT * 
FROM new_data
{% if is_incremental() %}

WHERE publication_date NOT IN 
(
    SELECT publication_date FROM {{ this }}
)
{% endif %}

