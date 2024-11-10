{{ config(
    materialized='table'
) }}

with transform as (
select 
t.id,
t.posted_date._date._numberLong as `posted_time_stamp`,
t.orgCompany.name as `company_name`,
t.orgAddress.city as  `job_city`,
t.orgCompany.nameOrg as `org_name`,
t.orgTags.CATEGORIES as `job_category`,
t.orgTags.INDUSTRIES as `industry`,
t.orgTags.JOBNAMES as `job_name`,
t.orgTags.REQUIREMENTS as `job_requirements`,
t.orgTags.SKILLS as `job_skills`,
t.position.careerLevel as `career_level`,
t.position.department as `department`,
t.position.workType as `work_type`,
t.salary.text as `salary_in_text`,
t.salary.value._numberDouble as `salary`,
t.source as `job_source`,
t.text as `job_description`

from `historic_job_postings.json_load2020` as t

union all

select 
t.id,
t.posted_date._date._numberLong as `posted_time_stamp`,
t.orgCompany.name as `company_name`,
t.orgAddress.city as  `job_city`,
t.orgCompany.nameOrg as `org_name`,
t.orgTags.CATEGORIES as `job_category`,
t.orgTags.INDUSTRIES as `industry`,
t.orgTags.JOBNAMES as `job_name`,
t.orgTags.REQUIREMENTS as `job_requirements`,
t.orgTags.SKILLS as `job_skills`,
t.position.careerLevel as `career_level`,
t.position.department as `department`,
t.position.workType as `work_type`,
t.salary.text as `salary_in_text`,
t.salary.value._numberDouble as `salary`,
t.source as `job_source`,
t.text as `job_description`

from `historic_job_postings.json_load2021` as t

union all 

select 
t.id, #
t.posted_date._date._numberLong as `posted_time_stamp`, 
t.orgCompany.name as `company_name`, 
t.orgAddress.city as  `job_city`, 
t.orgCompany.nameOrg as `org_name`, 
t.orgTags.CATEGORIES as `job_category`,
t.orgTags.INDUSTRIES as `industry`,
t.orgTags.JOBNAMES as `job_name`,
t.orgTags.REQUIREMENTS as `job_requirements`,
t.orgTags.SKILLS as `job_skills`,
t.position.careerLevel as `career_level`,
t.position.department as `department`,
t.position.workType as `work_type`,
t.salary.text as `salary_in_text`,
t.salary.value._numberDouble as `salary`,
t.source as `job_source`,
t.text as `job_description`

from `historic_job_postings.json_load2022` as t
)

select
ROW_NUMBER() OVER () AS id,
TIMESTAMP_MILLIS(cast(tf.`posted_time_stamp` as int64)) as `posted_time_stamp`,
tf.`company_name`,
tf.job_city,
job as `job_name`,
job_c as `job_category`,
job_i as `job_industry`,
job_r as `job_requirements`,
job_s as `job_skills`,
tf.`org_name`,
tf.`career_level`,
tf.`department`,
tf.`work_type`,
tf.`salary_in_text`,
tf.`salary`,
tf.`job_source`,
tf.`job_description`
from transform as tf
LEFT JOIN UNNEST(IFNULL(tf.job_name, [])) AS job
LEFT JOIN UNNEST(IFNULL(tf.job_category, [])) AS job_c
LEFT JOIN UNNEST(IFNULL(tf.industry, [])) AS job_i
LEFT JOIN UNNEST(IFNULL(tf.job_requirements, [])) AS job_r
LEFT JOIN UNNEST(IFNULL(tf.job_skills, [])) AS job_s

