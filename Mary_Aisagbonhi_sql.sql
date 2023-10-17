-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables")
-- MAGIC #check that the clinical trial and pharma file exist in DBFS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC filepath = "/FileStore/tables/clinicaltrial_2021.csv"
-- MAGIC clinicaltrial_2021DF = spark.read.options(delimiter ="|", header = True, inferSchema = True).csv(filepath)
-- MAGIC clinicaltrial_2021DF.show(5, truncate = False)
-- MAGIC # create a dataframe for the clinical trial file using spark.read.options

-- COMMAND ----------

-- MAGIC %python
-- MAGIC filepath1 = "/FileStore/tables/pharma.csv"
-- MAGIC pharmaDF = spark.read.options(delimiter =",", header = True, inferSchema = True).csv(filepath1)
-- MAGIC pharmaDF.show(5, truncate = False)
-- MAGIC # create a dataframe for the pharma file using spark.read.options

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_2021DF.createOrReplaceTempView ("clinicaltrial_2021_tempView")
-- MAGIC # create a temporary table from clinical trial data to write SQL queries with

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharmaDF.createOrReplaceTempView ("pharma_tempView")
-- MAGIC # create a temporary table from pharma data to write SQL queries with

-- COMMAND ----------

-- returm the first ten rows of the clinical trial data
SELECT * FROM clinicaltrial_2021_tempView LIMIT 10

-- COMMAND ----------

-- return the first ten rows of the clinical trial data
SELECT * FROM pharma_tempView LIMIT 10

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS BDTT_sql_assessment_db

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

--create a table in the database and copy the clinical temp view to it
CREATE OR REPLACE TABLE BDTT_sql_assessment_db.clinicaltrial_2021 AS SELECT * FROM clinicaltrial_2021_tempView

-- COMMAND ----------

--create a table in the database and copy the pharma temp view to it
CREATE OR REPLACE TABLE BDTT_sql_assessment_db.pharma AS SELECT * FROM pharma_tempView

-- COMMAND ----------

-- check the tables in the database
SHOW TABLES IN BDTT_sql_assessment_db

-- COMMAND ----------

-- find the distinct studies in clinical trial data set and store in a view
CREATE OR REPLACE VIEW BDTT_sql_assessment_db.study_count AS 
SELECT COUNT(DISTINCT Id) studies_count 
FROM BDTT_sql_assessment_db.clinicaltrial_2021

-- COMMAND ----------

-- select the view to find out the study count
SELECT * FROM BDTT_sql_assessment_db.study_count

-- COMMAND ----------

-- find and count the study types in clinical trial data set and store in a view
CREATE OR REPLACE VIEW BDTT_sql_assessment_db.study_types AS 
SELECT Type, COUNT(Type) AS Study_Types  
FROM BDTT_sql_assessment_db.clinicaltrial_2021
GROUP BY Type
ORDER BY Study_Types Desc

-- COMMAND ----------

SELECT * FROM BDTT_sql_assessment_db.study_types

-- COMMAND ----------

-- use the explode and split funtions, group by and order by clause to get the top 5 conditions
CREATE OR REPLACE VIEW BDTT_sql_assessment_db.Top_Conditions AS 
SELECT exploded_cdn AS Conditions, COUNT(exploded_cdn) AS conditions_count 
FROM (
    SELECT Id, explode(split(Conditions, ',')) AS exploded_cdn 
    FROM BDTT_sql_assessment_db.clinicaltrial_2021
) AS split_conditions
GROUP BY exploded_cdn
ORDER BY conditions_count DESC
LIMIT 5;

-- COMMAND ----------

-- view the top 5 conditions
SELECT * FROM BDTT_sql_assessment_db.Top_conditions

-- COMMAND ----------

-- use left join, where clause and is null function to combine the pharma and clinical trial data and return only non pharmaceutical sponsors in inner query
-- use group by and order by to group and order the non pharmaceutical sponsors in outer query 

CREATE OR REPLACE VIEW BDTT_sql_assessment_db.Top_10_sponsors  AS
SELECT Sponsor, count(Sponsor) AS Sponsored_trials 
FROM (SELECT * FROM BDTT_sql_assessment_db.clinicaltrial_2021 
LEFT JOIN BDTT_sql_assessment_db.pharma ON clinicaltrial_2021.Sponsor = pharma.Parent_Company 
WHERE  pharma.Parent_Company is null) AS top_ten_sponsors
GROUP BY Sponsor
ORDER BY Sponsored_trials desc
LIMIT 10


-- COMMAND ----------

SELECT * FROM BDTT_sql_assessment_db.Top_10_sponsors

-- COMMAND ----------

-- use the to date format, group by, where and order by clause to create view of 2021 studies
CREATE OR REPLACE TABLE BDTT_sql_assessment_db.completed_studies_2021 AS 
SELECT DATE_FORMAT(TO_DATE(Completion, 'MMM yyyy'), 'MMM') AS Completion_month, 
       COUNT(Completion) AS no_of_completed_studies_by_month 
FROM BDTT_sql_assessment_db.clinicaltrial_2021
WHERE Status LIKE 'Completed' 
  AND Completion LIKE '%2021%'
GROUP BY TO_DATE(Completion, 'MMM yyyy')
ORDER BY TO_DATE(Completion, 'MMM yyyy');


-- COMMAND ----------

-- view the completed studies
SELECT * FROM BDTT_sql_assessment_db.completed_studies_2021

-- COMMAND ----------

-- use the explode and split funtions, group by and order by clause to get the top 5 interventions
CREATE OR REPLACE VIEW BDTT_sql_assessment_db.Top_Interventions AS 
SELECT exploded_interventions AS Interventions, COUNT(exploded_interventions) AS interventions_count 
FROM (
    SELECT Id, explode(split(Interventions, ',')) AS exploded_interventions
    FROM BDTT_sql_assessment_db.clinicaltrial_2021
) AS split_interventions
GROUP BY exploded_interventions
ORDER BY interventions_count DESC
LIMIT 5;

-- COMMAND ----------

-- view the top interventions and count
SELECT * FROM BDTT_sql_assessment_db.Top_Interventions
