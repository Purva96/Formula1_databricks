-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

describe database extended demo;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

SHOW TABLES in default;

-- COMMAND ----------

Use demo;

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### MANAGED TABLE

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.printSchema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create managed table using python dataframe

-- COMMAND ----------

DROP TABLE IF EXISTS demo.race_results_python;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

select * 
from demo.race_results_python
where race_year = 2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create managed table using SQL

-- COMMAND ----------

--create table as statement

create Table demo.race_results_sql
AS 
select * 
from demo.race_results_python
where race_year = 2020;

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####External Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create external table using python

-- COMMAND ----------

DROP TABLE IF EXISTS demo.race_results_ext_py

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("delta").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED race_results_ext_py

-- COMMAND ----------

select * from race_results_ext_py;

-- COMMAND ----------

Drop TABLE IF EXISTS demo.race_results_ext_sql;
CREATE TABLE IF NOT EXISTS demo.race_results_ext_sql(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)
USING PARQUET
LOCATION "/mnt/purvadl/presentation/race_results_ext_sql" 

-- COMMAND ----------

SHOW TABLES IN demo;


-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

select COUNT(*) from demo.race_results_ext_sql

-- COMMAND ----------

drop table demo.race_results_ext_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on tables
-- MAGIC 1. Create Temp view
-- MAGIC 2. Create Global, Temp view
-- MAGIC 3. Create Permanent View

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS 
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2018;

-- COMMAND ----------

SELECT * from v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS 
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2018;

-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

SHOW TABLES in global_temp;

-- COMMAND ----------


