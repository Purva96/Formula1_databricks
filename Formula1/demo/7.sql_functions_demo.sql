-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

SELECT * FROM drivers LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####CONCAT Function

-- COMMAND ----------

SELECT *, concat(driver_ref, '-',code) as new_driver_ref FROM drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####SPLIT Function

-- COMMAND ----------

SELECT *, split(name, ' ')[0] as Forename, split(name, ' ')[1] as Surname FROM drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####CURRENT TIMESTAMP

-- COMMAND ----------

SELECT *, current_timestamp() as now FROM drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####date_format()

-- COMMAND ----------

SELECT *, date_format(dob, 'dd-MM-yyyy') from drivers;

-- COMMAND ----------

select COUNT(*) from drivers;

-- COMMAND ----------

select max(dob) from drivers;

-- COMMAND ----------

select * from drivers where dob = '2000-05-11';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####GROUP BY

-- COMMAND ----------

select nationality, COUNT(*) as count from drivers 
group by nationality
having count>50 
order by count desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Window function

-- COMMAND ----------

select * from drivers

-- COMMAND ----------

select name, dob, nationality, rank() over (partition by nationality order by dob) as age_rank_desc from drivers
order by nationality;

-- COMMAND ----------


