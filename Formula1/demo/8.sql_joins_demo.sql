-- Databricks notebook source
use f1_presentation;

-- COMMAND ----------

show tables;

-- COMMAND ----------

desc driver_standing;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standing_2018 AS
SELECT * FROM driver_standing WHERE race_year = 2018;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standing_2020 AS
SELECT * FROM driver_standing WHERE race_year = 2020;

-- COMMAND ----------

select * from v_driver_standing_2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Inner Join

-- COMMAND ----------

select *
from v_driver_standing_2018 as v1_2018
JOIN v_driver_standing_2020 as v2_2020 
ON v1_2018.driver_name = v2_2020.driver_name;

-- COMMAND ----------

select *
from v_driver_standing_2018 as v1_2018
LEFT JOIN v_driver_standing_2020 as v2_2020 
ON v1_2018.driver_name = v2_2020.driver_name;

-- COMMAND ----------

select *
from v_driver_standing_2018 as v1_2018
RIGHT JOIN v_driver_standing_2020 as v2_2020 
ON v1_2018.driver_name = v2_2020.driver_name;

-- COMMAND ----------

select *
from v_driver_standing_2018 as v1_2018
FULL JOIN v_driver_standing_2020 as v2_2020 
ON v1_2018.driver_name = v2_2020.driver_name;

-- COMMAND ----------

select *
from v_driver_standing_2018 as v1_2018
SEMI JOIN v_driver_standing_2020 as v2_2020 
ON v1_2018.driver_name = v2_2020.driver_name;

-- COMMAND ----------

select *
from v_driver_standing_2018 as v1_2018
ANTI JOIN v_driver_standing_2020 as v2_2020 
ON v1_2018.driver_name = v2_2020.driver_name;

-- COMMAND ----------

select *
from v_driver_standing_2018 as v1_2018
CROSS JOIN v_driver_standing_2020 as v2_2020 
ON v1_2018.driver_name = v2_2020.driver_name;

-- COMMAND ----------


