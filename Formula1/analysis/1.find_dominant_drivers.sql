-- Databricks notebook source
-- MAGIC %md
-- MAGIC Dominant drivers having high average points having total races played greater than or equal to 50

-- COMMAND ----------

select driver_name,
COUNT(1) as total_races,
SUM(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
group by driver_name
having total_races >= 50
order by avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Dominant drivers having high average points having total races played greater than or equal to 50 in the past decade

-- COMMAND ----------

select driver_name,
COUNT(1) as total_races,
SUM(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year BETWEEN 2011 AND 2020 
group by driver_name
having total_races >= 50
order by avg_points DESC

-- COMMAND ----------


