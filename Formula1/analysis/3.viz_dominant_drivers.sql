-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW v_dominant_driver
AS
select driver_name,
COUNT(1) as total_races,
SUM(calculated_points) as total_points,
avg(calculated_points) as avg_points,
RANK() OVER(ORDER BY avg(calculated_points) DESC) driver_rank
from f1_presentation.calculated_race_results
group by driver_name
having COUNT(1) >= 50
order by avg_points DESC

-- COMMAND ----------

select * from v_dominant_driver

-- COMMAND ----------

select
race_year,
driver_name,
COUNT(1) as total_races,
SUM(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where driver_name IN (select driver_name from v_dominant_driver where driver_rank <= 10)
group by race_year,driver_name
order by race_year, avg_points DESC
