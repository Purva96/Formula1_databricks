-- Databricks notebook source
select team_name,
AVG(calculated_points) as avg_points,
SUM(calculated_points) as total_points,
COUNT(*) as race_count
 from f1_presentation.calculated_race_results
 group by team_name
 having race_count >=100
 order by avg_points desc;

-- COMMAND ----------

select team_name,
AVG(calculated_points) as avg_points,
SUM(calculated_points) as total_points,
COUNT(*) as race_count
 from f1_presentation.calculated_race_results
 where race_year between 2011 and 2020
 group by team_name
 having race_count >=100
 order by avg_points desc;

-- COMMAND ----------


