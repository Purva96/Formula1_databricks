-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominan Formula 1 Drivers</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view v_dominant_teams
AS
select team_name,
AVG(calculated_points) as avg_points,
SUM(calculated_points) as total_points,
COUNT(*) as race_count,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) as team_rank  
 from f1_presentation.calculated_race_results
 group by team_name
 having race_count >=100
 order by avg_points desc;

-- COMMAND ----------

select * from v_dominant_teams

-- COMMAND ----------

select
team_name,
race_year, 
SUM(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where team_name IN (select team_name from v_dominant_teams where team_rank <= 5)
group by race_year, team_name
order by race_year, avg_points DESC

-- COMMAND ----------


