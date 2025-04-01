# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC from v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

display(_sqldf)

# COMMAND ----------

year = 2020

# COMMAND ----------

race_results_yearly_df = spark.sql(f"SELECT * from v_race_results where race_year = {year}")

# COMMAND ----------

display(race_results_yearly_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Global Temp View

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from global_temp.gv_race_results;

# COMMAND ----------

display(spark.sql(f"SELECT * from global_temp.gv_race_results where race_year = {year}"))

# COMMAND ----------


