# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_results = dbutils.notebook.run("1.insgest_circuits_file",0,{"p_data_source":"Ergast API", "p_file_date":"2021-03-21"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("2.Ingest_races_file",0,{"p_data_source":"Ergast API", "p_file_date":"2021-03-21"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("3.ingest_constructors_file",0,{"p_data_source":"Ergast API", "p_file_date":"2021-03-21"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("4.ingest_drivers_nested_json",0,{"p_data_source":"Ergast API", "p_file_date":"2021-03-21"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("5.ingest_results_file",0,{"p_data_source":"Ergast API", "p_file_date":"2021-03-21"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("6.ingest_pitstops_file",0,{"p_data_source":"Ergast API", "p_file_date":"2021-03-21"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("7.ingest_lap_times_folder",0,{"p_data_source":"Ergast API", "p_file_date":"2021-03-21"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("8.ingest_qualifying_folder",0,{"p_data_source":"Ergast API", "p_file_date":"2021-03-21"})

# COMMAND ----------

v_results
