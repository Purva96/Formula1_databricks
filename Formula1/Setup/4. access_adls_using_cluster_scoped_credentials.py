# Databricks notebook source
display(dbutils.fs.ls("abfss://demo@purvadl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@purvadl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


