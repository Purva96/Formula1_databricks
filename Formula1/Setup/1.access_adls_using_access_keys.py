# Databricks notebook source
formula1dl_access_key = dbutils.secrets.get(scope = "formula1-Scope", key = "access-key")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.purvadl.dfs.core.windows.net",formula1dl_access_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@purvadl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@purvadl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


