# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

sas_token = dbutils.secrets.get(scope = 'formula1-Scope',key = 'sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.purvadl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.purvadl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.purvadl.dfs.core.windows.net",sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@purvadl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@purvadl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


