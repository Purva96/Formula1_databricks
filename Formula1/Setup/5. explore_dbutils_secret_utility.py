# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("formula1-Scope")

# COMMAND ----------

dbutils.secrets.get(scope = "formula1-Scope", key = "access-key")

# COMMAND ----------


