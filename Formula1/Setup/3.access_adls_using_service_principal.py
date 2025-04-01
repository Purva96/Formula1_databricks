# Databricks notebook source
# MAGIC %md
# MAGIC # Access ADL using service principal
# MAGIC 1. Register Azure AD application/service principal
# MAGIC 2. Generate a secret/password for the application
# MAGIC 3. Set spark config with app/client id, directory/tenant id and secret
# MAGIC 4. Assign role storage blob data contributor to the data lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'service-principal-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'service-principal-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'service-principal-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.purvadl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.purvadl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.purvadl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.purvadl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.purvadl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@purvadl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@purvadl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


