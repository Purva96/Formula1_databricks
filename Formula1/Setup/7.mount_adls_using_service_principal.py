# Databricks notebook source
def mount_adls_containers(container_name, storage_account_name):
  #get secrets from key vault
  client_id = dbutils.secrets.get(scope = 'formula1-Scope', key = 'service-principal-client-id')
  tenant_id = dbutils.secrets.get(scope = 'formula1-Scope', key = 'service-principal-tenant-id')
  client_secret = dbutils.secrets.get(scope = 'formula1-Scope', key = 'service-principal-client-secret')

  #Set spark configurations
  config = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type" :"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
  }

  mounts = dbutils.fs.mounts()
  if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in mounts):
    #Check if the container is already mounted
    return f'{container_name} container already mounted'
  else:
    #mount the storage account container
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = config)
    
    return display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount all the containers

# COMMAND ----------

containers = ['demo', 'raw', 'processed', 'presentation']
for container in containers:
    print(mount_adls_containers(container,'purvadl'))
display(dbutils.fs.mounts())


# COMMAND ----------

display(dbutils.fs.ls("/mnt/purvadl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/purvadl/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


