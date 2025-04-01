# Databricks notebook source
# MAGIC %md
# MAGIC 1. write data to delta lake (managed tables)
# MAGIC 2. write data to delta lake (external table)
# MAGIC 3. read data from delta lake(table)
# MAGIC 4. read data from delta lake (file)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/purvadl/demo'

# COMMAND ----------

results_df = spark.read \
    .option("inferSchema", True) \
        .json("/mnt/purvadl/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').saveAsTable('f1_demo.results_managed')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').save('/mnt/purvadl/demo/results_external')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/purvadl/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external

# COMMAND ----------

results_external_df = spark.read.format('delta').load('/mnt/purvadl/demo/results_external')

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_df.write.format('delta') \
    .mode('overwrite') \
    .partitionBy('constructorId') \
    .option("path", "/mnt/purvadl/demo/results_partition") \
    .saveAsTable("f1_demo.results_partition")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partition

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update delta lake
# MAGIC 2. Delete from delta lake

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points = 11 - position
# MAGIC WHERE position<= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

#update table using python
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/purvadl/demo/results_managed")
deltaTable.update("position<=10",{"points":"11-position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position >10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

#delete table using python
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/purvadl/demo/results_managed")
deltaTable.delete("points=0")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------


