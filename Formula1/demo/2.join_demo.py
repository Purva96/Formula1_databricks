# Databricks notebook source
# MAGIC %md
# MAGIC ### Spark Join Transformation

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year=2019").withColumnRenamed('name','races_name')

# COMMAND ----------

display(races_df)

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed('name','circuits_name')

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'inner').select(circuits_df.circuits_name, circuits_df.location, circuits_df.country,races_df.races_name, races_df.round)

# COMMAND ----------

display(race_circuit_df.limit(5))

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
    .filter("circuit_id <70") \
    .withColumnRenamed('name','circuits_name')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

#left outer join

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'left').select(circuits_df.circuits_name, circuits_df.location, circuits_df.country,races_df.races_name, races_df.round)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

#right outer join
race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'right').select(circuits_df.circuits_name, circuits_df.location, circuits_df.country,races_df.races_name, races_df.round)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

#full outer join

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'full').select(circuits_df.circuits_name, circuits_df.location, circuits_df.country,races_df.races_name, races_df.round)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

#semi join - inner join but it will give matching rows only based on the left dataframe
# does not add the columns from right table. If right table columns are selected, then it will give error
race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'semi')

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Anti Join

# COMMAND ----------

#anti - everything on the left df not found on the right df
race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'anti')


# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

#cross join - cartessian product of both the table. Every row from left and join to every record on right.

race_circuit_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

race_circuit_df.count()

# COMMAND ----------

int(races_df.count())*int(circuits_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Cross joins are not used often

# COMMAND ----------


