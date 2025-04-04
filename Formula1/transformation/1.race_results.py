# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# step1 - Read races and circuits parquet file

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("name","circuit_name").withColumnRenamed('location','circuit_location')

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
    .withColumnRenamed('name','race_name') \
        .withColumnRenamed('race_timestamp','race_date')

# COMMAND ----------

display(races_df.limit(5))

# COMMAND ----------

display(circuits_df.limit(5))

# COMMAND ----------

#Join on circuit_id col
#select race_year,race_name,race_date,race_id for races_df
#select circuit_location from circuits_df

circuit_races_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id ,'inner').select(races_df.race_year,races_df.race_name,races_df.race_date,races_df.race_id,circuits_df.circuit_location)

# COMMAND ----------

display(circuit_races_df.count())

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
    .withColumnRenamed('name', 'driver_name') \
        .withColumnRenamed('number','driver_number') \
            .withColumnRenamed('nationality','driver_nationality')

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors").withColumnRenamed('name','team')

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
    .filter(f"file_date = '{v_file_date}'") \
    .withColumnRenamed('time','race_time') \
        .withColumnRenamed('race_id','results_race_id') \
            .withColumnRenamed('file_date','results_file_date')

# COMMAND ----------

race_results_df = results_df.join(circuit_races_df, circuit_races_df.race_id == results_df.results_race_id) \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id) 

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select('race_id','race_year', 'race_name', 'race_date', 'circuit_location', 'driver_name', 'driver_number', 'driver_nationality', 'team', 'grid', 'fastest_lap', 'race_time', 'points', 'position','results_file_date') \
    .withColumn('created_date', current_timestamp()) \
        .withColumnRenamed('results_file_date','file_date')

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# display(final_df.filter("race_year==2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

write_data_to_table(final_df, 'f1_presentation', 'race_results','race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_presentation.race_results
