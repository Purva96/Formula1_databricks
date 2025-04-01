# Databricks notebook source
dbutils.widgets.text("p_data_source"," ")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DateType

# COMMAND ----------

results_schema  = StructType(fields = [StructField("resultId",IntegerType(),False),
                                       StructField("raceId",IntegerType(),False),
                                       StructField("driverId",IntegerType(),False),
                                       StructField("constructorId",IntegerType(),False),
                                       StructField("number",IntegerType(),True),
                                       StructField("grid",IntegerType(),False),
                                       StructField("position",IntegerType(),True),
                                       StructField("positionText",StringType(),False),
                                       StructField("positionOrder",IntegerType(),False),
                                       StructField("points",DoubleType(),False),
                                       StructField("laps",IntegerType(),False),
                                       StructField("time",StringType(),True),
                                       StructField("milliseconds",IntegerType(),True),
                                       StructField("fastestLap",IntegerType(),True),
                                       StructField("rank",IntegerType(),True),
                                       StructField("fastestLapTime",StringType(),True),
                                       StructField("fastestLapSpeed",StringType(),True),
                                       StructField("statusId",IntegerType(),False),])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

display(results_df.printSchema())

# COMMAND ----------

display(results_df.limit(10))

# COMMAND ----------

results_renamed_df = results_df.withColumnsRenamed({"resultId":"result_id",
                                                    "raceId":"race_id",
                                                    "driverId":"driver_id",
                                                    "constructorId":"constructor_id",
                                                    "positionText":"position_text",
                                                    "positionOrder":"position_order",
                                                    "fastestLap":"fastest_lap",
                                                    "fastestLapTime":"fastest_lap_time",
                                                    "fastestLapSpeed":"fastest_lap_speed",
                                                    }).drop("statusId")

# COMMAND ----------

display(results_renamed_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
results_final = results_renamed_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("data_source", lit(v_data_source)) \
        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(results_final.limit(10))

# COMMAND ----------

results_final.write.mode('append').parquet(f"{processed_folder_path}/results/")

# COMMAND ----------

# MAGIC %md
# MAGIC Method 1

# COMMAND ----------

# for race_id_list in results_final.select("race_id").distinct().collect():
#     if spark._jsparkSession.catalog().tableExists("f1_processed.results"):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id={race_id_list.race_id})")

# COMMAND ----------

# results_final.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2

# COMMAND ----------

write_data_to_table(results_final, 'f1_processed', 'results','race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(race_id)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_processed.results

# COMMAND ----------

dbutils.notebook.exit("Success")
