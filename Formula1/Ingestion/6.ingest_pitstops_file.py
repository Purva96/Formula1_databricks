# Databricks notebook source
dbutils.widgets.text("p_data_source"," ")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, IntegerType, StringType

# COMMAND ----------

pitstops_schema = StructType(fields = [StructField("raceId", IntegerType(),False),
                                       StructField("driverId", IntegerType(),True),
                                       StructField("stop", IntegerType(),True),
                                       StructField("lap", IntegerType(),True),
                                       StructField("time", StringType(),True),
                                       StructField("duration", StringType(),True),
                                       StructField("milliseconds", IntegerType(),True)])

# COMMAND ----------

pitstops_df = spark.read.option("multiLine",True).schema(pitstops_schema).json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

display(pitstops_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
pitstops_renamed_df = pitstops_df.withColumnsRenamed({"driverId":"driver_id", "raceId":"race_id"}).\
    withColumn("ingestion_date",current_timestamp()) \
        .withColumn("data_source", lit(v_data_source)) \
            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(pitstops_renamed_df)

# COMMAND ----------


write_data_to_table(pitstops_renamed_df, 'f1_processed', 'pit_stops','race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.pit_stops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")
