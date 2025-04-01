# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", " ")
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date") 

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields = [StructField("qualifyId", IntegerType(),False),
                                        StructField("raceId", IntegerType(),False),
                                       StructField("driverId", IntegerType(),False),
                                       StructField("constructorId", IntegerType(),False),
                                       StructField("number", IntegerType(),False),
                                       StructField("position", IntegerType(),False),
                                       StructField("q1", StringType(),True),
                                       StructField("q2", StringType(),True),
                                       StructField("q3", StringType(),True)])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("multiLine", True).json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

display(qualifying_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
qualify_renamed_df = qualifying_df.withColumnsRenamed({"qualifyId":"qualify_id", "driverId":"driver_id", "raceId":"race_id", "constructorId": "constructor_id"}) \
    .withColumn("ingestion_date",current_timestamp()) \
        .withColumn('data_source',lit(v_data_source)) \
            .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

display(qualify_renamed_df)

# COMMAND ----------

# qualify_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

write_data_to_table(qualify_renamed_df, 'f1_processed', 'qualifying','race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.qualifying

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,COUNT(1) 
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")
