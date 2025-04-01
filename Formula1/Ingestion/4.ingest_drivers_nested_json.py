# Databricks notebook source
# MAGIC %md
# MAGIC #### step1: Read Json file using spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text("p_data_source"," ")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, IntegerType,StringType,DoubleType, DateType


# COMMAND ----------

name_schema = StructType(fields = [StructField("forename", StringType(), True),
                        StructField("surname", StringType(), True)])

# COMMAND ----------

driver_schema = StructType(fields = [StructField("driverId",IntegerType(), False),
                                     StructField("driverRef",StringType(), True),
                                     StructField("number",IntegerType(), True),
                                     StructField("code",StringType(), True),
                                     StructField("name",name_schema),
                                     StructField("dob",DateType(), True),
                                     StructField("nationality",StringType(), True),
                                     StructField("url",StringType(), True)
                                    ])

# COMMAND ----------

drivers_df = spark.read.schema(driver_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2: Rename the columns and add new columns
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import concat, col, current_timestamp, lit

# COMMAND ----------

drivers_with_col_df = drivers_df.withColumnsRenamed({"driverId":"driver_id","driverRef":"driver_ref"}).withColumn("ingestion_date", current_timestamp()).withColumn("name", concat(col("name.forename"),lit(" "), col("name.surname"))).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(drivers_with_col_df.limit(5))

# COMMAND ----------

driver_final_df = drivers_with_col_df.drop(col('url'))

# COMMAND ----------

driver_final_df.write.mode('overwrite').parquet(f"{processed_folder_path}/drivers/")

# COMMAND ----------

driver_final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/purvadl/processed/drivers

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/drivers",header = True)

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
