# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1:- Read the csv file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source"," ")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/purvadl/raw

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields= [StructField("circuitId", IntegerType(), False),
                                      StructField("circuitRef", StringType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("location", StringType(), True),
                                      StructField("country", StringType(), True),
                                      StructField("lat", DoubleType(), True),
                                      StructField("lng", DoubleType(), True),
                                      StructField("alt", IntegerType(), True),
                                      StructField("url", StringType(), True),
                                      ])

# COMMAND ----------

circuits_df = spark.read.schema(circuits_schema).csv(f"/mnt/purvadl/raw/{v_file_date}/circuits.csv",header = True)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step2: Select only the required coloumns
# MAGIC

# COMMAND ----------

# METHOD 1
circuits_selected_df = circuits_df.select('circuitId', 'circuitRef', 'name', 'location', 'country', 'lat', 'lng','alt')
                                          

# COMMAND ----------

# METHOD 2
circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng,circuits_df.alt)

# COMMAND ----------

#  METHOD 3
circuits_selected_df = circuits_df.select(circuits_df['circuitId'], circuits_df['circuitRef'], circuits_df['name'], circuits_df['location'],circuits_df['country'],circuits_df['lat'],circuits_df['lng'],circuits_df['alt'])

# COMMAND ----------

# Method 4
from pyspark.sql.functions import col

circuits_selected_df = circuits_df.select(col('circuitId'), col('circuitRef'), col('name'), col('location'), col('country'), col('lat'),col('lng'), col('alt'))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step3: Rename the columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed('circuitId','circuit_id') \
  .withColumnRenamed('circuitRef','circuit_ref') \
  .withColumnRenamed('lat','latitude') \
  .withColumnRenamed('lng', 'longitude') \
  .withColumnRenamed('alt', 'altitude') \
  .withColumn('data_source', lit(v_data_source)) \
  .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step4 : Add ingestion Date to the dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# to add literal value as a column in dataframe
from pyspark.sql.functions import lit
circuits_literal = circuits_final_df.withColumn('env',lit('Production'))
display(circuits_literal)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: write data frame to parquet file
# MAGIC  

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/purvadl/processed/circuits

# COMMAND ----------

# df = spark.read.parquet("/mnt/purvadl/processed/circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit("Success")
