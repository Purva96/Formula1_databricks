# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1: Read Json file using spark dataframe reader 
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source"," ")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

#DDL style schema
constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructor_schema).json(f'{raw_folder_path}/{v_file_date}/constructors.json')

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2: Drop the url column and add the ingestion_date col
# MAGIC

# COMMAND ----------

constructor_drop_df = constructor_df.drop('url')

# COMMAND ----------

display(constructor_drop_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
constructor_drop_df = constructor_drop_df.withColumn('ingestion_date', current_timestamp()) \
    .withColumn('data_source', lit(v_data_source)) \
        .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

display(constructor_drop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Rename Columns

# COMMAND ----------

constructor_final_df = constructor_drop_df.withColumnsRenamed({'constructorId':'constructor_id', 'constructorRef':'constructor_ref'})

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step4: Write dataframe to parquet file
# MAGIC

# COMMAND ----------

constructor_final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

constructor_final_df.write.mode('overwrite').parquet(f"{processed_folder_path}/constructors/")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}/constructors/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_processed.constructors

# COMMAND ----------

dbutils.notebook.exit("Success")
