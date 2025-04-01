# Databricks notebook source
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/purvadl/raw
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source", " ")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, TimestampType
races_df_schema = StructType(fields = [StructField('raceId', IntegerType(), False),
                                       StructField('year', IntegerType(), True),
                                       StructField('round', IntegerType(), True),
                                       StructField('circuitId', IntegerType(), True),
                                       StructField('name', StringType(), True),
                                       StructField('date', StringType(), True),
                                       StructField('time', StringType(), True),
                                       StructField('url', StringType(), True)])

# COMMAND ----------

races_df =  spark.read.option("header", True).schema(races_df_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")


# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, concat,lit, current_timestamp
races_df = races_df.withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn('ingestion_date',current_timestamp()) \
            .withColumn('data_source', lit(v_data_source)) \
                .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

display(races_df)

# COMMAND ----------

display(races_df.printSchema())

# COMMAND ----------

races_df = races_df.drop('url','date','time','source')

# COMMAND ----------

from pyspark.sql.functions import col
races_df_selected = races_df.select(col('raceId'),col('year'), col('round'), col('circuitId'), col('name'),col('race_timestamp'), col('ingestion_date'),col('data_source'), col('file_date'))

# COMMAND ----------

display(races_df_selected.limit(5))

# COMMAND ----------

races_df_final = races_df_selected.withColumnsRenamed({'raceId':'race_id', 'year':'race_year','circuitId':'circuit_id'})

# COMMAND ----------

display(races_df_final)

# COMMAND ----------

races_df_final.write.mode('overwrite').parquet("/mnt/purvadl/processed/races")

# COMMAND ----------

races_df_final.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_processed.races;

# COMMAND ----------

#Partition data by race year
# races_df_final.write.mode('overwrite').partitionBy('race_year').parquet("/mnt/purvadl/processed/races")

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/races",header = True)

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Races file ingested successfully")
