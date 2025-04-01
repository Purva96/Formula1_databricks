# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn('ingestion_date',current_timestamp())
    return output_df

# COMMAND ----------

def select_columns(input_df, partition_column):
    new_list_of_columns = []
    list_of_columns = input_df.schema.names
    for column in list_of_columns:
         if column != partition_column:
            new_list_of_columns.append(column)
    new_list_of_columns.append(partition_column)
    input_df = input_df.select(new_list_of_columns)
    return input_df


# COMMAND ----------

def write_data_to_table(input_df, database_name, table_name, partition_column):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    new_df = select_columns(input_df, partition_column)
    display(new_df)
    if spark._jsparkSession.catalog().tableExists(f"{database_name}.{table_name}"):
        #if table already exists then overwrite only the partition data
        new_df.write.mode("overwrite").insertInto(f"{database_name}.{table_name}")
    else:
        new_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{database_name}.{table_name}") 

# COMMAND ----------

def get_list_of_columns(input_df, column_name):
    df_column_list = input_df.select(column_name) \
        .distinct() \
            .collect()
    column_list = [i[column_name] for i in df_column_list]
    return column_list

# COMMAND ----------


