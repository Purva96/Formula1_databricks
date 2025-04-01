# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results").filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_result_list = get_list_of_columns(race_result_df, 'race_year')
print(race_result_list)

# COMMAND ----------

from pyspark.sql.functions import col
race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
    .filter(col("race_year").isin(race_result_list))

# COMMAND ----------

display(race_result_df)

# COMMAND ----------

from pyspark.sql.functions import when, sum, col, count
constructor_standing_df = race_result_df \
    .groupBy('race_year', 'team') \
        .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

# display(constructor_standing_df.filter("race_year == 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank


constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins")) 
final_df =constructor_standing_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

write_data_to_table(final_df, 'f1_presentation', 'constructor_standings','race_year') 

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standing")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructor_standings;

# COMMAND ----------


