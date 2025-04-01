# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC Find year for which the data is to be reprocessed

# COMMAND ----------

race_result_list = spark.read.parquet(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date = '{v_file_date}'") \
        .select('race_year') \
            .distinct() \
                .collect()

# COMMAND ----------

race_result_list

# COMMAND ----------

race_year_list = []
for race_year in race_result_list:
    race_year_list.append(race_year.race_year)


# COMMAND ----------

from pyspark.sql.functions import col
race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_result_df)

# COMMAND ----------

display(race_result_df.describe())

# COMMAND ----------

from pyspark.sql.functions import when, sum, col, count
driver_standing_df = race_result_df \
    .groupBy('race_year','driver_name', 'driver_nationality', 'team') \
        .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

# display(driver_standing_df.filter("race_year == 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank


driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins")) 
final_df =driver_standing_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

# display(final_df.filter("race_year == 2020"))

# COMMAND ----------

write_data_to_table(final_df, 'f1_presentation', 'driver_standings','race_year') 

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standing")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_standings;
