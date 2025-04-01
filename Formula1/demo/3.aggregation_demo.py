# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

demo_df = race_result_df.filter("race_year == 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

# total number of races in year 2020
demo_df.select(count("*")).show()

# COMMAND ----------

#Distinct count of races
demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name == 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name")) \
.withColumnRenamed("sum(points)","total_points") \
.withColumnRenamed("count(DISTINCT race_name)","number_of_races") \
.show()

# COMMAND ----------

demo_driver = demo_df.groupBy("driver_name").sum("points").withColumnRenamed("sum(points)", "sum_of_points")

# COMMAND ----------

#multiple aggregate functions

demo_driver = demo_df.groupBy("driver_name") \
    .agg(sum("points").alias("total_points"),countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

display(demo_driver.orderBy(demo_driver.total_points.desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Window Functions

# COMMAND ----------

demo_df = race_result_df.filter("race_year in (2019,2020)")

# COMMAND ----------

#groupby multiple columns
demo_driver = demo_df.groupBy("race_year","driver_name") \
    .agg(sum("points").alias("total_points"),countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

display(demo_driver)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_driver = demo_driver.withColumn("rank",rank().over(driverRankSpec))

# COMMAND ----------

display(demo_driver)

# COMMAND ----------


