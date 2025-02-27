# Databricks notebook source
import numpy as np

# COMMAND ----------

arr = np.random.randint(0,11,10)

# COMMAND ----------

arr

# COMMAND ----------

arr[1:5] = 100

# COMMAND ----------

arr

# COMMAND ----------

arr_copy= arr.copy()

# COMMAND ----------

slice_of_array = arr[:5]

# COMMAND ----------

slice_of_array

# COMMAND ----------

slice_of_array[:]= 100

# COMMAND ----------

arr

# COMMAND ----------

arr_copy

# COMMAND ----------

arr_2d = np.array([[0,5,10],[15,20,25],[30,35,40]])

# COMMAND ----------

arr_2d

# COMMAND ----------

arr_2d[1:,:2]

# COMMAND ----------


