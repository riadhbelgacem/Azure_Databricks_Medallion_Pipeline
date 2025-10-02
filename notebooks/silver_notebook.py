# Databricks notebook source
# MAGIC %md
# MAGIC # DATA READING

# COMMAND ----------

df=spark.read.format("parquet")\
    .option('inferSchema','true')\
    .load('abfss://bronze@carriadhdatalake.dfs.core.windows.net/rawdata')


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformation

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

df=df.withColumn('model_category',split(col('Model_ID'),'-')[0])

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('Unit_Sold',col('Unit_Sold').cast(StringType())).printSchema()

# COMMAND ----------

df=df.withColumn('RevperUnit',col('Revenue')/col('Unit_Sold'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #AD HOC

# COMMAND ----------

df.groupBy('Year','BranchName').agg(sum("Unit_Sold").alias('Total_Units')).sort('Year','Total_Units',ascending=[1,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Data writing

# COMMAND ----------

df.write.format('parquet')\
    .mode('overwrite')\
    .option('path','abfss://silver@carriadhdatalake.dfs.core.windows.net/carsales')\
    .save()

# COMMAND ----------

# MAGIC %md 
# MAGIC #Querying Silver Data 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM parquet.`abfss://silver@carriadhdatalake.dfs.core.windows.net/carsales`

# COMMAND ----------

