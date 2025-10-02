# Databricks notebook source
# MAGIC %md
# MAGIC # create fact table 

# COMMAND ----------

# MAGIC %md
# MAGIC **reading silver data**

# COMMAND ----------

df_silver=spark.sql('''select * from parquet.`abfss://silver@carriadhdatalake.dfs.core.windows.net/carsales` ''')

# COMMAND ----------

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **reading all the DMs**

# COMMAND ----------

df_dealer=spark.sql("select * from cars_catlog.gold.dim_dealer")
df_branch=spark.sql("select * from cars_catlog.gold.dim_branch")
df_date=spark.sql("select * from cars_catlog.gold.dim_date")
df_model=spark.sql("select * from cars_catlog.gold.dim_model")


# COMMAND ----------

# MAGIC %md 
# MAGIC **bringing keys to the fact table**

# COMMAND ----------

df_fact = df_silver.join(df_branch,df_silver['Branch_ID']==df_branch['Branch_ID'],'left')\
.join(df_dealer,df_silver['Dealer_ID']==df_dealer['Dealer_ID'],'left')\
.join(df_date,df_silver['Date_ID']==df_date['Date_ID'],'left')\
.join(df_model,df_silver['Model_ID']==df_model['Model_ID'],'left')\
.select(df_silver['Revenue'],df_silver['Unit_Sold'],df_silver['RevperUnit'],df_branch['dim_branch_key'],df_dealer['dim_dealer_key'],df_model['dim_model_key'],df_date['dim_date_key'])

# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **writing fact table**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

#incremental RUN 
if spark.catalog.tableExists("cars_catlog.gold.factsales"):
    delta_tbl = DeltaTable.forPath(spark,"abfss://gold@carriadhdatalake.dfs.core.windows.net/factsales")
    delta_tbl.alias("trg").merge(df_fact.alias("src"),"trg.dim_date_key=src.dim_date_key and trg.dim_branch_key=src.dim_branch_key and trg.dim_dealer_key=src.dim_dealer_key and trg.dim_model_key=src.dim_model_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_fact.write.format('delta').\
        mode("overwrite").\
        option("path", "abfss://gold@carriadhdatalake.dfs.core.windows.net/factsales")\
            .saveAsTable("cars_catlog.gold.factsales")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catlog.gold.factsales

# COMMAND ----------

