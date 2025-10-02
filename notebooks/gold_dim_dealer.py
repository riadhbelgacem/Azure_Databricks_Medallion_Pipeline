# Databricks notebook source
# MAGIC %md
# MAGIC #CREATE FLAG PARAMETER

# COMMAND ----------

from pyspark.sql.functions import*
from pyspark.sql.types import*

# COMMAND ----------

dbutils.widgets.text('incremental_falg','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_falg')
print(type(incremental_flag))

# COMMAND ----------

# MAGIC %md
# MAGIC #CREATING DIMENTION Model

# COMMAND ----------

# MAGIC %md
# MAGIC ### fetch relative columns

# COMMAND ----------

df_src=spark.sql('''select distinct(Dealer_ID) as Dealer_ID, DealerName from parquet.`abfss://silver@carriadhdatalake.dfs.core.windows.net/carsales` ''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim_model_Sink-Initial Incremental 

# COMMAND ----------

if spark.catalog.tableExists('cars_catlog.gold.dim_dealer'):
  df_sink=spark.sql('select dim_dealer_key,Dealer_ID,DealerName from cars_catlog.gold.dim_dealer')
else:
    df_sink=spark.sql('''
    select 1 as dim_dealer_key,Dealer_ID,DealerName from parquet.`abfss://silver@carriadhdatalake.dfs.core.windows.net/carsales`
    where 1=0 ''')

# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Filtring new recoreds and old records 

# COMMAND ----------

df_filter=df_src.join(df_sink,df_src['Dealer_ID']==df_sink['Dealer_ID'],'left').select(df_src['Dealer_ID'],df_src['DealerName'],df_sink['dim_dealer_key'])

# COMMAND ----------

df_filter.display()


# COMMAND ----------

# MAGIC %md  
# MAGIC ### df filter old 
# MAGIC

# COMMAND ----------

df_filter_old=df_filter.filter(col('dim_dealer_key').isNotNull())

# COMMAND ----------

# MAGIC %md 
# MAGIC ###df_filter_new

# COMMAND ----------

df_filter_new=df_filter.filter(col('dim_dealer_key').isNull()).select(df_src['Dealer_ID'],df_src['DealerName'])

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### create surrogate key

# COMMAND ----------

# MAGIC %md
# MAGIC **fetch the max suggogate key form existing table**

# COMMAND ----------

if spark.catalog.tableExists("cars_catlog.gold.dim_dealer"):
    max_key_row = spark.sql("select coalesce(max(dim_dealer_key), 0) as max_key from cars_catlog.gold.dim_dealer").collect()[0]
    max_key = int(max_key_row['max_key'])
else:
    max_key = 0

# COMMAND ----------

if incremental_flag == '0':
    # if table exists, start after current max; otherwise start at 1
    max_value = max_key + 1 if max_key > 0 else 1
else:
    max_value = max_key + 1

# COMMAND ----------

# MAGIC %md
# MAGIC **cretate suggorate key column and add the max surrogate key**

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_dealer_key', max_value + monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### create finale DF df_filter_old + df_filter_new

# COMMAND ----------

df_final= df_filter_new.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD TYPE -1(UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

#incremental RUN 
if spark.catalog.tableExists("cars_catlog.gold.dim_dealer"):
    delta_tbl = DeltaTable.forPath(spark,"abfss://gold@carriadhdatalake.dfs.core.windows.net/dim_dealer")
    delta_tbl.alias("trg").merge(df_final.alias("src"),"trg.dim_dealer_key=src.dim_dealer_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_final.write.mode("overwrite").\
        option("path", "abfss://gold@carriadhdatalake.dfs.core.windows.net/dim_dealer")\
            .saveAsTable("cars_catlog.gold.dim_dealer")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catlog.gold.dim_dealer

# COMMAND ----------

