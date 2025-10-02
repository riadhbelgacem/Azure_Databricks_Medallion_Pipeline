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

df_src=spark.sql('''select distinct(Model_ID) as Model_ID, model_category from parquet.`abfss://silver@carriadhdatalake.dfs.core.windows.net/carsales` ''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim_model_Sink-Initial Incremental 

# COMMAND ----------

if spark.catalog.tableExists('cars_catlog.gold.dim_model'):
  df_sink=spark.sql('select dim_model_key,Model_ID,model_category  from cars_catlog.gold.dim_model')
else:
    df_sink=spark.sql('''
    select 1 as dim_model_key,Model_ID,model_category from parquet.`abfss://silver@carriadhdatalake.dfs.core.windows.net/carsales`
    where 1=0 ''')

# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Filtring new recoreds and old records 

# COMMAND ----------

df_filter=df_src.join(df_sink,df_src['Model_ID']==df_sink['Model_ID'],'left').select(df_src['Model_ID'],df_src['model_category'],df_sink['dim_model_key'])

# COMMAND ----------

df_filter.display()


# COMMAND ----------

# MAGIC %md  
# MAGIC ### df filter old 
# MAGIC

# COMMAND ----------

df_filter_old=df_filter.filter(col('dim_model_key').isNotNull())

# COMMAND ----------

# MAGIC %md 
# MAGIC ###df_filter_new

# COMMAND ----------

df_filter_new=df_filter.filter(col('dim_model_key').isNull()).select(df_src['Model_ID'],df_src['model_category'])

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### create surrogate key

# COMMAND ----------

# MAGIC %md
# MAGIC **fetch the max suggogate key form existing table**

# COMMAND ----------

if spark.catalog.tableExists("cars_catlog.gold.dim_model"):
    max_key_row = spark.sql("select coalesce(max(dim_model_key), 0) as max_key from cars_catlog.gold.dim_model").collect()[0]
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

df_filter_new = df_filter_new.withColumn('dim_model_key', max_value + monotonically_increasing_id())

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
if spark.catalog.tableExists("cars_catlog.gold.dim_model"):
    delta_tbl = DeltaTable.forPath(spark,"abfss://gold@carriadhdatalake.dfs.core.windows.net/dim_model")
    delta_tbl.alias("trg").merge(df_final.alias("src"),"trg.dim_model_key=src.dim_model_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_final.write.mode("overwrite").\
        option("path", "abfss://gold@carriadhdatalake.dfs.core.windows.net/dim_model")\
            .saveAsTable("cars_catlog.gold.dim_model")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catlog.gold.dim_model

# COMMAND ----------

