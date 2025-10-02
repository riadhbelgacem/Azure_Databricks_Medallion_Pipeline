# Databricks notebook source
# MAGIC %md
# MAGIC # create catalog

# COMMAND ----------

# %sql
# DROP CATALOG cars_catlog CASCADE;


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG cars_catlog;

# COMMAND ----------

# MAGIC %md
# MAGIC # create schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA cars_catlog.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA cars_catlog.gold;

# COMMAND ----------

