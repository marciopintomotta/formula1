# Databricks notebook source
# MAGIC %md
# MAGIC ### Access dataframes using SQL
# MAGIC #### Objectives
# MAGIC 1. Create global temporary views on dataframes
# MAGIC 1. Access the view from SQL cell
# MAGIC 1. Access the view from Python cell
# MAGIC 1. Access the view from another notebook

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

races_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

races_results_2019_df = spark.sql(f"select * from v_race_results where race_year = {p_race_year}")

# COMMAND ----------

display(races_results_2019_df)

# COMMAND ----------

races_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

races_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results

# COMMAND ----------

spark.sql("select * \
    from global_temp.gv_race_results").show()

# COMMAND ----------


