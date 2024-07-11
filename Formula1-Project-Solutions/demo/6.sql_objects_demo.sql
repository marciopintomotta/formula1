-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Lesson Objectives
-- MAGIC 1. Spark SQL Documentation
-- MAGIC 1. Create Database demo
-- MAGIC 1. Data Tab in the UI
-- MAGIC 1. SHOW command
-- MAGIC 1. DESCRIBE command
-- MAGIC 1. Find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

use demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Learning Objectives
-- MAGIC 1. Create manages table using Python
-- MAGIC 1. Create manage tables using SQL
-- MAGIC 1. Effect of dropping a managed table
-- MAGIC 1. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC races_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC races_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

SELECT * from demo.race_results_python where race_year = 2020

-- COMMAND ----------

CREATE TABLE race_results_sql
as
SELECT * from demo.race_results_python where race_year = 2020

-- COMMAND ----------

select current_database()

-- COMMAND ----------

desc extended demo.race_results_sql

-- COMMAND ----------

drop table demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Learning Objectives
-- MAGIC 1. Create external tables using Python
-- MAGIC 1. Create external tables usin SQL
-- MAGIC 1. Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC races_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

desc extended demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(
race_year	int
, race_name	string
, race_date	timestamp
, circuit_location	string
, driver_name	string
, driver_number	int
, driver_nationality	string
, team	string
, grid	int
, fastest_lap	int
, race_time	string
, points	float
, position	int
, created_date	timestamp
)
USING PARQUET
LOCATION "/mnt/formula1dlcoursempa/presentation/race_results_ext_sql"

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

insert into demo.race_results_ext_sql
select * from demo.race_results_ext_py where race_year = 2020;

-- COMMAND ----------

select count(1) from demo.race_results_ext_sql

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

drop table demo.race_results_ext_sql

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Views on tables
-- MAGIC #### Learning Objectives
-- MAGIC 1. Create Temp View
-- MAGIC 1. Create Global Temp View
-- MAGIC 1. Create Permanent View

-- COMMAND ----------

create temp view v_race_results
as
select * from demo.race_results_python where race_year = 2018

-- COMMAND ----------

select current_database()

-- COMMAND ----------

select * from v_race_results

-- COMMAND ----------

create or replace global temp view gv_race_results
as
select * from demo.race_results_python where race_year = 2012

-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

create or replace view demo.pv_race_results
as
select * from demo.race_results_python where race_year = 2000

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

select * from demo.pv_race_results

-- COMMAND ----------


