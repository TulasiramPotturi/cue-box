# Databricks notebook source
# MAGIC %md
# MAGIC #  Dimension Tables
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC | Detail Tag | Information |
# MAGIC |------------|-------------|
# MAGIC |Originally Created By | Sagar Omar |
# MAGIC |External References ||
# MAGIC |Input Datasets |<ul><li>data_lake_base_uri+/silver/</li></ul>|
# MAGIC |Output Datasets |<ul><li>data_lake_base_uri+/gold/</li></ul>|
# MAGIC |Input Data |ADLS Gen2 Bronze Layer|
# MAGIC |Output Data Source |ADLS Gen2 Silver Layer|
# MAGIC |Downstream External System|
# MAGIC
# MAGIC ## History
# MAGIC
# MAGIC | Date | Developed By | Reason |
# MAGIC |:----:|--------------|--------|
# MAGIC | 2023-12-28| Sagar Omar |Creating dimensions in gold layer|

# COMMAND ----------

# DBTITLE 1,Common Imports and Schema
# MAGIC %run /Repos/potturi.tulasiram@diggibyte.com/cue-box/cue_box/utility/functions

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("schedule_date", "", "schedule_date")
dbutils.widgets.text("data_lake_base_uri", "", "data_lake_base_uri")
dbutils.widgets.text("database_name", "", "database_name")
dbutils.widgets.text("target_table_name","orders")
dbutils.widgets.text("add_new_columns","add_new_columns")
dbutils.widgets.text("load_type","load_type")

data_lake_base_uri = dbutils.widgets.get("data_lake_base_uri")
database_name = dbutils.widgets.get("database_name")
target_table_name = dbutils.widgets.get("target_table_name")
schedule_date = dbutils.widgets.get("schedule_date")
add_new_columns = dbutils.widgets.get("add_new_columns")
load_type = dbutils.widgets.get("load_type")

# COMMAND ----------

# DBTITLE 1,Constants
table_name = "dim_" + target_table_name

gold_layer_path = data_lake_base_uri + "/{}/{}/{}/".format("data/gold", database_name, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #Extract & Transform

# COMMAND ----------

# DBTITLE 1,Read Silver table
silver_layer_df = get_table_data(database_name, target_table_name).withColumnRenamed("load_date", "at_load_date")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Load

# COMMAND ----------

# DBTITLE 1,Writing the data to Gold Layer
if load_type == "incremental":
    merge_into_delta_table(silver_layer_df, database_name, table_name,gold_layer_path)
else:
    overwrite_delta_table(silver_layer_df, database_name, table_name,gold_layer_path, add_new_columns)
