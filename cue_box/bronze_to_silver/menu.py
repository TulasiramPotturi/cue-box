# Databricks notebook source
# MAGIC %md
# MAGIC #  Menu Data Onboarding
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC | Detail Tag | Information |
# MAGIC |------------|-------------|
# MAGIC |Originally Created By | Sagar Omar |
# MAGIC |External References ||
# MAGIC |Input Datasets |<ul><li>data_lake_base_uri+/bronze/menu_source_system/menu/{schedule_date}/*.csv"</li></ul>|
# MAGIC |Output Datasets |<ul><li>data_lake_base_uri+/silver/menu_source_system/menu/</li></ul>|
# MAGIC |Input Data |ADLS Gen2 Bronze Layer|
# MAGIC |Output Data Source |ADLS Gen2 Silver Layer|
# MAGIC |Downstream External System|
# MAGIC
# MAGIC ## History
# MAGIC
# MAGIC | Date | Developed By | Reason |
# MAGIC |:----:|--------------|--------|
# MAGIC | 2023-12-28| Sagar Omar |onboarding the menu data to datalake - silver layer|

# COMMAND ----------

# MAGIC %md
# MAGIC ## Required Imports

# COMMAND ----------

# MAGIC %run /Users/sagar.omar@electrolux.com/CB/utility/functions

# COMMAND ----------

# MAGIC %run /Users/sagar.omar@electrolux.com/CB/utility/schemas

# COMMAND ----------

# MAGIC %md ## Required Constants/Parameters/Variables

# COMMAND ----------

# DBTITLE 1,Read Parameters from Pipeline - ADF
dbutils.widgets.text("schedule_date", "", "schedule_date")
dbutils.widgets.text("data_lake_base_uri", "", "data_lake_base_uri")
dbutils.widgets.text("database_name", "", "database_name")
dbutils.widgets.text("target_table_name","orders")
dbutils.widgets.text("file_format","file_format")
dbutils.widgets.text("add_new_columns","add_new_columns")
dbutils.widgets.text("load_type","load_type")

data_lake_base_uri = dbutils.widgets.get("data_lake_base_uri")
database_name = dbutils.widgets.get("database_name")
target_table_name = dbutils.widgets.get("target_table_name")
schedule_date = dbutils.widgets.get("schedule_date")
file_format = dbutils.widgets.get("file_format")
add_new_columns = dbutils.widgets.get("add_new_columns")
load_type = dbutils.widgets.get("load_type")

# COMMAND ----------

# DBTITLE 1,Required Variable(s)
if load_type == "incremental":
  sub_directory = schedule_date
  add_new_columns = False
else:
  sub_directory = "historical"

bronze_layer_path = data_lake_base_uri + "/{}/{}/{}/{}".format("bronze", database_name, target_table_name,sub_directory)
silver_layer_path = data_lake_base_uri + "/{}/{}/{}/".format("silver", database_name, target_table_name)

# COMMAND ----------

# DBTITLE 1,Constants
CUSTOM_SCHEMA = get_custom_schema(target_table_name)
MERGE_COLUMNS = get_merge_column(target_table_name)

OPTIONS_DICT = {
      "format" : file_format,
      "path": bronze_layer_path,
      "schema": CUSTOM_SCHEMA,
      "additional_options" : {
         "header": "true",
         "delimeter": ",",
         "multiline": "true"
      }
}

# COMMAND ----------

# MAGIC %md ## EXTRACT

# COMMAND ----------

bronze_layer_df = read_data(OPTIONS_DICT)

# COMMAND ----------

# MAGIC %md #TRANSFORM

# COMMAND ----------

# DBTITLE 1,add currency
add_currency = bronze_layer_df.withColumn("currency", lit("USD"))

# COMMAND ----------

# DBTITLE 1,add audit date
final_df = add_currency.withColumn("load_date", schedule_date)

# COMMAND ----------

# MAGIC %md ## LOAD

# COMMAND ----------

if load_type == "incremental":
  merge_into_delta_table(final_df, datbase_name, target_table_name, MERGE_COLUMNS)
else:
  overwrite_delta_table(final_df, database_name, target_table_name, add_new_columns)
