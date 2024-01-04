# Databricks notebook source
# MAGIC %md
# MAGIC #  Fact Orders
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

# DBTITLE 1,Common Imports
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
table_name = "fact_" + target_table_name
gold_layer_path = data_lake_base_uri + "/{}/{}/{}/".format("data/gold", database_name, table_name)

# COMMAND ----------

# DBTITLE 1,Constant Common Lists
payment_column_list = ["payment_id", "amount", "status"]
payment_rename_dict = {"amount" : "order_total", "status" : "payment_status"}

address_column_list = ["user_id", "state", "city"]
address_rename_dict = {"state" : "user_state", "city" : "user_city"}

order_rename_dict = {"delivery_status": "order_status"}

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Extract & Transform

# COMMAND ----------

# DBTITLE 1,Get User state & City from dim_users
orders_data   = rename_columns(get_table_data("cue_box", "orders"), order_rename_dict)

payments_data = rename_columns(get_table_data("cue_box", "fact_payments").select(*payment_column_list), payment_rename_dict)

address_data = rename_columns(get_table_data("cue_box", "dim_address").select(*address_column_list), address_rename_dict)

# COMMAND ----------

# DBTITLE 1,Get transaction amount from - fact_payments
get_transaction_amount = orders_data.join(payments_data, ["payment_id"], "left")

# COMMAND ----------

# DBTITLE 1,Get location details from - dim_address
get_location = get_transaction_amount.join(address_data, ["user_id"], "left")

# COMMAND ----------

# DBTITLE 1,Check order status against - payment_status
fix_order_status = get_location.withColumn("order_status", when(col("payment_status") == "Declined", lit("Declined")).otherwise(col("order_status")))

# COMMAND ----------

# MAGIC %md
# MAGIC #Load

# COMMAND ----------

# DBTITLE 1,Load Gold Table
if load_type == "incremental":
    merge_into_delta_table(fix_order_status, database_name, table_name,gold_layer_path)
else:
    overwrite_delta_table(fix_order_status, database_name, table_name,gold_layer_path, add_new_columns)
