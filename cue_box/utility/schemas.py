# Databricks notebook source
# MAGIC %md ## Schemas

# COMMAND ----------

# DBTITLE 1,Users
user_schema = StructType([
    StructField("user_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("email", StringType(), nullable=False),
    StructField("password", StringType(), nullable=False),
    StructField("phone", StringType(), nullable=False),
])

# COMMAND ----------

# DBTITLE 1,Address
address_schema = StructType([
    StructField("address_id", IntegerType(), nullable=False),
    StructField("user_id", IntegerType(), nullable=False),
    StructField("state", StringType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("address", StringType(), nullable=False),
])

# COMMAND ----------

# DBTITLE 1,Resturarnt
resturant_schema = StructType([
    StructField("resturant_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("address", StringType(), nullable=False),
    StructField("phone", StringType(), nullable=False)
])

# COMMAND ----------

# DBTITLE 1,Menu
menu_schema = StructType([
    StructField("menu_id", IntegerType(), nullable=False),
    StructField("resturant_id", IntegerType(), nullable=False),
    StructField("item_name", StringType(), nullable=False),
    StructField("price", StringType(), nullable=False)
])

# COMMAND ----------

# DBTITLE 1,Drivers
driver_schema = StructType([
    StructField("driver_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("phone", StringType(), nullable=False),
    StructField("state", StringType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("email", StringType(), nullable=False)
])

# COMMAND ----------

# DBTITLE 1,Ratings
rating_schema = StructType([
    StructField("rating_id", IntegerType(), nullable=False),
    StructField("user_id", IntegerType(), nullable=False),
    StructField("resturant_id", IntegerType(), nullable=False),
    StructField("raitings", StringType(), nullable=False)
])

# COMMAND ----------

# DBTITLE 1,Orders
orders_schema = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("user_id", IntegerType(), nullable=False),
    StructField("resturant_id", IntegerType(), nullable=False),
    StructField("order_total", StringType(), nullable=False),
    StructField("delivery_status", StringType(), nullable=False),
    StructField("driver_id", IntegerType(), nullable=False),
])

# COMMAND ----------

# DBTITLE 1,Payments
payments_schema = StructType([
    StructField("payment_id", IntegerType(), nullable=False),
    StructField("order_id", IntegerType(), nullable=False),
    StructField("payment_method", StringType(), nullable=False),
    StructField("amount", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False)
])

# COMMAND ----------

# MAGIC %md ## Mege Columns

# COMMAND ----------

user_merge_columns = ["user_id"]

# COMMAND ----------

address_merge_columns = ["address_id"]

# COMMAND ----------

resturarnt_merge_columns = ["resturarnt_id"]

# COMMAND ----------

menu_merge_columns = ["menu_id"]

# COMMAND ----------

driver_merge_columns = ["driver_id"]

# COMMAND ----------

ratings_merge_columns = ["rating_id"]

# COMMAND ----------

orders_merge_columns = ["order_id"]

# COMMAND ----------

payments_merge_columns = ["payments_id"]
