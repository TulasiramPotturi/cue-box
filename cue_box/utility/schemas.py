# Databricks notebook source
# MAGIC %md ## Schemas

# COMMAND ----------

# DBTITLE 1,Users
user_schema = StructType([
    StructField("user_id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("email", StringType(), nullable=False),
    StructField("password", StringType(), nullable=False),
    StructField("phone", StringType(), nullable=False),
])

# COMMAND ----------

# DBTITLE 1,Address
address_schema = StructType([
    StructField("address_id", LongType(), nullable=False),
    StructField("user_id", LongType(), nullable=False),
    StructField("state", StringType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("address", StringType(), nullable=False),
])

# COMMAND ----------

# DBTITLE 1,Resturarnt
restaurants_schema = StructType([
    StructField("resturant_id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("address", StringType(), nullable=False),
    StructField("phone", StringType(), nullable=False)
])

# COMMAND ----------

# DBTITLE 1,Menu
menu_schema = StructType([
    StructField("menu_id", LongType(), nullable=False),
    StructField("resturant_id", LongType(), nullable=False),
    StructField("item_name", StringType(), nullable=False),
    StructField("price", StringType(), nullable=False)
])

# COMMAND ----------

# DBTITLE 1,Drivers
driver_schema = StructType([
    StructField("driver_id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("email", StringType(), nullable=False),
    StructField("state", StringType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("phone", StringType(), nullable=False)
])

# COMMAND ----------

# DBTITLE 1,Ratings
rating_schema = StructType([
    StructField("rating_id", LongType(), nullable=False),
    StructField("order_id", LongType(), nullable=False),
    StructField("user_id", LongType(), nullable=False),
    StructField("resturant_id", LongType(), nullable=False),
    StructField("raitings", StringType(), nullable=False)
])

# COMMAND ----------

# DBTITLE 1,Orders
orders_schema = StructType([
    StructField("order_id", LongType(), nullable=False),
    StructField("order_placed_date", StringType(), nullable=False),
    StructField("user_id", LongType(), nullable=False),
    StructField("resturant_id", LongType(), nullable=False),
    StructField("payment_id", LongType(), nullable=False),
    StructField("delivery_status", StringType(), nullable=False),
    StructField("driver_id", LongType(), nullable=False),
    StructField("estimated_delivery_time", IntegerType(), nullable=False),
    StructField("delivered_time", IntegerType(), nullable=False)
])

# COMMAND ----------

# DBTITLE 1,Payments
payments_schema = StructType([
    StructField("payment_id", LongType(), nullable=False),
    StructField("payment_date", StringType(), nullable=False),
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

restaurants_merge_columns = ["restaurant_id"]

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
