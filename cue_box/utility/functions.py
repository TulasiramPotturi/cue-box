# Databricks notebook source
# MAGIC %md ##IMPORTS

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Repos/potturi.tulasiram@diggibyte.com/cue-box/cue_box/utility/schemas

# COMMAND ----------

# MAGIC %md ## UTILITY FUNCTIONS

# COMMAND ----------

# MAGIC %md ### Get Schema

# COMMAND ----------

def get_custom_schema(table_name):
    """
    Takes a string and returns a variable based on the string.

    Parameters:
    - input_string: String value

    Returns:
    - Variable or value associated with the input string
    """
    table_schema_mapping = {
        "users": user_schema,
        "drivers": driver_schema,
        "menu": menu_schema,
        "restaurants": restaurants_schema,
        "address": address_schema,
        "ratings": rating_schema,
        "orders": orders_schema,
        "payments": payments_schema
    }

    #  Return the custom schema associated with the input table
    return table_schema_mapping.get(table_name, None)

# COMMAND ----------

# MAGIC %md ###Get Merge Column List

# COMMAND ----------

def get_merge_column(table_name):
    """
    Takes a string and returns a variable based on the string.

    Parameters:
    - input_string: String value

    Returns:
    - Variable or value associated with the input string
    """
    table_merge_columns_mapping = {
        "users": user_merge_columns,
        "drivers": driver_merge_columns,
        "menu": menu_merge_columns,
        "restaurants": restaurants_merge_columns,
        "address": address_merge_columns,
        "ratings": ratings_merge_columns,
        "orders": orders_merge_columns,
        "payments": payments_merge_columns
    }

    # Return the merge columns associated with the input table
    return table_merge_columns_mapping.get(table_name, None)

# COMMAND ----------

# MAGIC %md ### Replace characters

# COMMAND ----------

def replace_characters(df, replacements):
    """
    Replaces characters in specified columns of a PySpark DataFrame.

    Parameters:
    - df: PySpark DataFrame
    - replacements: Dictionary where keys are column names, and values are pairs (tuples)
                    representing (character_to_replace, replacement)

    Returns:
    - DataFrame with characters replaced according to the specified rules
    """
    for column, (char_to_replace, replacement) in replacements.items():
        # Using regexp_replace to replace characters in the specified column
        df = df.withColumn(column, regexp_replace(col(column), char_to_replace, replacement))

    return df

# COMMAND ----------

# MAGIC %md ### Change Date Format

# COMMAND ----------

def change_date_format(df, date_columns, desired_format):
    """
    Changes the date format of specified columns in a PySpark DataFrame.

    Parameters:
    - df: PySpark DataFrame
    - date_columns: List of columns containing date values
    - desired_format: Desired date format

    Returns:
    - DataFrame with date format changed for specified columns
    """
    # Convert date columns to the desired format
    for date_column in date_columns:
        df = df.withColumn(date_column, date_format(col(date_column), desired_format))

    return df

# COMMAND ----------

# MAGIC %md ### Rename Column Name(s)

# COMMAND ----------

def rename_columns(df, column_mapping):
    """
    Renames columns in a PySpark DataFrame based on a dictionary mapping.

    Parameters:
    - df: PySpark DataFrame
    - column_mapping: Dictionary mapping old column names to new column names

    Returns:
    - DataFrame with columns renamed according to the mapping
    """
    for old_column, new_column in column_mapping.items():
        df = df.withColumnRenamed(old_column, new_column)
    return df

# COMMAND ----------

# MAGIC %md ### Typecast Column(s)

# COMMAND ----------

def typecast_columns(df, column_datatypes):
    """
    Typecasts columns in a PySpark DataFrame based on a dictionary mapping.

    Parameters:
    - df: PySpark DataFrame
    - column_datatypes: Dictionary mapping column names to desired data types

    Returns:
    - DataFrame with columns typecasted according to the mapping
    """
    for column, datatype in column_datatypes.items():
        df = df.withColumn(column, col(column).cast(datatype))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Convert Epoch to Timestamp

# COMMAND ----------

def convert_epoch_columns_to_datetime(data_frame, epoch_columns):
    """
    Converts epoch timestamp columns in a PySpark DataFrame to human-readable date-time format.

    Parameters:
    - data_frame: PySpark DataFrame
    - epoch_columns: List of column names with epoch timestamp values

    Returns:
    - DataFrame with specified epoch timestamp columns converted to date-time format
    """
    for column in epoch_columns:
        # Clean the column values by keeping only numerical digits
        cleaned_column = regexp_replace(col(column), "\\.", "")

        # Use withColumn to apply the conversion to each specified column
        data_frame = data_frame.withColumn(
            column,
            when(cleaned_column != "",  # Ensure the cleaned column is not empty
                 from_unixtime(cleaned_column / 10).cast("timestamp"))
        )

    return data_frame

# COMMAND ----------

# MAGIC %md ##READ FUNCTIONS

# COMMAND ----------

def read_data(options):
    """
    Reads data from CSV, Parquet, or JSON based on the provided options.

    Parameters:
    - options: Dictionary containing options for reading data. Supported options:
        - format: Data format (csv, parquet, json)
        - path: File path or directory containing the data
        - additional_options: Additional options specific to the data format

    Returns:
    - PySpark DataFrame
    """
    supported_formats = ['csv', 'parquet', 'json']

    # Check if the specified format is supported
    file_format = options.get('format').lower()
    if file_format not in supported_formats:
        raise ValueError(f"Unsupported format: {file_format}. Supported formats: {', '.join(supported_formats)}")

    # Extract options
    path = options.get('path')
    custom_schema = options.get('schema')
    additional_options = options.get('additional_options')

    # Read data based on the specified format
    return spark.read.format(file_format).schema(custom_schema).options(**additional_options).load(path)

# COMMAND ----------

# MAGIC %md ##WRITE FUNCTIONS

# COMMAND ----------

# MAGIC %md ### Merge with Delta Table

# COMMAND ----------


def merge_into_delta_table(source_df, database_name, table_name,merge_columns, update_columns=None):
    """
    Merges data from a source DataFrame into a Delta table based on the database and table names.

    Parameters:
    - spark: PySpark SparkSession
    - source_df: Source DataFrame to merge into the Delta table
    - database_name: Name of the database containing the Delta table
    - table_name: Name of the Delta table
    - merge_columns: List of columns to use for the merge condition
    - update_columns: List of columns to update. If None or empty, update all columns.
    - quarantine_path: Path to move records with NULL values in merge columns.

    Returns:
    - None
    """
    not_null_check = " & ".join(f"col('{col}').isNotNull()" for col in merge_columns)
    
    source_df = source_df.dropDuplicates().filter(eval(not_null_check))

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
    # Construct Delta table path based on database and table names
    delta_table_path = f"{database_name}.{table_name}"

    # Read the Delta table using delta.forPath
    delta_table = DeltaTable.forPath(delta_table_path)

    # Construct merge condition based on merge_columns
    merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in merge_columns])

    update_columns = {source_df.columns} if update_columns else {update_columns}
    # Determine update setExprs based on update_columns
    update_set_exprs = {col: f"source.{col}" for col in update_columns}

    delta_table.alias("target").merge(
        source_df.alias("source"),
        merge_condition
    ).whenMatchedUpdate(setExprs=update_set_exprs)\
     .whenNotMatchedInsertAll()\
     .execute()

# COMMAND ----------

# MAGIC %md ### Overwrite Delta Table

# COMMAND ----------

def overwrite_delta_table(source_df, database_name, table_name, table_path, add_new_columns):
  spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
  if add_new_columns:
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
  source_df.write.mode("overwrite").option("overwriteSchema", "true").option("path", table_path).saveAsTable(f"{database_name}.{table_name}")

# COMMAND ----------


