# Databricks notebook source
# MAGIC %run /Workspace/Shared/Retail_Data_Pipeline/_config/config_paths

# COMMAND ----------

# MAGIC %run /Workspace/Shared/Retail_Data_Pipeline/_config/config_tables

# COMMAND ----------

# MAGIC %run /Workspace/Shared/Retail_Data_Pipeline/_utils/common_functions

# COMMAND ----------

from pyspark.sql.types import *

customer_schema = StructType([
    StructField("customer_id", IntegerType()),
    StructField("customer_name", StringType()),
    StructField("city", StringType())
])

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(customer_schema)
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_CUSTOMERS}/schema")
    .load(RAW_CUSTOMERS_INCREMENTAL)
)

# COMMAND ----------

df_stream_meta = (
    df_stream
    .withColumn("ingestion_ts", current_timestamp().cast(TimestampType()))
    .withColumn("source_file", col("_metadata.file_path").cast(StringType()))
)


# COMMAND ----------

(
    df_stream_meta
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_CUSTOMERS)
    .trigger(availableNow=True)
    .toTable(BRONZE_CUSTOMERS)
)
