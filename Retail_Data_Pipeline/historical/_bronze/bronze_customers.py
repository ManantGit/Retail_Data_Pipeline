# Databricks notebook source
# MAGIC %run /Workspace/Shared/Retail_Data_Pipeline/_config/config_paths

# COMMAND ----------

# MAGIC %run /Workspace/Shared/Retail_Data_Pipeline/_config/config_tables

# COMMAND ----------

# MAGIC %run /Workspace/Shared/Retail_Data_Pipeline/_utils/common_functions

# COMMAND ----------

df_customers_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true") 
    .csv("s3://retail-analytics-pipeline/Raw/full_load/customers/*.csv")
)

df_customers_bronze = add_ingestion_metadata(df_customers_raw)

(
    df_customers_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(BRONZE_CUSTOMERS)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECt * from retail_catalog.BRONZE.customers limit 5