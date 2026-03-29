# Databricks notebook source
# MAGIC %run /Workspace/Shared/Retail_Data_Pipeline/_config/config_paths

# COMMAND ----------

# MAGIC %run /Workspace/Shared/Retail_Data_Pipeline/_config/config_tables

# COMMAND ----------

# MAGIC %run /Workspace/Shared/Retail_Data_Pipeline/_utils/common_functions

# COMMAND ----------

df_products_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true") 
    .csv(RAW_PRODUCTS)
)

df_products_bronze = add_ingestion_metadata(df_products_raw)

(
    df_products_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(BRONZE_PRODUCTS)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECt * from retail_catalog.BRONZE.products limit 5