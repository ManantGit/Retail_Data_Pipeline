# Databricks notebook source
# MAGIC %run /Workspace/Shared/Retail_Data_Pipeline/_config/config_paths

# COMMAND ----------

# MAGIC %run /Workspace/Shared/Retail_Data_Pipeline/_config/config_tables

# COMMAND ----------

# MAGIC %run /Workspace/Shared/Retail_Data_Pipeline/_utils/common_functions

# COMMAND ----------

df_gross_price_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true") 
    .csv(RAW_GROSS_PRICE)
)

df_gross_price_bronze = add_ingestion_metadata(df_gross_price_raw)

(
    df_gross_price_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(BRONZE_GROSS_PRICE)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECt * from retail_catalog.BRONZE.gross_price limit 5