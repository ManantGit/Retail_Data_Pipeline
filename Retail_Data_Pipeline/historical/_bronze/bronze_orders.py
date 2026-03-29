# Databricks notebook source
# MAGIC %run /Workspace/Shared/Retail_Data_Pipeline/_config/config_paths

# COMMAND ----------

# MAGIC %run /Workspace/Shared/Retail_Data_Pipeline/_config/config_tables

# COMMAND ----------

# MAGIC %run /Workspace/Shared/Retail_Data_Pipeline/_utils/common_functions

# COMMAND ----------

df_orders_raw = (
  spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(RAW_ORDERS_FULL)
)

df_orders_bronze = add_ingestion_metadata(df_orders_raw)

(
    df_orders_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(BRONZE_ORDERS)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from retail_catalog.bronze.orders limit 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from retail_catalog.bronze.customers limit 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from retail_catalog.bronze.products limit 2;

# COMMAND ----------

spark.readStream