# Databricks notebook source
# Base paths
RAW_BASE_PATH = "s3://retail-analytics-pipeline/Raw"
BRONZE_BASE_PATH = "s3://retail-analytics-pipeline/bronze"
SILVER_BASE_PATH = "s3://retail-analytics-pipeline/silver"
GOLD_BASE_PATH = "s3://retail-analytics-pipeline/gold"

# Raw paths
RAW_CUSTOMERS = f"{RAW_BASE_PATH}/full_load/customers/*.csv"
RAW_PRODUCTS = f"{RAW_BASE_PATH}/full_load/products/*.csv"
RAW_GROSS_PRICE = f"{RAW_BASE_PATH}/full_load/gross_price/*.csv"
RAW_ORDERS_FULL = f"{RAW_BASE_PATH}/full_load/orders/landing/*.csv"
RAW_ORDERS_INCREMENTAL = f"{RAW_BASE_PATH}/incremental/orders/*.csv"

# Incremental paths

RAW_CUSTOMERS_INCREMENTAL = "s3://retail-analytics-pipeline/Raw/incremental_load/customers/"
CHECKPOINT_CUSTOMERS = "s3://retail-analytics-pipeline/checkpoints/bronze/customers/"
