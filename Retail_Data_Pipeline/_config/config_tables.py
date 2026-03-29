# Databricks notebook source
CATALOG = "retail_catalog"

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

BRONZE_CUSTOMERS = f"{CATALOG}.{BRONZE_SCHEMA}.customers"
BRONZE_PRODUCTS = f"{CATALOG}.{BRONZE_SCHEMA}.products"
BRONZE_GROSS_PRICE = f"{CATALOG}.{BRONZE_SCHEMA}.gross_price"
BRONZE_ORDERS = f"{CATALOG}.{BRONZE_SCHEMA}.orders"
