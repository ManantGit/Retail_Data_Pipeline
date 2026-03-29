# Databricks notebook source
from pyspark.sql.functions import current_timestamp, input_file_name, col

def add_ingestion_metadata(df):
    return (
        df
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )
