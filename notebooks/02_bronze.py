# Databricks notebook source
from pyspark.sql.functions import col
import re

raw_path = s3_raw_path = "s3://bank-loan-lakehouse/raw/bank-loans/loan-clean.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_path)
)

def clean_column_name(name):
    name = name.strip().lower()
    name = re.sub(r"[ ,;{}()\n\t=]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")

bronze_df = df.select([
    col(c).alias(clean_column_name(c))
    for c in df.columns
])

bronze_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bank_lakehouse.bronze.bank_loans")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bank_lakehouse.bronze.bank_loans
# MAGIC LIMIT 10;
