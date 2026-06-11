# Databricks notebook source
volume_path = "/Volumes/databricks_bank_loan_modelling_dataset/v01/banking/loan-clean.csv"
s3_raw_path = "s3://bank-loan-lakehouse/raw/bank-loans/loan-clean.csv"

dbutils.fs.cp(
    volume_path,
    s3_raw_path,
    recurse=True
)
