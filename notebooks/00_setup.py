# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS bank_lakehouse;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bank_lakehouse.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS bank_lakehouse.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS bank_lakehouse.gold;
