# Databricks notebook source
# DBTITLE 1,Gold Layer - Business Analytics Tables
# MAGIC %md
# MAGIC Gold Layer - Business Analytics Tables
# MAGIC
# MAGIC This notebook creates 7 aggregated, business-ready tables for analytics and reporting.
# MAGIC Source: bank_lakehouse.silver.bank_loans_clean
# MAGIC
# MAGIC Gold tables created:
# MAGIC - loan_acceptance_by_income_group: Loan metrics grouped by income ranges
# MAGIC - loan_acceptance_by_education: Loan metrics grouped by education level
# MAGIC - loan_acceptance_by_family_size: Loan metrics grouped by family size
# MAGIC - loan_acceptance_by_digital_engagement: Loan metrics grouped by digital banking usage (online + credit card segments)
# MAGIC - customer_segment_summary: Comprehensive metrics by customer segment (high-income prospects, existing loan customers, etc.)
# MAGIC - customer_profile_summaries: AI-generated customer profiles using ai_gen() function for CRM and marketing use
# MAGIC - kpi_overview: Overall business KPIs and summary metrics

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    count,
    avg,
    sum as spark_sum,
    round,
    when
)

silver_df = spark.table("bank_lakehouse.silver.bank_loans_clean")

# COMMAND ----------

gold_income = (
    silver_df
    .groupBy("income_group")
    .agg(
        count("*").alias("customer_count"),
        spark_sum("personal_loan").alias("loan_accept_count"),
        round(avg("personal_loan") * 100, 2).alias("loan_acceptance_rate_percent"),
        round(avg("income") * 1000, 2).alias("avg_income"),
        round(avg("ccavg") * 1000, 2).alias("avg_credit_card_spend")
    )
)

gold_income.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bank_lakehouse.gold.loan_acceptance_by_income_group")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bank_lakehouse.gold.loan_acceptance_by_income_group;

# COMMAND ----------

gold_education = (
    silver_df
    .groupBy("education_level")
    .agg(
        count("*").alias("customer_count"),
        spark_sum("personal_loan").alias("loan_accept_count"),
        round(avg("personal_loan") * 100, 2).alias("loan_acceptance_rate_percent"),
        round(avg("income") * 1000, 2).alias("avg_income"),
        round(avg("ccavg") * 1000, 2).alias("avg_credit_card_spend")
    )
)

gold_education.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bank_lakehouse.gold.loan_acceptance_by_education")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bank_lakehouse.gold.loan_acceptance_by_education;

# COMMAND ----------

gold_family = (
    silver_df
    .groupBy("family")
    .agg(
        count("*").alias("customer_count"),
        spark_sum("personal_loan").alias("loan_accept_count"),
        round(avg("personal_loan") * 100, 2).alias("loan_acceptance_rate_percent"),
        round(avg("income") * 1000, 2).alias("avg_income"),
        round(avg("mortgage") * 1000, 2).alias("avg_mortgage"),
        round(avg("ccavg") * 1000, 2).alias("avg_credit_card_spend")
    )
)

gold_family.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bank_lakehouse.gold.loan_acceptance_by_family_size")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bank_lakehouse.gold.loan_acceptance_by_family_size;

# COMMAND ----------

digital_df = (
    silver_df
    .withColumn(
        "digital_engagement_segment",
        when((col("online") == 1) & (col("creditcard") == 1), "Online + Credit Card")
        .when((col("online") == 1) & (col("creditcard") == 0), "Online Only")
        .when((col("online") == 0) & (col("creditcard") == 1), "Credit Card Only")
        .otherwise("Low Digital Engagement")
    )
)

gold_digital = (
    digital_df
    .groupBy("digital_engagement_segment")
    .agg(
        count("*").alias("customer_count"),
        spark_sum("personal_loan").alias("loan_accept_count"),
        round(avg("personal_loan") * 100, 2).alias("loan_acceptance_rate_percent"),
        round(avg("income") * 1000, 2).alias("avg_income"),
        round(avg("ccavg") * 1000, 2).alias("avg_credit_card_spend")
    )
)

gold_digital.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bank_lakehouse.gold.loan_acceptance_by_digital_engagement")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bank_lakehouse.gold.loan_acceptance_by_digital_engagement;

# COMMAND ----------

segmented_df = (
    silver_df
    .withColumn(
        "customer_segment",
        when(
            (col("income") >= 100) & 
            (col("online") == 1) & 
            (col("personal_loan") == 0),
            "High-income digital prospect"
        )
        .when(
            (col("personal_loan") == 1),
            "Existing loan customer"
        )
        .when(
            (col("income") >= 100) & 
            (col("has_mortgage") == 1),
            "High-income mortgage customer"
        )
        .when(
            (col("income") < 50),
            "Low-income customer"
        )
        .otherwise("General customer")
    )
)

gold_segments = (
    segmented_df
    .groupBy("customer_segment")
    .agg(
        count("*").alias("customer_count"),
        spark_sum("personal_loan").alias("loan_accept_count"),
        round(avg("personal_loan") * 100, 2).alias("loan_acceptance_rate_percent"),
        round(avg("income") * 1000, 2).alias("avg_income"),
        round(avg("ccavg") * 1000, 2).alias("avg_credit_card_spend"),
        round(avg("mortgage") * 1000, 2).alias("avg_mortgage"),
        round(avg("online") * 100, 2).alias("online_banking_rate_percent"),
        round(avg("creditcard") * 100, 2).alias("credit_card_rate_percent")
    )
)

gold_segments.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bank_lakehouse.gold.customer_segment_summary")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bank_lakehouse.gold.customer_segment_summary;

# COMMAND ----------

gold_kpi = (
    silver_df
    .agg(
        count("*").alias("total_customers"),
        spark_sum("personal_loan").alias("total_loan_acceptances"),
        round(avg("personal_loan") * 100, 2).alias("loan_acceptance_rate_percent"),
        round(avg("income") * 1000, 2).alias("avg_income"),
        round(avg("ccavg") * 1000, 2).alias("avg_credit_card_spend"),
        round(avg("mortgage") * 1000, 2).alias("avg_mortgage"),
        round(avg("online") * 100, 2).alias("online_banking_rate_percent"),
        round(avg("creditcard") * 100, 2).alias("credit_card_rate_percent"),
        round(avg("securities_account") * 100, 2).alias("securities_account_rate_percent"),
        round(avg("cd_account") * 100, 2).alias("cd_account_rate_percent"),
        round(avg("has_mortgage") * 100, 2).alias("mortgage_customer_rate_percent")
    )
)

gold_kpi.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bank_lakehouse.gold.kpi_overview")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bank_lakehouse.gold.kpi_overview;

# COMMAND ----------

# DBTITLE 1,AI-Powered Customer Profile Summaries
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bank_lakehouse.gold.customer_profile_summaries AS
# MAGIC SELECT 
# MAGIC     id,
# MAGIC     age,
# MAGIC     income,
# MAGIC     education_level,
# MAGIC     family,
# MAGIC     personal_loan,
# MAGIC     ai_gen(
# MAGIC         CONCAT(
# MAGIC             'I am bank employee. 
# MAGIC              I want a short description of the financial position of this customer .
# MAGIC              Describe the customer in a short paragraph. 
# MAGIC              The customer has the following attributes ',
# MAGIC             'Age: ', CAST(age AS STRING), ', ',
# MAGIC             'Income: $', CAST(income AS STRING), 'K, ',
# MAGIC             'Education: ', education_level, ', ',
# MAGIC             'Family size: ', CAST(family AS STRING), ', ',
# MAGIC             CASE WHEN personal_loan = 1 THEN 'Has personal loan, ' ELSE 'No personal loan, ' END,
# MAGIC             CASE WHEN online = 1 THEN 'Uses online banking, ' ELSE 'No online banking, ' END,
# MAGIC             CASE WHEN creditcard = 1 THEN 'Has credit card, ' ELSE 'No credit card, ' END,
# MAGIC             'Credit card avg spend: $', CAST(ccavg AS STRING), 'K, ',
# MAGIC             CASE WHEN has_mortgage = 1 THEN CONCAT('Mortgage: $', CAST(mortgage AS STRING), 'K') ELSE 'No mortgage' END
# MAGIC         )
# MAGIC     ) AS profile_summary
# MAGIC FROM bank_lakehouse.silver.bank_loans_clean;

# COMMAND ----------

# DBTITLE 1,View Customer Profile Summaries
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     id,
# MAGIC     age,
# MAGIC     income,
# MAGIC     education_level,
# MAGIC     personal_loan,
# MAGIC     profile_summary
# MAGIC FROM bank_lakehouse.gold.customer_profile_summaries
# MAGIC ORDER BY income DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN bank_lakehouse.gold;
