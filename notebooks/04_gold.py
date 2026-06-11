# Databricks notebook source
# MAGIC %md
# MAGIC loan_acceptance_by_income_group
# MAGIC loan_acceptance_by_education
# MAGIC loan_acceptance_by_family_size
# MAGIC loan_acceptance_by_digital_engagement
# MAGIC customer_segment_summary
# MAGIC kpi_overview

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
        round(avg("income"), 2).alias("avg_income"),
        round(avg("ccavg"), 2).alias("avg_credit_card_spend")
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
        round(avg("income"), 2).alias("avg_income"),
        round(avg("ccavg"), 2).alias("avg_credit_card_spend")
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
        round(avg("income"), 2).alias("avg_income"),
        round(avg("mortgage"), 2).alias("avg_mortgage"),
        round(avg("ccavg"), 2).alias("avg_credit_card_spend")
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
        round(avg("income"), 2).alias("avg_income"),
        round(avg("ccavg"), 2).alias("avg_credit_card_spend")
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
        round(avg("income"), 2).alias("avg_income"),
        round(avg("ccavg"), 2).alias("avg_credit_card_spend"),
        round(avg("mortgage"), 2).alias("avg_mortgage"),
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
        round(avg("income"), 2).alias("avg_income"),
        round(avg("ccavg"), 2).alias("avg_credit_card_spend"),
        round(avg("mortgage"), 2).alias("avg_mortgage"),
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

# MAGIC %sql
# MAGIC SHOW TABLES IN bank_lakehouse.gold;
