# Databricks notebook source
from pyspark.sql.functions import col, when

bronze_df = spark.table("bank_lakehouse.bronze.bank_loans")

silver_df = (
    bronze_df
    .dropDuplicates()
    .withColumn("id", col("id").cast("int"))
    .withColumn("age", col("age").cast("int"))
    .withColumn("experience", col("experience").cast("int"))
    .withColumn("income", col("income").cast("double"))
    .withColumn("zip_code", col("zip_code").cast("string"))
    .withColumn("family", col("family").cast("int"))
    .withColumn("ccavg", col("ccavg").cast("double"))
    .withColumn("education", col("education").cast("int"))
    .withColumn("mortgage", col("mortgage").cast("double"))
    .withColumn("personal_loan", col("personal_loan").cast("int"))
    .withColumn("securities_account", col("securities_account").cast("int"))
    .withColumn("cd_account", col("cd_account").cast("int"))
    .withColumn("online", col("online").cast("int"))
    .withColumn("creditcard", col("creditcard").cast("int"))

    .withColumn(
        "experience",
        when(col("experience") < 0, 0).otherwise(col("experience"))
    )

    .withColumn(
        "education_level",
        when(col("education") == 1, "Undergraduate")
        .when(col("education") == 2, "Graduate")
        .when(col("education") == 3, "Advanced/Professional")
        .otherwise("Unknown")
    )

    .withColumn(
        "income_group",
        when(col("income") < 50, "Low income")
        .when((col("income") >= 50) & (col("income") < 100), "Medium income")
        .when((col("income") >= 100) & (col("income") < 150), "High income")
        .otherwise("Very high income")
    )

    .withColumn(
        "has_mortgage",
        when(col("mortgage") > 0, 1).otherwise(0)
    )
)

silver_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bank_lakehouse.silver.bank_loans_clean")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bank_lakehouse.silver.bank_loans_clean
# MAGIC LIMIT 10;

# COMMAND ----------

# Check if invalid ages exist
invalid_age_count = silver_df.filter((col("age") < 18) | (col("age") > 100)).count()

# Check if negative experience still exists
negative_experience_count = silver_df.filter(col("experience") < 0).count()

# Check duplicate IDs
duplicate_id_count = (
    silver_df
    .groupBy("id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Invalid ages:", invalid_age_count)
print("Negative experience rows:", negative_experience_count)
print("Duplicate customer IDs:", duplicate_id_count)
