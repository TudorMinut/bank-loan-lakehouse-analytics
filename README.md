# Bank Loan Lakehouse Analytics

A data engineering and analytics project that builds a Lakehouse pipeline for bank loan customer data using **Databricks**, **PySpark**, **SQL**, **AWS S3**, and **Power BI**.

The project takes raw bank client data, processes it through a Bronze-Silver-Gold architecture, creates business-ready analytics tables, and exposes the final results for dashboarding and reporting in Power BI.

---

## Project Overview

The goal of this project is to transform raw customer banking data into useful business insights for future credit and marketing programs.

The pipeline starts from a CSV file containing bank client information such as age, income, education, family size, mortgage value, online banking usage, credit card ownership, and personal loan acceptance. The initial file is provided by Databricks and has the data of 5000 clients.

The data is uploaded to AWS S3, processed in Databricks, stored as Delta tables, and organized into multiple layers:

* **Bronze Layer**: raw ingested data
* **Silver Layer**: cleaned and validated data
* **Gold Layer**: business-ready aggregated tables for reporting
* **Power BI Dashboard**: visual analytics for loan trends, customer segments, and KPIs

---

## Tech Stack

* **Databricks** — used as the main Lakehouse platform for data processing, table management, and job scheduling.
* **PySpark** — used for scalable transformations across the Bronze, Silver, and Gold layers.
* **SQL** — used for table creation, validation queries, and AI-powered customer summaries.
* **AWS S3** — used as cloud storage for the raw CSV file.
* **Delta Lake** — used for reliable table storage inside Databricks.
* **Power BI** — used to build dashboards from the final Gold-layer tables.

PySpark is useful because it can scale to much larger datasets, although for a small dataset of around 5,000 rows it may introduce more complexity than a simple pandas-based workflow. In this project, it is used to demonstrate a realistic cloud data engineering architecture.

---

## Pipeline Architecture Diagram
<img width="1169" height="827" alt="bank_loan_lakehouse_architecture drawio (1)" src="https://github.com/user-attachments/assets/b94de71c-1b8e-457f-b9e3-341aa4209d27" />

A Databricks scheduled job runs the pipeline every day at **08:00**, refreshing the Bronze, Silver, and Gold layers from the latest cloud data.

---

## Pipeline Steps

### 1. Setup

The setup notebook creates the main Databricks catalog and schemas used by the Lakehouse pipeline:

* `bank_lakehouse.bronze`
* `bank_lakehouse.silver`
* `bank_lakehouse.gold`

This keeps the project organized according to the Medallion Architecture.

---

### 2. Ingestion to AWS S3

The raw bank loan dataset is first copied from a Databricks volume into an AWS S3 bucket.

The S3 bucket acts as the cloud landing zone for the raw CSV file, making the data accessible to the Databricks ETL pipeline.

---

### 3. Bronze Layer

The Bronze layer reads the raw CSV file from S3.

At this stage, the data is kept close to its original format, but column names are standardized so they are easier to query and process later.

The result is saved as a Delta table:

```text
bank_lakehouse.bronze.bank_loans
```

---

### 4. Silver Layer

The Silver layer cleans and prepares the data for analytics.

Main transformations include:

* Removing duplicate records
* Casting columns to correct data types
* Fixing negative experience values
* Creating readable education categories
* Creating income groups
* Creating a mortgage ownership flag
* Running basic data quality checks

The cleaned table is saved as:

```text
bank_lakehouse.silver.bank_loans_clean
```

Examples of added fields:

* `education_level`
* `income_group`
* `has_mortgage`


---

### 5. Gold Layer

The Gold layer creates business-ready tables that can be used directly in Power BI.

The project creates analytics tables such as:

* `loan_acceptance_by_income_group`
* `loan_acceptance_by_education`
* `loan_acceptance_by_family_size`
* `loan_acceptance_by_digital_engagement`
* `customer_segment_summary`
* `customer_profile_summaries`
* `kpi_overview`

These tables help answer business questions such as:

* Which income groups are most likely to accept a personal loan?
* How does education level affect loan acceptance?
* Are online banking users more likely to use credit products?
* Which customer segments are most valuable for future campaigns?
* What are the main KPIs of the customer base?


*Example: taking the data from the Silver Layer and transforming it in a column determing how digital engaged is a client with the products of the bank:*
```
python
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
```
---

## AI-Powered Customer Summaries

The Gold layer also uses Databricks AI functionality through `ai_gen()` to generate short customer profile summaries.

These summaries are designed to help bank employees quickly understand a customer’s financial profile without manually reading every column.

For each customer, the summary considers attributes such as:

* Age
* Income
* Education level
* Family size
* Personal loan status
* Online banking usage
* Credit card ownership
* Average credit card spending
* Mortgage status

This adds an AI-assisted layer on top of the analytical pipeline and makes the project more practical for CRM, sales, and marketing use cases.

*Example of summary:*
"The customer is a 47-year-old individual with a stable income of $105,000 per year, holding an undergraduate degree. They have a small family size of 2, indicating relatively low dependents. Notably, they do not have any personal loans or mortgages, suggesting a conservative approach to debt. However, they do have a credit card with an average annual spend of $3,300, although they do not use online banking, preferring traditional banking methods. Overall, the customer appears to be financially responsible with a moderate spending habit."

---

## Power BI Dashboard

The final Gold tables are connected to Power BI for visualization.

The dashboard can include:

* Loan acceptance rate by income group
* Loan acceptance rate by education level
* Customer distribution by family size
* Digital engagement analysis
* High-value customer segments
* KPI overview
* AI-generated customer profile summaries

The goal of the dashboard is to help business users understand customer behavior and identify groups that may be suitable for future credit programs.

---

## Job orchestration in Databricks

The Databricks workflow is scheduled to run daily at **08:00**.

This means that the pipeline automatically refreshes the Bronze, Silver, and Gold layers from the data stored in AWS S3.

This simulates a real-world production data pipeline where dashboards are updated regularly with the latest available data.

---

## Possible Improvements

Future improvements could include:
* More AI powered functionalities
* Saving the ETL layers in AWS instead of Databricks?
* Ingesting data from Apache Kafka
* Adding more advanced data quality checks
* Using Databricks Auto Loader for incremental ingestion
* Adding historical snapshots instead of overwriting tables
* Adding unit tests or validation notebooks
* Adding CI/CD for Databricks notebooks




