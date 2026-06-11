E-Commerce Data Engineering Pipeline
Overview
This project implements a modern AWS-based data lake architecture for processing e-commerce transactional data. The pipeline ingests raw JSON datasets from Amazon S3, transforms and enriches the data through multiple processing layers, and produces analytics-ready datasets for reporting and business intelligence.
The solution follows the Medallion Architecture (Landing → Bronze → Silver → Gold) pattern commonly used in enterprise data platforms.

Business Objective
The primary objective of this project is to transform raw operational e-commerce data into curated analytical datasets that support:
Sales performance reporting
Product analytics
Customer analytics
Executive dashboards
Ad-hoc analytical queries
The pipeline enables stakeholders to analyze customer purchasing behavior, product performance, revenue trends, and operational metrics through a structured and scalable data platform.

Solution Architecture
Data Flow
Landing Layer → Bronze Layer → Silver Layer → Gold Layer → Athena → Dashboard
Landing Layer
Raw JSON data is ingested into Amazon S3 without modifications.
Data Sources:
Carts
Products
Users
Purpose:
Preserve source system data
Enable reprocessing when required
Maintain a historical raw data archive

Bronze Layer
The Bronze layer performs initial cleansing and schema standardization.
Transformations:
Data type standardization
Column selection
Sensitive data masking using SHA-256 hashing
Nested JSON flattening
Entity separation
Generated Tables:
Table	Description
bronze_cart_header	Cart transaction headers
bronze_cart_items	Cart line items
bronze_products	Product master data
bronze_reviews	Product reviews
bronze_users	User profile information
bronze_user_companies	User company information
bronze_user_locations	User geographic information
Storage Format:
Apache Parquet
Snappy Compression

Silver Layer
The Silver layer integrates related business entities and applies business-level transformations.
Transformations:
Entity consolidation
Review aggregation
Relationship modeling
Business-ready datasets
Generated Tables:
silver_user_profiles
Combines:
User information
Company information
Location information
silver_products
Combines:
Product information
Aggregated review metrics
Additional Metrics:
Average Product Rating
Review Count
silver_orders
Combines:
Cart headers
Cart items
Product attributes
Additional Business Attributes:
Category
Brand

Gold Layer
The Gold layer implements a dimensional model optimized for analytics workloads.
Data Model:
Fact Table
fact_sales
Measures:
Sales Amount
Quantity Sold
Discount Amount
Total Products
Total Quantity
Foreign Keys:
User Key
Product Key

Dimension Tables
dim_users
Contains:
User demographics
Location information
Company information
dim_products
Contains:
Product details
Category
Brand
Product ratings

Technology Stack
Component	Technology
Storage	Amazon S3
Processing	AWS Glue
Compute Engine	Apache Spark
Query Engine	Amazon Athena
Language	Python
Version Control	Git
Repository Hosting	GitHub
Data Format	Parquet
Compression	Snappy

Project Structure
Ecommerce-Data-Engineering/
│
├── data_pipelines/
│   ├── landing_to_bronze.py
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
│
├── diagrams/
│   └── architecture.png
│
├── docs/
│   └── project_notes.md
│
├── requirements.txt
│
└── README.md

Data Pipeline Execution Flow
Step 1: Landing to Bronze
AWS Glue Job:
landing_to_bronze.py
Responsibilities:
Read raw JSON data
Flatten nested structures
Apply schema enforcement
Mask sensitive information
Write curated parquet files
Output:
s3://ecommerce-bronze90/

Step 2: Bronze to Silver
AWS Glue Job:
bronze_to_silver.py
Responsibilities:
Join related datasets
Aggregate review metrics
Build business entities
Output:
s3://ecommerce-silver90/

Step 3: Silver to Gold
AWS Glue Job:
silver_to_gold.py
Responsibilities:
Build star schema
Create fact table
Create dimension tables
Output:
s3://ecommerce-gold90/

Data Security
The pipeline applies hashing to personally identifiable information (PII) before storage.
Protected Attributes:
Email Address
Phone Number
IP Address
MAC Address
SSN
EIN
Cryptocurrency Wallet Information
Hashing Algorithm:
SHA-256

Query Layer
Amazon Athena is used to query Gold datasets directly from Amazon S3.
Typical Analytics Queries:
Total Sales by Product
Revenue by Category
Top Customers
Product Rating Analysis
Sales Trend Analysis

Performance Optimizations
Implemented optimizations include:
Parquet storage format
Snappy compression
Column pruning
Distributed Spark processing
Aggregation pushdown
Dimensional modeling for analytics

Future Enhancements
Potential improvements include:
Incremental processing
AWS Glue Workflows
Data Quality Validation Framework
CI/CD Pipeline Deployment
Infrastructure as Code (Terraform)
Dashboard Integration with Amazon QuickSight
Real-time Streaming Architecture

Author
Stephen Njoroge
Data Engineering Portfolio Project
AWS | Spark | Glue | Athena | Python
