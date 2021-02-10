# Data Pipeline Analytics
<a href=""><img src="images/Header.png" align="centre" height="400" width="700"></a>

## Architecture

### Data Pipeline Components

Below are high level components of the project:

1. AWS S3

Input data files will be stored in S3 buckets.
Three buckets will be used in the project:
a) Ground Store: This bucket stores the input data files.
b) Processintg Store: This bucket stores processed data from input data files present in Ground Store.
c) Final Store: This bucket stores final version of processed input data, ready to be taken to Redshift data warehouse for further processing.

2. AWS EMR Cluster

Performs ETL using cluster to process input data files in Ground Store S3 bucket, and moves the transformed data to Redshift data warehouse Staging tables for further processing.

3. AWS Redshift

Processed data from Final Store bucket is moved to Staging tables Redshift data warehouse by EMR cluster.
Post data validation/transformation, data is now available for analytics in Redshift data warehouse scehema.
There are separate schemas for storing Staging and Analytic tables in Redshift data warehouse.

4. Apache Airflow

This automates the ETL jobs written in Spark.

### Data Pipeline Architecture

Data is provided as input in the form of CSV files obtained from Kaggle. Input data is stored on local disk and then moved to the Ground Store Bucket on AWS S3. 
ETL jobs are written in Spark and scheduled in Airflow to run every 10 minutes to process the input data in Ground Store bucket, and move them to Final Store bucket, from where data is stored into Redshift data warehouse for final processing and running analytic queries.

Below diagram depicts the architecture:

<a href=""><img src="images/Architecture_1.png" align="centre" height="500" width="1200"></a>

### Data Pipeline Flow

## Host Environment Setup

### AWS S3

### AWS Redshift

### AWS EMR Cluster

### Apache Airflow

## Data Pipeline Execution
