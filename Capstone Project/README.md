# Data Pipeline Analytics
<a href=""><img src="images/Header.png" align="centre" height="400" width="700"></a>

## Architecture

### Data Pipeline Components

Below are high level components of the project:

1. AWS S3

Input data files will be stored in S3 buckets.

Three S3 buckets will be used in the project:
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

This automates the ETL jobs written in Spark. ETL jobs can be scheduled as per requirement.

### Data Pipeline Architecture

Data is provided as input in the form of CSV files obtained from [Kaggle](https://www.kaggle.com/san089/goodreads-dataset). Input data is stored on local disk and then moved to the Ground Store Bucket on AWS S3. 
ETL jobs are written in Spark and scheduled in Airflow to run every 10 minutes to process the input data in Ground Store bucket, and move them to Final Store bucket, from where data is stored into Redshift data warehouse for final processing and running analytic queries.

Below diagram depicts the architecture:

<a href=""><img src="images/Architecture_1.png" align="centre" height="500" width="1200"></a>

### Data Pipeline Flow

Below are steps undertaken during the Data Pipeline flow:

1. Input data files are placed in Ground Store S3 bucket.
2. S3 module of ETL Spark job copies data from Ground Store bucket to Processing Store bucket.
3. Once the data is moved to Processing Store bucket, Spark job is triggered which reads the data from Processing Store bucket and applies transformation. 
4. Dataset is repartitioned and moved to the Final Store bucket.
5. Warehouse module of ETL Spark job picks up data from Final Store bucket and stages it into the Redshift staging tables.
6. Using the Redshift staging tables, UPSERT operation is performed on the Data Warehouse tables to update the dataset.
7. ETL job execution is successfully completed once the Data Warehouse is updated.
8. Airflow DAG runs the data quality check on all Warehouse tables once the ETL job execution is completed.
9. Airflow DAG has Analytics queries configured in a Custom Designed Operator. These queries are run and again a Data Quality Check is 
done on some selected Analytics Table.
10. DAG execution completes after these Data Quality check.

## Host Environment Setup

### AWS S3

Create three buckets using AWS UI. Ensure bucket has Public access.
1. Ground Store
2. Processing Store
3. Final Store

Below diagram depicts S3 buckets created in AWS.

<a href=""><img src="images/aws_s3_buckets.png" align="centre" height="300" width="800"></a>

### AWS Redshift

Create a Redshift cluster using AWS UI. Ensure to select below details during cluster creation.
1. Choose Free Trial.
2. Enter Database Name and Master User Name or choose default values.
3. Enter Master User Password.
4. Under Cluster Permissions -> Select IAM Role from the list -> Click "Add IAM Role". 
5. Click "Create Cluster".

Below diagram depicts Redshift cluster created in AWS.

<a href=""><img src="images/aws_redshift_datawarehouse.png" align="centre" height="200" width="900"></a>

### AWS EMR Cluster

Create a EMR cluster using AWS UI. Ensure to select below details during cluster creation.
1. Enter Cluster name or choose default value.
2. Choose Release "emr-5.20.0". 
3. Select Applications "Spark: Spark 2.4.0 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and Zeppelin 0.8.0".
4. Choose Instance Type "m3.xlarge".
5. Enter Number of Instances "2".
6. Select EC2 key pair with already created key-pair.
7. Click "Create Cluster".

Below diagram depicts EMR cluster created in AWS.

<a href=""><img src="images/aws_emr_cluster.png" align="centre" height="100" width="750"></a>

### Apache Airflow

Confifure Apache Airflow instance either on local setup or on AWS EC2 instance.

Followed GIT [repo](https://github.com/andresionek91/airflow-autoscaling-ecs) to setup Airflow.

## Data Dictionary

Data model consists of 2 schemas in Redshift warehouse. One schema is used for storing Staging data, and the other schema for storing Analytics data.

Refer to below links for more information on the data model for the 2 schemas.
1. Staging Schema
2. Analytics Schema

## Data Pipeline Execution

Ensure Airflow Webserver and Scheduler are running. Airflow UI can be accessed using http://<ip-address>:<configured-port>.

Below diagrams depicts Data Pipeline Airflow DAG.

<a href=""><img src="images/aws_airflow_1.png" align="centre" height="200" width="2000"></a>


<a href=""><img src="images/AirflowDAG.png" align="centre" height="300" width="3500"></a>

## Scenarios

1. If the database size was increased by 100X:


2. To update the database every morning at 7am:


3. If the database needed to be accessed by 100+ people:


