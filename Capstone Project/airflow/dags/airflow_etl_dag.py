from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.datapipeline_plugin import DataQualityOperator
from airflow.operators.datapipeline_plugin import DataAnalyticsOperator
from helpers import AnalyticQueries

# Define DAG args
default_args = {
    'owner': 'datapipelineanalytics',
    'depends_on_past': True,
    'start_date' : datetime(2021, 2, 9, 0, 0, 0, 0),
    'end_date' : datetime(2021, 2, 10, 0, 0, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': True
}

# Define DAG
dag_name = 'data_pipeline'
dag = DAG(
    dag_name,
    default_args=default_args,
    description='Load and Transform data from Ground Store to Final Store. Populate data from Final Store to the Warehouse.',
    schedule_interval='*/10 * * * *',
    max_active_runs = 1
)

# Start DAG operation
start_operator = DummyOperator(task_id='Start_Execution', dag=dag)

# Define SSH Hook for EMR SSH connection
emr_ssh_hook= SSHHook(
    ssh_conn_id='emr_ssh_connection'
)

# Using SSH hook, use the SSH operator to enable SSH connection for submitting Spark jobs to EMR cluster
jobOperator = SSHOperator(
    task_id="DataPipelineETLJob",
    command='cd /home/hadoop/data_pipeline_analytics/main/src/cmn;export PYSPARK_DRIVER_PYTHON=python3;export PYSPARK_PYTHON=python3;spark-submit --master yarn spark_component.py;',
    ssh_hook=emr_ssh_hook,
    dag=dag
)

# Using Redshift connection, perform Data Quality checks on Wareshouse schema tables
warehouse_data_quality_checks = DataQualityOperator(
    task_id='Warehouse_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables = ["data_warehouse.authors", "data_warehouse.reviews", "data_warehouse.books", "data_warehouse.users"]
)

# Using Redshift connection, create Analytics schema
create_analytics_schema = DataAnalyticsOperator(
    task_id='Create_analytics_schema',
    redshift_conn_id = 'redshift',
    sql_query = [AnalyticQueries.create_analytics_schema],
    dag=dag
)

# Using Redshift connection, create Author based Analytic tables in Anaytics schema
create_author_analytics_table = DataAnalyticsOperator(
    task_id='Create_author_analytics_table',
    redshift_conn_id = 'redshift',
    sql_query = [AnalyticQueries.create_author_reviews,AnalyticQueries.create_author_rating, AnalyticQueries.create_best_authors],
    dag=dag
)

# Using Redshift connection, create Book based Analytic tables in Anaytics schema
create_book_analytics_table = DataAnalyticsOperator(
    task_id='Create_book_analytics_table',
    redshift_conn_id = 'redshift',
    sql_query = [AnalyticQueries.create_book_reviews,AnalyticQueries.create_book_rating, AnalyticQueries.create_best_books],
    dag=dag
)

# Using Redshift connection, populate Author based Analytic table in Anaytics schema
load_author_table_reviews = DataAnalyticsOperator(
    task_id='Load_author_table_reviews',
    redshift_conn_id = 'redshift',
    sql_query = [AnalyticsQueries.populate_authors_reviews.format('2021-02-09 00:00:00.000000', '2021-02-28 00:00:00.000000')],
    dag=dag
)

# Using Redshift connection, populate Author based Analytic table in Anaytics schema
load_author_table_ratings = DataAnalyticsOperator(
    task_id='Load_author_table_ratings',
    redshift_conn_id = 'redshift',
    sql_query = [AnalyticQueries.populate_authors_ratings.format('2021-02-09 00:00:00.000000', '2021-02-28 00:00:00.000000')],
    dag=dag
)

# Using Redshift connection, populate Author based Analytic table in Anaytics schema
load_best_author = DataAnalyticsOperator(
    task_id='Load_best_author',
    redshift_conn_id = 'redshift',
    sql_query = [AnalyticQueries.populate_best_authors],
    dag=dag
)

# Using Redshift connection, populate Book based Analytic table in Anaytics schema
load_book_table_reviews = DataAnalyticsOperator(
    task_id='Load_book_table_reviews',
    redshift_conn_id = 'redshift',
    sql_query = [AnalyticQueries.populate_books_reviews.format('2021-02-09 00:00:00.000000', '2021-02-28 00:00:00.000000')],
    dag=dag
)

# Using Redshift connection, populate Book based Analytic table in Anaytics schema
load_book_table_ratings = DataAnalyticsOperator(
    task_id='Load_book_table_ratings',
    redshift_conn_id = 'redshift',
    sql_query = [AnalyticQueries.populate_books_ratings.format('2021-02-09 00:00:00.000000', '2021-02-28 00:00:00.000000')],
    dag=dag
)

# Using Redshift connection, populate Book based Analytic table in Anaytics schema
load_best_book = DataAnalyticsOperator(
    task_id='Load_best_books',
    redshift_conn_id = 'redshift',
    sql_query = [AnalyticQueries.populate_best_books],
    dag=dag
)

# Using Redshift connection, perform Data Quality checks on Author based Analytic table in Analytics schema
authors_data_quality_checks = DataQualityOperator(
    task_id='Authors_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables = ["data_analytics.popular_authors_average_rating", "data_analytics.popular_authors_average_rating"]
)

# Using Redshift connection, perform Data Quality checks on Books based Analytic table in Analytics schema
books_data_quality_checks = DataQualityOperator(
    task_id='Books_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables = ["data_analytics.popular_books_average_rating", "data_analytics.popular_books_review_count"]
)

# End DAG operation
end_operator = DummyOperator(task_id='End_Execution', dag=dag)

# Define DAG flow
start_operator >> jobOperator >> warehouse_data_quality_checks >> create_analytics_schema
create_analytics_schema >> [create_author_analytics_table, create_book_analytics_table]
create_author_analytics_table >> [load_author_table_reviews, load_author_table_ratings, load_best_author] >> authors_data_quality_checks
create_book_analytics_table >> [load_book_table_reviews, load_book_table_ratings, load_best_book] >> books_data_quality_checks
[authors_data_quality_checks, books_data_quality_checks] >> end_operator
