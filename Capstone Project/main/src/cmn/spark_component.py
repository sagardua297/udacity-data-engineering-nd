from pyspark.sql import SparkSession
from spark_data_transform import DataPipelineTransform
from s3_component import DataPipelineS3Module
from pathlib import Path
import configparser
from warehouse.warehouse_component import DataPipelineWarehouse
import time

# Setup configurations
config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/s3_config.cfg"))

def create_sparksession():
    """
    Initialize a spark session
    """
    return SparkSession.builder.master('yarn').appName("datapipelineanalytics") \
           .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11") \
           .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2") \
           .enableHiveSupport().getOrCreate()

def main():
    """
    This method performs below tasks:
        1: Check for data in Ground Store, if new files are present move them to Processing Store.
        2: Transform data present in Processing Store and save the transformed data to Final Store.
        3: Run Data Warehouse functionality by setting up Staging and Warehouse tables, then loading staging and warehouse tables, and finally performing upsert operations on warehouse tables.
    """
    spark = create_sparksession()
    dpat = DataPipelineTransform(spark)

    mod_list = {
        "author.csv" : dpat.transform_author_dataset,
        "book.csv" : dpat.transform_books_dataset,
        "reviews.csv" : dpat.transform_reviews_dataset,
        "user.csv" : dpat.tranform_users_dataset
    }

    dpas3 = DataPipelineS3Module()
    dpas3.s3_move_data(source_bucket= config.get('BUCKET','GROUND_STORE'), target_bucket= config.get('BUCKET', 'PROCESSING_STORE'))

    files_in_processing_store = dpas3.get_files(config.get('BUCKET', 'PROCESSING_STORE'))

    # Cleanup Final Store bucket if files available in Processing Store
    if len([set(mod_list.keys()) & set(files_in_processing_store)]) > 0:
        dpas3.clean_bucket(config.get('BUCKET', 'FINAL_STORE'))

    for file in files_in_processing_store:
        if file in mod_list.keys():
            mod_list[file]()

    time.sleep(5)

    # Execute Warehouse logic
    dpawarehouse = DataPipelineWarehouse()
    dpawarehouse.setup_staging_tables()
    dpawarehouse.load_staging_tables()
    dpawarehouse.setup_warehouse_tables()
    dpawarehouse.perform_data_process()

# Entry point for the pipeline
if __name__ == "__main__":
    main()
