from pyspark.sql import SparkSession
from spark_data_transform import DataPipelineTransform
from s3_component import DataPipelineS3Module
from pathlib import Path
import configparser
from warehouse.warehouse_component import DataPipelineWarehouse
import time

# Setting configurations. Look config.cfg for more details
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
        3: Run Data Warehouse functionality by setting up Staging tables, then loading staging tables, performing upsert operations on warehouse.
    """
    spark = create_sparksession()
    grt = DataPipelineTransform(spark)

    modules = {
        "author.csv" : grt.transform_author_dataset,
        "book.csv" : grt.transform_books_dataset,
        "reviews.csv" : grt.transform_reviews_dataset,
        "user.csv" : grt.tranform_users_dataset
    }

    gds3 = DataPipelineS3Module()
    gds3.s3_move_data(source_bucket= config.get('BUCKET','GROUND_STORE'), target_bucket= config.get('BUCKET', 'PROCESSING_STORE'))

    files_in_processing_store = gds3.get_files(config.get('BUCKET', 'PROCESSING_STORE'))

    # Cleanup Final Store bucket if files available in Processing Store
    if len([set(modules.keys()) & set(files_in_processing_store)]) > 0:
        gds3.clean_bucket(config.get('BUCKET', 'FINAL_STORE'))

    for file in files_in_processing_store:
        if file in modules.keys():
            modules[file]()

    time.sleep(5)

    # Execute Warehouse logic
    grwarehouse = DataPipelineWarehouse()
    grwarehouse.setup_staging_tables()
    grwarehouse.load_staging_tables()
    grwarehouse.setup_warehouse_tables()
    grwarehouse.perform_data_process()

# Entry point for the pipeline
if __name__ == "__main__":
    main()
