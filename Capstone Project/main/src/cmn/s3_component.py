import boto3
import configparser
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/s3_config.cfg"))

class DataPipelineS3Module:
    """
    This class performs the following:
        1. Copy input files from Source to Target bucket.
        2. Clean bucket when required.
    """

    def __init__(self):
        """
        Setup config for S3 buckets.
        """
        self._s3 = boto3\
                    .resource(service_name = 's3', region_name = 'us-west-2', aws_access_key_id=config.get('AWS', 'KEY'), aws_secret_access_key=config.get('AWS', 'SECRET'))
        self._files = []
        self._ground_store = config.get('BUCKET','GROUND_STORE')
        self._processing_store = config.get('BUCKET','PROCESSING_STORE')
        self._final_store = config.get('BUCKET','FINAL_STORE')

    def s3_move_data(self, source_bucket = None, target_bucket= None):
        """
        Detect files in source bucket and move those files to target bucket.
        :param source_bucket: name of source bucket
        :param target_bucket: name of target bucket
        """

        # If no argument passed default to the project related landing zone and working zone
        if source_bucket is None:
            source_bucket = self._ground_store
        if target_bucket is None:
            target_bucket = self._processing_store

        logging.debug(f"Inside s3_move_data : Source bucket set is : {source_bucket}\n Target bucket set is : {target_bucket}")

        # Cleanup Target bucket
        self.clean_bucket(target_bucket)

        # Move files from Source to Target bucket
        for key in self.get_files(source_bucket):
            if key in config.get('FILES','NAME').split(","):
                logging.debug(f"Copying file {key} from {source_bucket} to {target_bucket}")
                self._s3.meta.client.copy({'Bucket': source_bucket, 'Key': key}, target_bucket, key)

    def get_files(self, bucket_name):
        """
        Get all the files present in the provided bucket.
        :param bucket_name: bucket to search
        :return: keys or files present in the bucket
        """
        logging.debug(f"Inspecting bucket : {bucket_name} for files present")
        return [bucket_object.key for bucket_object in self._s3.Bucket(bucket_name).objects.all()]

    def clean_bucket(self, bucket_name):
        """
        Clean the bucket and delete all files.
        :param bucket_name: bucket name, bucket to clean
        """
        logging.debug(f"Cleaning bucket : {bucket_name}")
        self._s3.Bucket(bucket_name).objects.all().delete()
