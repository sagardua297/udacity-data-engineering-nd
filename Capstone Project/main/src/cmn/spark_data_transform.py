from pyspark.sql.types import StringType
from pyspark.sql import functions as fn
import datapipeline_udf
import configparser
from pathlib import Path

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/s3_config.cfg"))

class DataPiplelineTransform:
    """
    This class performs the following transformation operations on the dataset:
        1. Transform timestamp format, clean text part, remove extra spaces etc.
        2. Create a lookup dataframe which contains the id and the timestamp for the latest record.
        3. Join this lookup data frame with original dataframe to get only the latest records from the dataset.
        4. Save the dataset by repartitioning using gzip compression.
    """

    def __init__(self, spark):
        self._spark = spark
        self._load_path = 's3a://' + config.get('BUCKET', 'PROCESSING_STORE')
        self._save_path = 's3a://' + config.get('BUCKET', 'FINAL_STORE')

    def transform_author_dataset(self):
        """
        Transform operations on the Authors dataset.
        """
        author_df = \
            self._spark.read.csv( self._load_path + '/author.csv', header=True, mode='PERMISSIVE',inferSchema=True)

        author_lookup_df = author_df.groupBy('author_id')\
                            .agg(fn.max('create_timestamp').alias('create_timestamp'))
        author_lookup_df.persist()
        fn.broadcast(author_lookup_df)

        deduped_author_df = author_df\
                            .join(author_lookup_df, ['author_id', 'create_timestamp'], how='inner')\
                            .select(author_df.columns) \
                            .withColumn('name', datapipeline_udf.remove_extra_spaces('name'))

        deduped_author_df\
            .repartition(10)\
            .write\
            .csv(path = self._save_path + '/authors/', sep = '|', mode='overwrite', compression='gzip', header=True, timestampFormat = 'yyyy-MM-dd HH:mm:ss.SSS', quote = '"', escape = '"')

    def transform_reviews_dataset(self):
        """
        Transform operations on the Reviews dataset.
        """
        reviews_df = self._spark.read \
                    .csv(self._load_path + '/reviews.csv', header=True, \
                            mode = 'PERMISSIVE', inferSchema=True, quote = "\"", escape = "\"")

        reviews_lookup_df = reviews_df\
                            .groupBy('review_id')\
                            .agg(fn.max('create_timestamp').alias('create_timestamp'))

        reviews_lookup_df.persist()
        fn.broadcast(reviews_lookup_df)

        deduped_reviews_df = reviews_df \
                             .join(reviews_lookup_df, ['review_id', 'create_timestamp'], how='inner')\
                             .select(reviews_df.columns)

        deduped_reviews_df = deduped_reviews_df \
            .withColumn('review_added_date', datapipeline_udf.stringtodatetime('review_added_date')) \
            .withColumn('review_updated_date', datapipeline_udf.stringtodatetime('review_updated_date'))

        deduped_reviews_df\
            .repartition(10)\
            .write\
            .csv(path = self._save_path + '/reviews/', sep = '|', mode='overwrite', compression='gzip', header=True, timestampFormat = 'yyyy-MM-dd HH:mm:ss.SSS', quote = '"', escape = '"')

    def transform_books_dataset(self):
        """
        Transform operations on the Books dataset.
        """
        books_df = self._spark.read.csv(self._load_path + '/book.csv', header=True, mode='PERMISSIVE',
                                  inferSchema=True, quote="\"", escape="\"")

        books_lookup_df = books_df\
                            .groupBy('book_id')\
                            .agg(fn.max('create_timestamp').alias('create_timestamp'))
        books_lookup_df.persist()
        fn.broadcast(books_lookup_df)

        deduped_books_df = books_df\
                           .join(books_lookup_df, ['book_id', 'create_timestamp'], how='inner')\
                           .select(books_df.columns)

        deduped_books_df\
            .repartition(10)\
            .write\
            .csv(path = self._save_path + '/books/', sep = '|', mode='overwrite', compression='gzip', header=True, timestampFormat = 'yyyy-MM-dd HH:mm:ss.SSS', quote = '"', escape = '"')

    def tranform_users_dataset(self):
        """
        Transform operations on the Users dataset.
        """
        users_df = self._spark.read.csv(self._load_path + '/user.csv', header=True, mode='PERMISSIVE',
                                  inferSchema=True, quote="\"", escape="\"")

        users_lookup_df = users_df\
                          .groupBy('user_id')\
                           .agg(fn.max('create_timestamp').alias('create_timestamp'))

        users_lookup_df.persist()
        fn.broadcast(users_lookup_df)

        deduped_users_df = users_df\
                           .join(users_lookup_df, ['user_id', 'create_timestamp'], how='inner')\
                           .select(users_df.columns)

        deduped_users_df\
            .repartition(10)\
            .write\
            .csv(path = self._save_path + '/users/', sep = '|', mode='overwrite', compression='gzip', header=True, timestampFormat = 'yyyy-MM-dd HH:mm:ss.SSS', quote = '"', escape = '"')
