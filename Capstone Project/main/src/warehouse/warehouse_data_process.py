import configparser
from pathlib import Path

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/warehouse_config.cfg"))

staging_schema = config.get('STAGING', 'SCHEMA')
warehouse_schema = config.get('WAREHOUSE', 'SCHEMA')

# Process Authors table
data_process_authors = """
BEGIN TRANSACTION;

DELETE FROM {1}.authors
using {0}.authors
where {1}.authors.author_id = {0}.authors.author_id;

INSERT INTO {1}.authors
SELECT * FROM {0}.authors;

END TRANSACTION ;
COMMIT;
""".format(staging_schema, warehouse_schema)

# Process Reviews table
data_process_reviews = """
BEGIN TRANSACTION;

DELETE FROM {1}.reviews
using {0}.reviews
where {1}.reviews.review_id = {0}.reviews.review_id;

INSERT INTO {1}.reviews
SELECT * FROM {0}.reviews;

END TRANSACTION ;
COMMIT;
""".format(staging_schema, warehouse_schema)

# Process Books table
data_process_books = """
BEGIN TRANSACTION;

DELETE FROM {1}.books
using {0}.books
where {1}.books.book_id = {0}.books.book_id;

INSERT INTO {1}.books
SELECT * FROM {0}.books;

END TRANSACTION ;
COMMIT;
""".format(staging_schema, warehouse_schema)

# Process Users table
data_process_users = """
BEGIN TRANSACTION;

DELETE FROM {1}.users
using {0}.users
where {1}.users.user_id = {0}.users.user_id;

INSERT INTO {1}.users
SELECT * FROM {0}.users;

END TRANSACTION ;
COMMIT;
""".format(staging_schema, warehouse_schema)

data_process_queries = [data_process_authors, data_process_reviews, data_process_books, data_process_users]
