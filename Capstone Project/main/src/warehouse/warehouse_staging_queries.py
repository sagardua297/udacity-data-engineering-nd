import configparser
from pathlib import Path

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/warehouse_config.cfg"))

# Setup Warehouse Staging Schema Configs
staging_schema = config.get('STAGING', 'SCHEMA')
s3_final_store = 's3://' + config.get('BUCKET', 'FINAL_STORE')
iam_role = config.get('IAM_ROLE', 'ARN')

# Create Warehouse Staging Schema
create_staging_schema = "CREATE SCHEMA IF NOT EXISTS {};".format(staging_schema)

# Drop Warehouse Staging Schema Tables
drop_authors_table = "DROP TABLE IF EXISTS {}.authors;".format(staging_schema)
drop_reviews_table = "DROP TABLE IF EXISTS {}.reviews;".format(staging_schema)
drop_books_table = "DROP TABLE IF EXISTS {}.books;".format(staging_schema)
drop_users_table = "DROP TABLE IF EXISTS {}.users;".format(staging_schema)

# Create Warehouse Staging Schema Table - AUTHORS
create_authors_table = """
CREATE TABLE IF NOT EXISTS {}.authors
(
    author_id           BIGINT PRIMARY KEY,
    name                VARCHAR,
    role                VARCHAR,
    profile_url         VARCHAR,
    average_rating      FLOAT,
    rating_count        INT,
    text_review_count   INT,
    create_timestamp    TIMESTAMP
)
DISTSTYLE ALL;
""".format(staging_schema)

# Create Warehouse Staging Schema Table - REVIEWS
create_reviews_table = """
CREATE TABLE IF NOT EXISTS {}.reviews
(
    review_id           BIGINT PRIMARY KEY ,
    user_id             BIGINT,
    book_id             BIGINT,
    author_id           BIGINT,
    review_text         VARCHAR(max),
    review_rating       FLOAT,
    review_votes        INT,
    spoiler_flag        BOOLEAN,
    spoiler_state       VARCHAR,
    review_added_date   TIMESTAMP,
    review_updated_date TIMESTAMP,
    review_read_count   INT,
    comments_count      INT,
    review_url          VARCHAR,
    create_timestamp    TIMESTAMP
)
DISTSTYLE ALL;
""".format(staging_schema)

# Create Warehouse Staging Schema Table - BOOKS
create_books_table = """
CREATE TABLE IF NOT EXISTS {}.books
(
    book_id              BIGINT PRIMARY KEY ,
    title                VARCHAR,
    title_without_series VARCHAR,
    image_url            VARCHAR,
    book_url             VARCHAR,
    num_pages            INT,
    "format"             VARCHAR,
    edition_information  VARCHAR,
    publisher            VARCHAR,
    publication_day      INT2,
    publication_year     INT2,
    publication_month    INT2,
    average_rating       FLOAT,
    ratings_count        INT,
    description          VARCHAR(max),
    authors              BIGINT,
    published            INT2,
    create_timestamp     TIMESTAMP
)
DISTSTYLE ALL;
""".format(staging_schema)

# Create Warehouse Staging Schema Table - USERS
create_users_table = """
CREATE TABLE IF NOT EXISTS {}.users
(
    user_id             BIGINT PRIMARY KEY ,
    user_name           VARCHAR,
    user_display_name   VARCHAR,
    location            VARCHAR,
    profile_link        VARCHAR,
    uri                 VARCHAR,
    user_image_url      VARCHAR,
    small_image_url     VARCHAR,
    has_image           BOOLEAN,
    create_timestamp    TIMESTAMP
)
DISTSTYLE ALL;
""".format(staging_schema)

# Copy to Staging for table - AUTHORS
copy_authors_table = """
COPY {0}.authors
FROM '{1}/authors'
IAM_ROLE '{2}'
CSV
DELIMITER '|'
GZIP
NULL AS  '\\000'
IGNOREHEADER 1;
""".format(staging_schema, s3_final_store, iam_role)

# Copy to Staging for table - REVIEWS
copy_reviews_table = """
COPY {0}.reviews
FROM '{1}/reviews'
IAM_ROLE '{2}'
CSV
DELIMITER '|'
GZIP
NULL AS  '\\000'
IGNOREHEADER 1;
""".format(staging_schema, s3_final_store, iam_role)

# Copy to Staging for table - BOOKS
copy_books_table = """
COPY {0}.books
FROM '{1}/books'
IAM_ROLE '{2}'
CSV
DELIMITER '|'
GZIP
NULL AS  '\\000'
IGNOREHEADER 1;
""".format(staging_schema, s3_final_store, iam_role)

# Copy to Staging for table - USERS
copy_users_table = """
COPY {0}.users
FROM '{1}/users'
IAM_ROLE '{2}'
CSV
DELIMITER '|'
GZIP
NULL AS  '\\000'
IGNOREHEADER 1;
""".format(staging_schema, s3_final_store, iam_role)

drop_staging_tables = [drop_authors_table, drop_reviews_table, drop_books_table, drop_users_table]
create_staging_tables = [create_authors_table, create_reviews_table, create_books_table, create_users_table]
copy_staging_tables = [copy_authors_table, copy_reviews_table, copy_books_table, copy_users_table]
