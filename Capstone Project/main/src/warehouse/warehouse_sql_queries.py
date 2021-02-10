import configparser
from pathlib import Path

# Setup Warehouse Schema Tables Configs
config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/warehouse_config.cfg"))

warehouse_schema = config.get('WAREHOUSE', 'SCHEMA')

# Create Warehouse Schema
create_warehouse_schema = "CREATE SCHEMA IF NOT EXISTS {};".format(warehouse_schema)

# Drop Warehouse Schema tables
drop_authors_table = "DROP TABLE IF EXISTS {}.authors;".format(warehouse_schema)
drop_reviews_table = "DROP TABLE IF EXISTS .reviews;".format(warehouse_schema)
drop_books_table = "DROP TABLE IF EXISTS {}.books;".format(warehouse_schema)
drop_users_table = "DROP TABLE IF EXISTS {}.users;".format(warehouse_schema)

# Create Warehouse Schema table - AUTHORS
create_authors_table = """
CREATE TABLE IF NOT EXISTS {}.authors
(
    author_id           BIGINT PRIMARY KEY DISTKEY,
    name                VARCHAR,
    role                VARCHAR,
    profile_url         VARCHAR,
    average_rating      FLOAT,
    rating_count        INT,
    text_review_count   INT,
    create_timestamp    TIMESTAMP
)
DISTSTYLE KEY;
""".format(warehouse_schema)

# Create Warehouse Schema table - REVIEWS
create_reviews_table = """
CREATE TABLE IF NOT EXISTS {}.reviews
(
    review_id               BIGINT PRIMARY KEY ,
    user_id                 BIGINT,
    book_id                 BIGINT,
    author_id               BIGINT DISTKEY ,
    review_text             VARCHAR(max),
    review_rating           FLOAT,
    review_votes            INT,
    spoiler_flag            BOOLEAN,
    spoiler_state           VARCHAR,
    review_added_date       TIMESTAMP,
    review_updated_date     TIMESTAMP,
    review_read_count       INT,
    comments_count          INT,
    review_url              VARCHAR,
    create_timestamp        TIMESTAMP
)
DISTSTYLE KEY;
""".format(warehouse_schema)

# Create Warehouse Schema table - BOOKS
create_books_table = """
CREATE TABLE IF NOT EXISTS {}.books
(
    book_id                 BIGINT PRIMARY KEY ,
    title                   VARCHAR,
    title_without_series    VARCHAR,
    image_url               VARCHAR,
    book_url                VARCHAR,
    num_pages               INT,
    "format"                VARCHAR,
    edition_information     VARCHAR,
    publisher               VARCHAR,
    publication_day         INT2,
    publication_year        INT2,
    publication_month       INT2,
    average_rating          FLOAT,
    ratings_count           INT,
    description             VARCHAR(max),
    authors                 BIGINT,
    published               INT2,
    create_timestamp        TIMESTAMP
)
DISTSTYLE EVEN;
""".format(warehouse_schema)

# Create Warehouse Schema table - USERS
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
DISTSTYLE EVEN;
""".format(warehouse_schema)

drop_warehouse_tables = [drop_authors_table, drop_reviews_table, drop_books_table, drop_users_table]
create_warehouse_tables = [create_authors_table, create_reviews_table, create_books_table, create_users_table]
