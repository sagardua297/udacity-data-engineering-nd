class AnalyticQueries:
    """
    This class performs the following:
        1. Create Analytics Schema.
        2. Create Analytics tables in Analytics schema.
        3. Populate Analytic tables using Warehouse schema tables.
    """

    # Create "DATA_ANALYTICS" Schema
    create_analytics_schema = """CREATE SCHEMA IF NOT EXISTS data_analytics;"""

    # Create "AUTHOR" specific analytic tables
    create_author_reviews = """
        CREATE TABLE IF NOT EXISTS data_analytics.popular_authors_review_count
        (
            author_id           BIGINT PRIMARY KEY DISTKEY,
            review_count        BIGINT,
            name                VARCHAR,
            role                VARCHAR,
            profile_url         VARCHAR,
            average_rating      FLOAT,
            rating_count        INT,
            text_review_count   INT,
            create_timestamp    TIMESTAMP
        );
        """

    create_author_rating = """
        CREATE TABLE IF NOT EXISTS data_analytics.popular_authors_average_rating
        (
            author_id               BIGINT PRIMARY KEY DISTKEY,
            average_review_rating   FLOAT,
            name                    VARCHAR,
            role                    VARCHAR,
            profile_url             VARCHAR,
            average_rating          FLOAT,
            rating_count            INT,
            text_review_count       INT,
            create_timestamp        TIMESTAMP
        );
    """

    create_best_authors = """
        CREATE TABLE IF NOT EXISTS data_analytics.best_authors
        (
            author_id               BIGINT PRIMARY KEY DISTKEY,
            review_count            BIGINT,
            average_review_rating   FLOAT,
            name                    VARCHAR,
            role                    VARCHAR,
            profile_url             VARCHAR,
            average_rating          FLOAT,
            rating_count            INT,
            text_review_count       INT,
            create_timestamp        TIMESTAMP
        );
    """

    # Populate "AUTHOR" specific analytic tables
    insert_authors_reviews = """
        INSERT INTO data_analytics.popular_authors_review_count
        SELECT  p.author_id AS author_id,
                review_count,
                name,
                role,
                profile_url,
                average_rating,
                rating_count,
                text_review_count,
                create_timestamp
        FROM (
                SELECT TOP 10 rev.author_id AS author_id,
                       COUNT(rev.review_id) AS review_count
                FROM data_warehouse.reviews AS rev
                WHERE rev.create_timestamp > '{0}' AND rev.create_timestamp < '{1}'
                GROUP BY rev.author_id
                ORDER BY review_count DESC
            ) p
        INNER JOIN data_warehouse.authors aut
            ON p.author_id = aut.author_id;
    """

    insert_authors_ratings = """
        INSERT INTO data_analytics.popular_authors_average_rating
        SELECT p.author_id AS author_id,
               average_review_rating,
               name,
               role,
               profile_url,
               average_rating,
               rating_count,
               text_review_count,
               create_timestamp
        FROM (
                SELECT TOP 10 rev.author_id AS author_id,
                       AVG(rev.review_rating) AS average_review_rating
                FROM data_warehouse.reviews AS rev
                WHERE rev.create_timestamp > '{0}' AND rev.create_timestamp < '{1}'
                GROUP BY rev.author_id
                ORDER BY average_review_rating DESC
            ) p
        INNER JOIN data_warehouse.authors aut
            ON p.author_id = aut.author_id;
    """

    insert_best_authors = """
        INSERT INTO data_analytics.best_authors
        SELECT ar.author_id,
               rc.review_count,
               ar.average_review_rating,
               ar.name,
               ar.role,
               ar.profile_url,
               ar.average_rating,
               ar.rating_count,
               ar.text_review_count,
               ar.create_timestamp
        FROM data_analytics.popular_authors_average_rating ar
        INNER JOIN data_analytics.popular_authors_review_count rc
            ON ar.author_id = rc.author_id;
    """

    # Create "BOOKS" specific analytic tables
    create_book_reviews = """
        CREATE TABLE IF NOT EXISTS data_analytics.popular_books_review_count
        (
            book_id                 BIGINT PRIMARY KEY,
            review_count            BIGINT,
            title                   VARCHAR,
            title_without_series    VARCHAR,
            image_url               VARCHAR,
            book_url                VARCHAR,
            num_pages               INT,
            "format"                VARCHAR,
            edition_information     VARCHAR,
            publisher               VARCHAR,
            average_rating          FLOAT,
            ratings_count           INT,
            description             VARCHAR(max),
            authors                 BIGINT,
            create_timestamp        TIMESTAMP
        );
    """

    create_book_rating = """
        CREATE TABLE IF NOT EXISTS data_analytics.popular_books_average_rating
        (
            book_id                 BIGINT PRIMARY KEY,
            average_reviews_rating  FLOAT,
            title                   VARCHAR,
            title_without_series    VARCHAR,
            image_url               VARCHAR,
            book_url                VARCHAR,
            num_pages               INT,
            "format"                VARCHAR,
            edition_information     VARCHAR,
            publisher               VARCHAR,
            average_rating          FLOAT,
            ratings_count           INT,
            description             VARCHAR(max),
            authors                 BIGINT,
            create_timestamp        TIMESTAMP
        );
    """

    create_best_books = """
        CREATE TABLE IF NOT EXISTS data_analytics.best_books
        (
            book_id                 BIGINT PRIMARY KEY,
            average_reviews_rating  BIGINT,
            review_count            BIGINT,
            title                   VARCHAR,
            title_without_series    VARCHAR,
            image_url               VARCHAR,
            book_url                VARCHAR,
            num_pages               INT,
            "format"                VARCHAR,
            edition_information     VARCHAR,
            publisher               VARCHAR,
            average_rating          FLOAT,
            ratings_count           INT,
            description             VARCHAR(max),
            authors                 BIGINT,
            create_timestamp        TIMESTAMP
        );
    """

    # Populate "BOOKS" specific analytic tables
    insert_books_reviews = """
        INSERT INTO data_analytics.popular_books_review_count
        SELECT p.book_id AS author_id,
               review_count,
               title,
               title_without_series,
               image_url,
               book_url,
               num_pages,
               format,
               edition_information,
               publisher,
               average_rating,
               ratings_count,
               description,
               authors,
               create_timestamp
        FROM (
                SELECT TOP 10 rev.book_id AS book_id,
                       COUNT(rev.review_id) AS review_count
                FROM data_warehouse.reviews as rev
                where rev.create_timestamp > '{0}' AND rev.create_timestamp < '{1}'
                GROUP BY rev.book_id
                ORDER BY review_count DESC
            ) p
        INNER JOIN data_warehouse.books book
            ON p.book_id = book.book_id
        ORDER BY review_count;
    """

    insert_books_ratings = """
        INSERT INTO data_analytics.popular_books_average_rating
        SELECT p.book_id AS author_id,
               average_review_rating,
               title,
               title_without_series,
               image_url,
               book_url,
               num_pages,
               format,
               edition_information,
               publisher,
               average_rating,
               ratings_count,
               description,
               authors,
               create_timestamp
        FROM (
                SELECT TOP 10 rev.book_id AS book_id,
                       AVG(rev.review_rating) AS average_review_rating
                FROM data_warehouse.reviews AS rev
                WHERE rev.create_timestamp > '{0}' AND rev.create_timestamp < '{1}'
                GROUP BY rev.book_id
                ORDER BY average_review_rating DESC
            ) p
        INNER JOIN data_warehouse.books book
            ON p.book_id = book.book_id
        ORDER BY average_review_rating;
    """

    insert_best_books = """
        INSERT INTO data_analytics.best_books
        SELECT ar.book_id,
               ar.average_reviews_rating,
               rc.review_count,
               ar.title,
               ar.title_without_series,
               ar.image_url,
               ar.book_url,
               ar.num_pages,
               ar.format,
               ar.edition_information,
               ar.publisher,
               ar.average_rating,
               ar.ratings_count,
               ar.description,
               ar.authors,
               ar.record_create_timestamp
        FROM data_analytics.popular_books_average_rating ar
        INNER JOIN data_analytics.popular_books_review_count rc
            ON ar.book_id = rc.book_id;
    """
