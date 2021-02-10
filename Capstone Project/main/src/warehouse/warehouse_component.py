import configparser
import psycopg2
from .warehouse_staging_queries import create_staging_schema, drop_staging_tables, create_staging_tables, copy_staging_tables
from .warehouse_sql_queries import create_warehouse_schema, drop_warehouse_tables, create_warehouse_tables
from .warehouse_data_process import data_process_queries
from pathlib import Path

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/warehouse_config.cfg"))

class DataPipelineWarehouse:
    """
    This class performs the following:
        1. Create Staging Schema.
        2. Create Staging tables in Staging schema.
        3. Populate Staging tables.
        4. Create Warehouse Schema.
        5. Create Warehouse tables in Warehouse schema.
        6. Populate Warehouse tables.
    """

    def __init__(self):
        self._conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        self._cur = self._conn.cursor()

    def setup_staging_tables(self):
        """
        Create Staging schema and Staging schema tables.
        """
        self.execute_query([create_staging_schema])

        self.execute_query(drop_staging_tables)

        self.execute_query(create_staging_tables)

    def load_staging_tables(self):
        """
        Populate Staging schema tables.
        """
        self.execute_query(copy_staging_tables)

    def setup_warehouse_tables(self):
        """
        Create Warehouse schema and Warehouse schema tables.
        """
        self.execute_query([create_warehouse_schema])

        self.execute_query(create_warehouse_tables)

    def perform_data_process(self):
        """
        Populate Warehouse schema tables.
        """
        self.execute_query(data_process_queries)

    def execute_query(self, query_list):
        """
        Process Warehouse schema tables list.
        :param query_list: list of data process operations
        """
        for query in query_list:
            print(query)
            self._cur.execute(query)
            self._conn.commit()
