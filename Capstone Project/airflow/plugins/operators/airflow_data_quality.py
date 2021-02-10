from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    This class performs the following:
        1. Perform Data Quality validation checks for Analytic tables.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        """
        Execute Data Quality validation checks.
        """
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        for table in self.tables:

            self.log.info("Executing Data Quality validation for table: {}".format(table))
            records = redshift_hook.get_records("SELECT COUNT(*) FROM {};".format(table))

            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error("Data Quality validation failed for table: {}.".format(table))
                raise ValueError("Data Quality validation failed for table: {}".format(table))
            self.log.info("Data Quality validation successfully completed for table: {}!!!".format(table))
