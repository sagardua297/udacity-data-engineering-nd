from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataAnalyticsOperator(BaseOperator):
    """
    This class performs the following:
        1. Executes Data Analytics query.
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query=[],
                 *args, **kwargs):
        super(DataAnalyticsOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query

    def execute(self, context):
        """
        Executes Data Analytics query.
        """
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for query in self.sql_query:
            self.log.info("Executing Analytics query:  {}".format(query))
            redshift_hook.run(self.sql_query)
            self.log.info("Analytics query executed successfully!!")
