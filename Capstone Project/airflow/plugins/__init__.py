from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class DataPipelinePlugin(AirflowPlugin):
    name = "datapipeline_plugin"
    operators = [
        operators.DataQualityOperator,
        helpers.DataAnalyticsOperator
    ]
