from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from pyspark.sql import SparkSession
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.silver_00_reusable_functions_list import silver_00_reusable_functions
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.silver_01_data_loaders_functions_list import silver_01_data_loaders_functions_and_return_types
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.silver_enumerations_functions_list import silver_enumerations_functions


@run_and_log_function
def register_user_defined_functions_in_spark(
        spark_session: SparkSession) \
        -> None:
    for function_and_return_type in silver_enumerations_functions:
        spark_session.udf.register(
            function_and_return_type[0],
            function_and_return_type[1],
            function_and_return_type[2])

    for function_and_return_type in silver_00_reusable_functions:
        spark_session.udf.register(
            function_and_return_type[0],
            function_and_return_type[1],
            function_and_return_type[2])

    for function_and_return_type in silver_01_data_loaders_functions_and_return_types:
        spark_session.udf.register(
            # function_and_return_type[0].__name__,
            function_and_return_type[0],
            function_and_return_type[1],
            function_and_return_type[2])
