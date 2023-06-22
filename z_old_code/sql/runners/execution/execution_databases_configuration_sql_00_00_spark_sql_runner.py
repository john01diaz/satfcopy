import os

from pandas import DataFrame
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.common_spark_sql_runner import \
    run_common_spark_sql
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.git_root_path_getter import \
    get_git_root_path


# TODO: WARNING This returns an empty dataframe
def run_execution_databases_configuration_sql_00_00_spark_sql() \
        -> DataFrame:
    sql_script_file_path = \
        os.path.join(
            get_git_root_path(),
            r'sat_workflow_source/b_code/etl_processes/etl_layers/sql/base_scripts/etl_execution_packages/databases_configuration_sql_00_00.sql')

    query_tables = {
        'temp': None
    }

    vw_database_names = \
        run_common_spark_sql(
            sql_script_file_path=sql_script_file_path,
            sql_table_names_mapping_dictionary=dict(),
            input_tables=query_tables)
    
    return \
        vw_database_names
