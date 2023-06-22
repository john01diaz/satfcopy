import os
from pandas import DataFrame
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.common_spark_sql_runner import \
    run_common_spark_sql
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.sql_base_scripts_folder_path_getter import \
    get_sql_base_scripts_folder_path


def create_dataframe_gold_c06_io_catalogue_sql_01_00_from_sql(
        input_tables: dict) \
        -> DataFrame:
    # move into JSON as the next step
    script_file_path = \
        os.path.join(
            get_sql_base_scripts_folder_path(),
            'gold',
            '06_IO_Catalogue_sql_01_00.sql')

    sql_table_names_mapping_dictionary = \
        {
            'SIGRAPH_SILVER.S_IO_CATALOGUE': 's_io_catalogue',
            'VW_Database_names': 'database_names'
        }

    output_dataframe = \
        run_common_spark_sql(
            sql_script_file_path=script_file_path,
            sql_table_names_mapping_dictionary=sql_table_names_mapping_dictionary,
            input_tables=input_tables)
    
    return \
        output_dataframe
