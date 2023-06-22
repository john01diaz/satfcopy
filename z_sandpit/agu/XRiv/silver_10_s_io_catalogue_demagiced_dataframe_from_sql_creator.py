import os
from pandas import DataFrame
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.gold.common.common_spark_sql_runner import \
    run_common_spark_sql
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.sql_base_scripts_folder_path_getter import \
    get_sql_base_scripts_folder_path


def create_dataframe_silver_10_s_io_catalogue_demagiced_dataframe_from_sql(
        input_tables: dict) \
        -> DataFrame:
    # move into JSON as the next step
    script_file_path = \
        os.path.join(
            get_sql_base_scripts_folder_path(),
            'silver',
            '10_S_IO_Catalogue_demagiced_depythonised.sql')

    sql_table_names_mapping_dictionary = \
        {
            'Sigraph_silver.S_Item_Function_Model': 's_item_function_model',
            'Sigraph_Silver.S_ItemFunction': 's_itemfunction',
            'Sigraph_silver.S_Pin':'s_pin'
        }

    output_dataframe = \
        run_common_spark_sql(
            sql_script_file_path=script_file_path,
            sql_table_names_mapping_dictionary=sql_table_names_mapping_dictionary,
            query_tables=input_tables)
    
    return \
        output_dataframe
