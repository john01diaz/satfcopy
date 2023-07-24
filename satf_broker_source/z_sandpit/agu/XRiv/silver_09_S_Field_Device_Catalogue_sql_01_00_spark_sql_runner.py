from pandas import DataFrame

from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.gold.common.common_spark_sql_runner import \
    run_common_spark_sql


def run_silver_09_S_Field_Device_Catalogue_sql_01_00_spark_sql(
        input_tables: dict,
        vw_database_names: DataFrame,
        sql_script_file_path: str) \
        -> DataFrame:
    sql_table_names_mapping_dictionary = \
        {
            'Sigraph.Loop_Elements': 'loop_elements',
            'Sigraph_Silver.S_ItemFunction': 's_item_function',
            'sigraph_Reference.DeviceType': 'device_type'
        }

    query_tables = \
        input_tables | {'vw_database_names': vw_database_names}

    silver_09_S_Field_Device_Catalogue_sql_01_00 = \
        run_common_spark_sql(
            sql_script_file_path=sql_script_file_path,
            sql_table_names_mapping_dictionary=sql_table_names_mapping_dictionary,
            query_tables=query_tables)
    
    return \
        silver_09_S_Field_Device_Catalogue_sql_01_00

