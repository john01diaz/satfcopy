import os

import pandas
from deltalake import DeltaTable
from nf_common_source.code.services.datetime_service.time_helpers.time_getter import now_time_as_string_for_files
from nf_common_source.code.services.input_output_service.delimited_text.dataframe_dictionary_to_csv_files_writer import \
    write_dataframe_dictionary_to_csv_files

from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.execution.execution_databases_configuration_sql_00_00_sql_runner import \
    run_execution_databases_configuration_sql_00_00_sql
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.git_root_path_getter import \
    get_git_root_path
from satf_broker_source.z_sandpit.agu.XRiv.silver_09_S_Field_Device_Catalogue_sql_01_00_spark_sql_runner import \
    run_silver_09_S_Field_Device_Catalogue_sql_01_00_spark_sql
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.enum_from_string.enum_getter import get_enum


def __get_parquet_full_table_device_type():
    absolute_table_name_folder_path = \
        r'C:\bWa\AGu\etl\collect\blob\blob-temp-anusha_folder-sigraph_reference_files_2023_05_21_1500\sigraph_reference_files\DeviceType'

    delta_table = \
        DeltaTable(
            absolute_table_name_folder_path)

    device_type = \
        delta_table.to_pyarrow_table().to_pandas()
    
    return \
        device_type

    print("device_type loaded")

def __get_parquet_full_table_loop_elements():
    absolute_table_name_folder_path = \
        r'C:\bWa\AGu\etl\collect\blob\blob-temp-anusha_folder-sigraph_bronze_2023_05_21_1500\sigraph_bronze\Loop_elements'

    enum = \
        get_enum(
            enum_name='Loop_elements_FilteredFor09SFieldDeviceCatalogueSql0100SqlParquet')

    columns = \
        [column.value for column in enum]

    loop_elements = \
        pandas.read_parquet(
                absolute_table_name_folder_path,
                engine='pyarrow',
                columns=columns)
    
    reduced_loop_elements = loop_elements.head(500)
    
    # delta_table = \
    #     DeltaTable(
    #         absolute_table_name_folder_path)

    # loop_elements = \
    #     delta_table.to_pyarrow_table().to_pandas()

    print("loop_elements loaded")
    
    return \
        reduced_loop_elements

def __get_parquet_full_table_s_item_function():
    absolute_table_name_folder_path = \
        r'C:\bWa\AGu\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500\sigraph_silver\S_ItemFunction'

    delta_table = \
        DeltaTable(
            absolute_table_name_folder_path)

    s_item_function = \
        delta_table.to_pyarrow_table().to_pandas()

    print("s_item_function loaded")
    
    return \
        s_item_function



def __report_tests(
        output_root_folder_path: str,
        dataframes_dictionary: dict):
    output_folder_path = \
        os.path.join(
            output_root_folder_path,
            now_time_as_string_for_files())

    os.makedirs(
        output_folder_path,
        exist_ok=True)

    write_dataframe_dictionary_to_csv_files(
        folder_name=output_folder_path,
        dataframes_dictionary=dataframes_dictionary)


if __name__ == '__main__':
    vw_database_names = \
        run_execution_databases_configuration_sql_00_00_sql()

    # TODO: WARNING - Following commented line returns an empty dataframe
    # vw_database_names = \
    #     run_execution_databases_configuration_sql_00_00_spark_sql()

    ###########################################

    # TODO: Note - This gold script uses standard SQL - Test successful
    silver_09_sql_01_00_file_path = \
        os.path.join(
            get_git_root_path(),
            r'sat_workflow_source/b_code/etl_processes/etl_layers/sql/base_scripts/silver/09_S_Field_Device_Catalogue_sql_01_00.sql')

    silver_09_sql_01_00_input_tables = \
        {
            'device_type': __get_parquet_full_table_device_type(),
            's_item_function': __get_parquet_full_table_s_item_function(),
            'loop_elements': __get_parquet_full_table_loop_elements()
        }
    
    print ("all input tables loaded")

    silver_09_S_Field_Device_Catalogue_sql_01_00 = \
        run_silver_09_S_Field_Device_Catalogue_sql_01_00_spark_sql(
            input_tables=silver_09_sql_01_00_input_tables,
            vw_database_names=vw_database_names,
            sql_script_file_path=silver_09_sql_01_00_file_path)

    
    output_dataframes_dictionary = \
        {
            'silver_09_sql_01_00': silver_09_S_Field_Device_Catalogue_sql_01_00
        }

    __report_tests(
        output_root_folder_path=r'C:\bWa\AGu\etl\sql_run_outputs',
        dataframes_dictionary=output_dataframes_dictionary)

