import os
from deltalake import DeltaTable
from nf_common_source.code.services.datetime_service.time_helpers.time_getter import now_time_as_string_for_files
from nf_common_source.code.services.input_output_service.delimited_text.dataframe_dictionary_to_csv_files_writer import \
    write_dataframe_dictionary_to_csv_files

from sat_workflow_source.b_code.etl_processes.etl_layers.python.helpers.dataframe_columns_to_lowercase_renamer import \
    rename_dataframe_columns_to_lowercase
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.execution.execution_databases_configuration_sql_00_00_sql_runner import \
    run_execution_databases_configuration_sql_00_00_sql
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.gold.gold_06_io_catalogue_sql_01_00_spark_sql_runner import \
    run_gold_06_io_catalogue_sql_01_00_spark_sql
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.gold.gold_13_terminations_sql_01_00_spark_sql_runner import \
    run_gold_13_terminations_spark_sql_01_00_sql
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.dataframe_schema_as_dictionary_getter import \
    get_dataframe_schema_as_dictionary
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.git_root_path_getter import \
    get_git_root_path


def __get_parquet_file_dataframe_s_terminations():
    absolute_table_name_folder_path = \
        r'C:\bWa\AGu\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500\sigraph_silver\S_Terminations'

    delta_table = \
        DeltaTable(
            absolute_table_name_folder_path)

    # s_terminations_parquet_schema = \
    #     get_parquet_file_schema_as_dictionary(
    #         parquet_file_path=individual_parquet_file_path)

    s_terminations = \
        delta_table.to_pyarrow_table().to_pandas()

    rename_dataframe_columns_to_lowercase(
        dataframe=s_terminations)

    s_terminations_schema = \
        get_dataframe_schema_as_dictionary(
            dataframe=s_terminations)

    return \
        s_terminations


def __get_parquet_full_table_s_io_catalogue():
    absolute_table_name_folder_path = \
        r'C:\bWa\AGu\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500\sigraph_silver\S_IO_Catalogue'

    delta_table = \
        DeltaTable(
            absolute_table_name_folder_path)

    s_io_catalogue = \
        delta_table.to_pyarrow_table().to_pandas()

    return \
        s_io_catalogue


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
    sql_13_script_file_path = \
        os.path.join(
            get_git_root_path(),
            r'sat_workflow_source/b_code/etl_processes/etl_layers/sql/base_scripts/gold/13_Terminations_sql_01_00.sql')

    s_terminations_input_tables = \
        {
            's_terminations': __get_parquet_file_dataframe_s_terminations()
        }

    g_terminations = \
        run_gold_13_terminations_spark_sql_01_00_sql(
            input_tables=s_terminations_input_tables,
            vw_database_names=vw_database_names,
            sql_script_file_path=sql_13_script_file_path)

    ####################################

    # TODO: Note - This gold script uses non-standard SQL (spark SQL - cast(), replace()) - Test successful - eyeballed
    sql_06_script_file_path = \
        os.path.join(
            get_git_root_path(),
            r'sat_workflow_source/b_code/etl_processes/etl_layers/sql/base_scripts/gold/06_IO_Catalogue_sql_01_00.sql')

    io_catalogue_input_tables = \
        {
            's_io_catalogue': __get_parquet_full_table_s_io_catalogue()
        }

    g_io_catalogue = \
        run_gold_06_io_catalogue_sql_01_00_spark_sql(
            input_tables=io_catalogue_input_tables,
            vw_database_names=vw_database_names,
            sql_script_file_path=sql_06_script_file_path)

    output_dataframes_dictionary = \
        {
            '13_Terminations': g_terminations,
            '6_IO_Catalogue': g_io_catalogue
        }

    __report_tests(
        output_root_folder_path=r'C:\bWa\AGu\etl\sql_run_outputs',
        dataframes_dictionary=output_dataframes_dictionary)

