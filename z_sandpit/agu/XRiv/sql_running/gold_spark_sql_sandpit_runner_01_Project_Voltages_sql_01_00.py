import os
from deltalake import DeltaTable
from nf_common_source.code.services.datetime_service.time_helpers.time_getter import now_time_as_string_for_files
from nf_common_source.code.services.input_output_service.delimited_text.dataframe_dictionary_to_csv_files_writer import \
    write_dataframe_dictionary_to_csv_files
from nf_common_source.code.services.table_as_dictionary_service.table_as_dictionary_to_dataframe_converter import \
    convert_table_as_dictionary_to_dataframe

from sat_workflow_source.b_code.etl_processes.etl_layers.python.helpers.dataframe_columns_to_lowercase_renamer import \
    rename_dataframe_columns_to_lowercase
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.execution.execution_databases_configuration_sql_00_00_sql_runner import \
    run_execution_databases_configuration_sql_00_00_sql
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.gold.WIP\
    .gold_01_project_voltages_sql_01_00_spark_sql_runner import run_gold_01_project_voltages_sql_01_00_spark_sql
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.dataframe_schema_as_dictionary_getter import \
    get_dataframe_schema_as_dictionary
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.git_root_path_getter import \
    get_git_root_path
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.parquet_file_schema_as_dictionary_getter import \
    get_parquet_file_schema_as_dictionary
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.schema_to_schemas_report_adder import \
    add_schema_to_schemas_report
from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.format_table_types import FormatTableTypes
from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.origin_table_types import OriginTableTypes
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers\
    .sql_create_table_statement_from_pandas_dataframe_exporter import \
    export_sql_create_table_statement_from_pandas_dataframe


def __get_output_folder_path(
        output_root_folder_path: str) \
        -> str:
    output_folder_path = \
        os.path.join(
            output_root_folder_path,
            now_time_as_string_for_files())

    os.makedirs(
        output_folder_path,
        exist_ok=True)

    return \
        output_folder_path


def __get_parquet_file_dataframe_cs_layer_loop_loop_elements(
        schemas_report_dictionary: dict,
        output_folder_path: str):
    absolute_table_name_folder_path = \
        r'C:\bWa\AGu\etl\collect\blob\blob-temp-anusha_folder-sigraph_bronze_2023_05_21_1500\sigraph_bronze\CS_Layer_Loop_Loop_elements'

    delta_table = \
        DeltaTable(
            absolute_table_name_folder_path)

    cs_layer_loop_loop_elements_parquet_schema = \
        get_parquet_file_schema_as_dictionary(
            delta_table=delta_table,
            parquet_file_path=absolute_table_name_folder_path)

    add_schema_to_schemas_report(
        schemas_report=schemas_report_dictionary,
        schema_dictionary=cs_layer_loop_loop_elements_parquet_schema,
        table_name='cs_layer_loop_loop_elements',
        origin_table_type=OriginTableTypes.SOURCE,
        format_table_type=FormatTableTypes.PARQUET)

    cs_layer_loop_loop_elements = \
        delta_table.to_pyarrow_table().to_pandas()

    rename_dataframe_columns_to_lowercase(
        dataframe=cs_layer_loop_loop_elements)

    export_sql_create_table_statement_from_pandas_dataframe(
        output_root_folder_path=output_folder_path,
        output_sql_file_name='cs_layer_loop_loop_elements_input_pandas.sql',
        dataframe=cs_layer_loop_loop_elements,
        table_name='cs_layer_loop_loop_elements')

    cs_layer_loop_loop_elements_schema = \
        get_dataframe_schema_as_dictionary(
            dataframe=cs_layer_loop_loop_elements)

    add_schema_to_schemas_report(
        schemas_report=schemas_report_dictionary,
        schema_dictionary=cs_layer_loop_loop_elements_schema,
        table_name='cs_layer_loop_loop_elements',
        origin_table_type=OriginTableTypes.SOURCE,
        format_table_type=FormatTableTypes.PANDAS_DATAFRAME)

    return \
        cs_layer_loop_loop_elements

def __report_tests(
        output_folder_path: str,
        dataframes_dictionary: dict):
    write_dataframe_dictionary_to_csv_files(
        folder_name=output_folder_path,
        dataframes_dictionary=dataframes_dictionary)


if __name__ == '__main__':
    output_folder_path_test = \
        __get_output_folder_path(
            output_root_folder_path=r'C:\bWa\AGu\etl\sql_run_outputs')

    schemas_report = \
        dict()

    vw_database_names = \
        run_execution_databases_configuration_sql_00_00_sql()

    # TODO: WARNING - Following commented line returns an empty dataframe
    # vw_database_names = \
    #     run_execution_databases_configuration_sql_00_00_spark_sql()

    ###########################################

    # TODO: Note - This gold script uses standard SQL - Test successful
    sql_01_script_file_path = \
        os.path.join(
            get_git_root_path(),
            r'sat_workflow_source/b_code/etl_processes/etl_layers/sql/base_scripts/gold/01_Project_Voltages_sql_01_00.sql')

    project_voltages_input_tables = \
        {
            'cs_layer_loop_loop_elements': __get_parquet_file_dataframe_cs_layer_loop_loop_elements(
                schemas_report_dictionary=schemas_report,
                output_folder_path=output_folder_path_test)
        }

    g_project_voltages = \
        run_gold_01_project_voltages_sql_01_00_spark_sql(
            input_tables=project_voltages_input_tables,
            vw_database_names=vw_database_names,
            sql_script_file_path=sql_01_script_file_path)

    # TODO: Need to translate this into SQL CREATE table - maybe this is not the right place to do this
    g_project_voltages_dataframe_schema = \
        get_dataframe_schema_as_dictionary(
            dataframe=g_project_voltages)

    add_schema_to_schemas_report(
        schemas_report=schemas_report,
        schema_dictionary=g_project_voltages_dataframe_schema,
        table_name='g_project_voltages',
        origin_table_type=OriginTableTypes.GENERATED,
        format_table_type=FormatTableTypes.PANDAS_DATAFRAME)

    schemas_report_dataframe = \
        convert_table_as_dictionary_to_dataframe(
            table_as_dictionary=schemas_report)

    output_dataframes_dictionary = \
        {
            '01_Project_Voltages': g_project_voltages,
            'schemas_report': schemas_report_dataframe,
        }

    __report_tests(
        output_folder_path=output_folder_path_test,
        dataframes_dictionary=output_dataframes_dictionary)

