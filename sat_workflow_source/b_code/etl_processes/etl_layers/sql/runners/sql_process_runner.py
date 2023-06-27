from typing import Optional
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from pandas import DataFrame

from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.common_spark_sql_runner import \
    run_common_spark_sql
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.code_from_string.base_script_root_folder_path_getter import \
    get_base_script_root_folder_path
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.code_from_string.file_path_from_name_and_root_folder_path_getter import \
    get_file_path_from_file_name_and_root_folder_path


def run_sql_process(
        raw_and_bie_sub_register,
        process_name: str,
        input_tables: dict) \
        -> Optional[DataFrame]:
    sql_base_script_root_folder_path = \
        get_base_script_root_folder_path(
            type_folder_name='sql')

    sql_script_file_path = \
        get_file_path_from_file_name_and_root_folder_path(
            filename_to_find=process_name + '.sql',
            root_folder_path=sql_base_script_root_folder_path)

    if not sql_script_file_path:
        message = \
            'ERROR - no ' + process_name + ' sql file found in ' + sql_base_script_root_folder_path

        log_message(
            message=message)

        return

    output_table = \
        run_common_spark_sql(
            raw_and_bie_sub_register=raw_and_bie_sub_register,
            sql_script_file_path=sql_script_file_path,
            input_tables=input_tables)

    return \
        output_table
