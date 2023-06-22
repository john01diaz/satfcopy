from typing import Optional
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from pandas import DataFrame
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.code_from_string.base_script_root_folder_path_getter import \
    get_base_script_root_folder_path
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.code_from_string.code_process_from_name_and_root_folder_path_getter import \
    get_code_process_from_name_and_root_folder_path


def run_code_process(
        code_process_name: str,
        input_tables: dict) \
        -> Optional[DataFrame]:
    base_script_root_folder_path = \
        get_base_script_root_folder_path(
            type_folder_name='python')
    
    code_process = \
        get_code_process_from_name_and_root_folder_path(
            code_process_name=code_process_name,
            root_folder_path=base_script_root_folder_path)

    if not code_process:
        message = \
            'ERROR - no ' + code_process_name + ' code process found in ' + base_script_root_folder_path

        log_message(
            message=message)

        return

    output_table = \
        code_process(
            input_tables=input_tables)

    return \
        output_table
