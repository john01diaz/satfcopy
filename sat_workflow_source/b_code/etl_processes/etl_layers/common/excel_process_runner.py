from typing import Optional
from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from pandas import DataFrame
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.gold.implicit.table_to_excel_exporter import \
    export_table_to_excel


@run_and_log_function
def run_excel_process(
        bie_process_id: str,
        process_table_configurations: dict,
        code_process_name: str,
        input_tables: dict,
        table_configurations: list,
        output_table_name: str) \
        -> Optional[DataFrame]:
    bie_table_id = \
        [
            process_table_configuration['bie_table_ids']
            for process_table_configuration in process_table_configurations
            if process_table_configuration['bie_process_ids'] == bie_process_id
            and process_table_configuration['table_role_types'] == 'output'
        ][0]

    output_table = \
        export_table_to_excel(
            bie_table_id=bie_table_id,
            input_tables=input_tables,
            table_configurations=table_configurations,
            code_process_name=code_process_name,
            output_table_name=output_table_name)

    return \
        output_table
