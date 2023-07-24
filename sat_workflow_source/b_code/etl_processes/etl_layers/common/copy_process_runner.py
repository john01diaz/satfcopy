from typing import Optional
from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from pandas import DataFrame
from sat_workflow_source.b_code.etl_processes.etl_layers.common.single_input_table_getter import get_single_input_table


@run_and_log_function
def run_copy_process(
        bie_process_id: str,
        process_table_configurations: dict,
        input_tables: dict,
        table_configurations: dict) \
        -> Optional[DataFrame]:
    input_table = \
        get_single_input_table(
            bie_process_id=bie_process_id,
            process_table_configurations=process_table_configurations,
            input_tables=input_tables,
            table_configurations=table_configurations)

    output_table = \
        input_table.copy(
            deep=True)

    return \
        output_table
