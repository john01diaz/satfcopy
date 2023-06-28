from typing import Optional
from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from pandas import DataFrame
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.gold.implicit.table_to_excel_exporter import \
    export_table_to_excel


@run_and_log_function
def run_parquet_process(
        bie_process_id: str,
        process_table_configurations: dict,
        input_tables: dict,
        table_configurations: dict) \
        -> Optional[DataFrame]:
    bie_table_id = \
        [
            process_table_configuration['bie_table_ids']
            for process_table_configuration in process_table_configurations
            if process_table_configuration['bie_process_ids'] == bie_process_id
            and process_table_configuration['table_role_types'] == 'input'
        ][0]

    table_name = \
        [
            table_configuration['table_names']
            for table_configuration in table_configurations
            if table_configuration['bie_table_ids'] == bie_table_id
        ][0]

    input_table = \
        input_tables[table_name]

    output_table = \
        input_table.copy(
            deep=True)

    return \
        output_table
