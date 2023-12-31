from typing import Optional
from pandas import DataFrame


def get_single_input_table(
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

    return \
        input_table
