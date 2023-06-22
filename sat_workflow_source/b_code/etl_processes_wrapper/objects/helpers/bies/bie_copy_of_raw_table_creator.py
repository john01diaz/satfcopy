import pandas
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from sat_workflow_source.b_code.etl_processes_wrapper.objects.bie_sub_registers import BieSubRegisters
from sat_workflow_source.b_code.etl_processes_wrapper.objects.b_tables import BTables
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.bie_ids_to_etl_table_adder import \
    add_bie_ids_to_table


def create_bie_copy_of_raw_table(
        bie_sub_register: BieSubRegisters,
        raw_table: BTables,
        table_name: str) \
        -> pandas.DataFrame:
    table_with_bie_ids = \
        raw_table.table.copy(
            deep=True)

    log_message(
        'Adding bie_immutable_item_ids to ' + table_name)

    raw_table_column_names = \
        list(
            raw_table.table.columns.values)

    add_bie_ids_to_table(
        table=table_with_bie_ids,
        table_name=table_name,
        bie_id_column_name='bie_immutable_item_ids',
        identifier_column_names=raw_table_column_names)

    log_message(
        'Adding bie_ids to ' + table_name)

    if not raw_table.identifier_column_names:
        identifier_column_names = \
            raw_table_column_names
        
    else:
        identifier_column_names = \
            raw_table.identifier_column_names
        
    add_bie_ids_to_table(
        table=table_with_bie_ids,
        table_name=table_name,
        bie_id_column_name='bie_ids',
        identifier_column_names=identifier_column_names)

    bie_sub_register.bie_tables[table_name] = \
        table_with_bie_ids

    return \
        table_with_bie_ids
